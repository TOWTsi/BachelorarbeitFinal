package eu.stratosphere.nephele.checkpointing;


/***********************************************************************************************************************
*
* Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
* an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
* specific language governing permissions and limitations under the License.
*
**********************************************************************************************************************/

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.FileBufferManager;

/**
* An ephemeral checkpoint is a checkpoint that can be used to recover from
* crashed tasks within a processing pipeline. An ephemeral checkpoint is created
* for each task (more precisely its {@link Environment} object). For file channels
* an ephemeral checkpoint is always persistent, i.e. data is immediately written to disk.
* For network channels the ephemeral checkpoint is held into main memory until a checkpoint
* decision is made. Based on this decision the checkpoint is either made permanent or discarded.
* <p>
* This class is not thread-safe.
* 
* @author warneke
*/
public class EphemeralChannelCheckpoint implements CheckpointDecisionRequester {

	/**
	 * The log object used to report problems.
	 */
	private static final Log LOG = LogFactory.getLog(EphemeralCheckpoint.class);

	/**
	 * The enveloped which are currently queued until the state of the checkpoint is decided.
	 */
	private final Queue<TransferEnvelope> queuedEnvelopes = new ArrayDeque<TransferEnvelope>();

	/**
	 * The task this checkpoint is created for.
	 */
	private final RuntimeTask task;

	/**
	 * The total number of output channels connected to this checkpoint.
	 */
	private int totalNumberOfOutputChannels;

	/**
	 * Stores whether a completed checkpoint has already been announced to the task.
	 */
	private boolean completeCheckpointAnnounced = false;

	/**
	 * Reference to a write thread that may be spawned to write the checkpoint data asynchronously
	 */
	//private WriteThread writeThread = null;
	private HashMap<ChannelID,WriteChannelThread>  writeThreads = new HashMap<ChannelID,WriteChannelThread>();
	/**
	 * This enumeration reflects the possible states an ephemeral
	 * checkpoint can be in.
	 * 
	 * @author warneke
	 */
	private enum CheckpointingDecisionState {
		NO_CHECKPOINTING, UNDECIDED, CHECKPOINTING
	};

	/**
	 * The current state the ephemeral checkpoint is in.
	 */
	private CheckpointingDecisionState checkpointingDecision;

	/**
	 * Stores whether a checkpoint decision has been requested asynchronously.
	 */
	private volatile boolean asyncronousCheckpointDecisionRequested = false;

	private ArrayList<ChannelID> channelIDs;

	/**
	 * Constructs a new ephemeral checkpoint.
	 * 
	 * @param task
	 *        the task this checkpoint belongs to
	 * @param totalNumberOfOutputChannels
	 *        the total number of output channels connected to this checkpoint
	 * @param channelIDs 
	 * @param ephemeral
	 *        <code>true</code> if the checkpoint is initially ephemeral, <code>false</code> if the checkpoint shall be
	 *        persistent from the beginning
	 */
	public EphemeralChannelCheckpoint(final RuntimeTask task, final int totalNumberOfOutputChannels, ArrayList<ChannelID> channelIDs, final boolean ephemeral) {

		this.task = task;
		this.totalNumberOfOutputChannels = totalNumberOfOutputChannels;
		this.channelIDs = channelIDs;
		this.checkpointingDecision = (ephemeral ? CheckpointingDecisionState.UNDECIDED
			: CheckpointingDecisionState.CHECKPOINTING);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Created checkpoint for vertex " + task.getVertexID() + ", state " + this.checkpointingDecision);
		}

		if (this.checkpointingDecision == CheckpointingDecisionState.CHECKPOINTING) {
			createFile("_part");
			try {
				this.task.checkpointStateChanged(CheckpointState.PARTIAL);
			} catch (Exception e) {
				LOG.error(StringUtils.stringifyException(e));
			}
			for(int i = 0; i < this.channelIDs.size(); i++){	
				WriteChannelThread writeThread = new WriteChannelThread(FileBufferManager.getInstance(), this.task.getVertexID(), this.channelIDs.get(i),
					this.totalNumberOfOutputChannels);
				writeThread.start();
				this.writeThreads.put(this.channelIDs.get(i),writeThread);
			}
		}
	}

	private void createFile(String suffix) {
		String path =  CheckpointUtils.getLocalCheckpointPath().toUri().getPath()
				+ Path.SEPARATOR + CheckpointUtils.METADATA_PREFIX + "_" + this.task.getVertexID() +  suffix;
				FileOutputStream fos;
				try {
					fos = new FileOutputStream(path);
					fos.write(2);
					fos.close();
					
				} catch ( IOException e) {
					e.printStackTrace();
				}
				
	}

	public void destroy() {

		while (!this.queuedEnvelopes.isEmpty()) {

			final TransferEnvelope transferEnvelope = this.queuedEnvelopes.poll();
			final Buffer buffer = transferEnvelope.getBuffer();
			if (buffer != null) {
				buffer.recycleBuffer();
			}
		}

		Iterator<ChannelID> keyIter = this.writeThreads.keySet().iterator();
		while(keyIter.hasNext()){
			ChannelID channel = keyIter.next();
			WriteChannelThread writeThread = this.writeThreads.get(channel);
			if (writeThread != null) {
				writeThread.cancelAndDestroy();
				this.writeThreads.put(channel,null);
			}
		}
	}

	private void write() throws IOException, InterruptedException {

		while (!this.queuedEnvelopes.isEmpty()) {
			TransferEnvelope envelope = this.queuedEnvelopes.poll();
			WriteChannelThread writeThread = writeThreads.get(envelope.getSource());
					if(writeThread == null){
						writeThread = createWriteThread(envelope);
					}
			writeThread.write(envelope);
		}
	}

	private WriteChannelThread createWriteThread(TransferEnvelope envelope) {
		ChannelID source = envelope.getSource();
		WriteChannelThread writeThread = this.writeThreads.get(source) ;
		if( writeThread == null){
			
			writeThread = new WriteChannelThread(FileBufferManager.getInstance(), this.task.getVertexID(), source,
				this.totalNumberOfOutputChannels);
			writeThread.setName("WriteThread for " + task.getEnvironment().getTaskName() + " " + source );
			writeThread.start();
			this.writeThreads.put(source, writeThread);
		}
		return writeThread;
	}

	public void setCheckpointDecisionSynchronously(final boolean checkpointDecision) throws IOException,
			InterruptedException {
		LOG.info("Setting the Decision to " + checkpointDecision);
		if (this.checkpointingDecision != CheckpointingDecisionState.UNDECIDED) {
			System.out.println("Decision already set to " + this.checkpointingDecision + " while trying to set it to " + checkpointDecision);
			return;
		}

		if (checkpointDecision) {
			this.checkpointingDecision = CheckpointingDecisionState.CHECKPOINTING;
			// Write the data which has been queued so far and update checkpoint state
			createFile("_part");
			write();
			this.task.checkpointStateChanged(CheckpointState.PARTIAL);
		} else {
			this.checkpointingDecision = CheckpointingDecisionState.NO_CHECKPOINTING;
			// Simply destroy the checkpoint
			destroy();
			this.task.checkpointStateChanged(CheckpointState.NONE);
		}
	}

	public void forward(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException {

		if (this.checkpointingDecision == CheckpointingDecisionState.NO_CHECKPOINTING) {
			return;
		}

		final TransferEnvelope dup = transferEnvelope.duplicate();

		if (this.checkpointingDecision == CheckpointingDecisionState.UNDECIDED) {
			this.queuedEnvelopes.add(dup);

			if (this.asyncronousCheckpointDecisionRequested) {
				LOG.info("Requesting Decsion");
				new CheckpointDecision();
				setCheckpointDecisionSynchronously(CheckpointDecision.getDecision(task));
			}

		} else {
			WriteChannelThread writeThread = this.writeThreads.get(dup.getSource());
			if(writeThread == null){
				writeThread = createWriteThread(dup);
			}
			writeThread.write(dup);
		}
	}

	public boolean isUndecided() {

		return (this.checkpointingDecision == CheckpointingDecisionState.UNDECIDED);
	}

	public boolean hasDataLeft() throws IOException, InterruptedException {

		if (isUndecided()) {
			setCheckpointDecisionSynchronously(true);
		}
		
		Iterator<ChannelID> keyIter = this.writeThreads.keySet().iterator();
		boolean allnull = true;
		boolean dataLeft = false;
		while(keyIter.hasNext()){
			ChannelID channel = keyIter.next();
			WriteChannelThread writeThread = this.writeThreads.get(channel);
			if (writeThread != null) {
				
				allnull = false;
				if(writeThread.hasDataLeft()){
					dataLeft = true;
				}
			}
		}
		if (allnull) {
			return false;
		}
		

		if (dataLeft) {
			return true;
		}

		if (!this.completeCheckpointAnnounced) {
			this.completeCheckpointAnnounced = true;
			renameCheckpointPart();
			createFile(CheckpointUtils.COMPLETED_CHECKPOINT_SUFFIX);
			// Send notification that checkpoint is completed
			this.task.checkpointStateChanged(CheckpointState.COMPLETE);
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestCheckpointDecision() {

		this.asyncronousCheckpointDecisionRequested = true;
		if (this.checkpointingDecision == CheckpointingDecisionState.UNDECIDED) {
				LOG.info("Requesting Decsion");
				new CheckpointDecision();
				try {
					boolean decision = CheckpointDecision.getDecision(task);
					LOG.info("Setting it to " + decision);
					setCheckpointDecisionSynchronously(decision);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		
	}
	
	public void setNumberOfOutputChannels(int channels){
		this.totalNumberOfOutputChannels = channels;
		//this.writeThread.setNumberOfConnectedChannels(channels);
	}
	
	private boolean renameCheckpointPart() throws IOException {

		final Path oldFile = CheckpointUtils.getLocalCheckpointPath().suffix( Path.SEPARATOR + CheckpointUtils.METADATA_PREFIX + "_" + this.task.getVertexID() +  "_part");

		final Path newFile = CheckpointUtils.getLocalCheckpointPath().suffix(Path.SEPARATOR + CheckpointUtils.METADATA_PREFIX + "_"
			+ this.task.getVertexID() + "_0");

		if (!CheckpointUtils.getLocalCheckpointPath().getFileSystem().rename(oldFile, newFile)) {
			LOG.error("Unable to rename " + oldFile + " to " + newFile);
			return false;
		}

		return true;
	}
	
	
}
