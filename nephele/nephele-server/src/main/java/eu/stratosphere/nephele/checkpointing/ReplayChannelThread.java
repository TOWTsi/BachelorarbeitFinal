package eu.stratosphere.nephele.checkpointing;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;

import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.fs.FileChannelWrapper;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.transferenvelope.CheckpointDeserializer;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.util.StringUtils;

public class ReplayChannelThread extends Thread {

	private static final String REPLAY_SUFFIX = " (Replay)";

	/**
	 * The interval to sleep in case a communication channel is not yet entirely set up (in milliseconds).
	 */
	private static final int SLEEPINTERVAL = 100;

	/**
	 * The buffer size in bytes to use for the meta data file channel.
	 */
	private static final int BUFFER_SIZE = 4096;

	private final ExecutionVertexID vertexID;

	private final ExecutionObserver executionObserver;

	private final boolean isCheckpointLocal;

	private final boolean isCheckpointComplete;

	private final  ReplayOutputChannelBroker outputBroker;

	private ChannelID channlID;

	private ReplayThread replayThread;

	ReplayChannelThread(ReplayThread replayThread, final ExecutionVertexID vertexID, final ExecutionObserver executionObserver, final String taskName,
			final boolean isCheckpointLocal, final boolean isCheckpointComplete, final ChannelID channelID,
			 ReplayOutputChannelBroker outputBroker) {
		super((taskName == null ? "Unkown" : taskName) + REPLAY_SUFFIX);
		this.setName(taskName +  " channel " + channelID + REPLAY_SUFFIX );
		this.replayThread = replayThread;
		this.vertexID = vertexID;
		this.executionObserver = executionObserver;
		this.isCheckpointLocal = isCheckpointLocal;
		this.isCheckpointComplete = isCheckpointComplete;
		this.channlID = channelID;
		this.outputBroker = outputBroker;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {


		try {

			// Replay the actual checkpoint
			replayCheckpoint();

			

		} catch (Exception e) {


			e.printStackTrace();
		}

		this.replayThread.announceFinish(this.channlID);
	}

	
	private void replayCheckpoint() throws Exception {

		final CheckpointDeserializer deserializer = new CheckpointDeserializer(this.vertexID, !this.isCheckpointLocal);

		final Path checkpointPath = this.isCheckpointLocal ? CheckpointUtils.getLocalCheckpointPath() : CheckpointUtils
			.getDistributedCheckpointPath();

		if (checkpointPath == null) {
			throw new IOException("Cannot determine checkpoint path for vertex " + this.vertexID);
		}

		// The file system the checkpoint's meta data is stored on
		final FileSystem fileSystem = checkpointPath.getFileSystem();

		int metaDataIndex = 0;

		Buffer firstDeserializedFileBuffer = null;
		FileChannel fileChannel = null;

		try {

			while (true) {

				// Try to locate the meta data file
				final Path metaDataFile = checkpointPath.suffix(Path.SEPARATOR + CheckpointUtils.METADATA_PREFIX
					+ "_" + this.vertexID + "_" + this.channlID + "_"+ metaDataIndex);

				while (!fileSystem.exists(metaDataFile)) {

					// Try to locate the final meta data file
					final Path finalMetaDataFile = checkpointPath.suffix(Path.SEPARATOR
						+ CheckpointUtils.METADATA_PREFIX
						+ "_" + this.vertexID + "_" + this.channlID + "_final");

					if (fileSystem.exists(finalMetaDataFile)) {
						return;
						
					}

					if (this.isCheckpointComplete) {
						throw new FileNotFoundException("Cannot find meta data file " + metaDataIndex
							+ " for checkpoint of vertex " + this.vertexID);
					}

					// Wait for the file to be created
					Thread.sleep(1000);

					if (this.executionObserver.isCanceled()) {
						return;
					}
				}

				fileChannel = getFileChannel(fileSystem, metaDataFile);

				while (true) {
					try {
						deserializer.read(fileChannel);

						final TransferEnvelope transferEnvelope = deserializer.getFullyDeserializedTransferEnvelope();
						if (transferEnvelope != null) {

				
							if (this.outputBroker == null) {
								throw new IOException("Cannot find output broker for channel "
									+ transferEnvelope.getSource());
							}

							final Buffer srcBuffer = transferEnvelope.getBuffer();
							if (srcBuffer != null) {

								// Prevent underlying file from being closed
								if (firstDeserializedFileBuffer == null) {
									firstDeserializedFileBuffer = srcBuffer.duplicate();
								}

								if (transferEnvelope.getSequenceNumber() < this.outputBroker.getNextEnvelopeToSend()) {
									srcBuffer.recycleBuffer();
									continue;
								}

								final Buffer destBuffer =this.outputBroker.requestEmptyBufferBlocking(srcBuffer.size());
								srcBuffer.copyToBuffer(destBuffer);
								transferEnvelope.setBuffer(destBuffer);
								srcBuffer.recycleBuffer();
							}
							
							this.outputBroker.outputEnvelope(transferEnvelope);

							if (this.executionObserver.isCanceled()) {
								return;
							}
						}
					} catch (EOFException eof) {
						// Close the file channel
						fileChannel.close();
						fileChannel = null;
						// Increase the index of the meta data file
						++metaDataIndex;
						break;
					}
				}
			}

		} finally {
			if (firstDeserializedFileBuffer != null) {
				firstDeserializedFileBuffer.recycleBuffer();
				firstDeserializedFileBuffer = null;
			}
			if (fileChannel != null) {
				fileChannel.close();
				fileChannel = null;
			}
		}
	}

	private FileChannel getFileChannel(final FileSystem fs, final Path p) throws IOException {

		// Bypass FileSystem API for local checkpoints
		if (this.isCheckpointLocal) {

			final URI uri = p.toUri();
			@SuppressWarnings("resource")
			FileInputStream fileInputStream = new FileInputStream(uri.getPath());
			return fileInputStream.getChannel();
		}

		return new FileChannelWrapper(fs, p, BUFFER_SIZE, (short) -1);
	}
}