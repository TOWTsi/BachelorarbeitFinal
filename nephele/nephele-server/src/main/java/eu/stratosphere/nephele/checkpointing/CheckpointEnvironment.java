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

package eu.stratosphere.nephele.checkpointing;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionObserver;
import eu.stratosphere.nephele.execution.FailureReport;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordFactory;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.Record;

final class CheckpointEnvironment implements Environment {

	private final ExecutionVertexID vertexID;

	private final Environment environment;

	private final boolean hasLocalCheckpoint;

	private final boolean hasCompleteCheckpoint;

	private final Map<ChannelID, ReplayOutputChannelBroker> outputBrokerMap;

	/**
	 * The observer object for the task's execution.
	 */
	private volatile ExecutionObserver executionObserver = null;

	private volatile ReplayThread executingThread = null;

	private ArrayList<ArrayList<Long>> failedRecords;

	CheckpointEnvironment(final ExecutionVertexID vertexID, final Environment environment,
			final boolean hasLocalCheckpoint, final boolean hasCompleteCheckpoint,
			final Map<ChannelID, ReplayOutputChannelBroker> outputBrokerMap) {

		this.vertexID = vertexID;
		this.environment = environment;
		this.hasLocalCheckpoint = hasLocalCheckpoint;
		this.hasCompleteCheckpoint = hasCompleteCheckpoint;
		this.outputBrokerMap = outputBrokerMap;
	}
	
	CheckpointEnvironment(final ExecutionVertexID vertexID, final Environment environment,
		final boolean hasLocalCheckpoint, final boolean hasCompleteCheckpoint,
		final Map<ChannelID, ReplayOutputChannelBroker> outputBrokerMap, ArrayList<ArrayList<Long>> failedRecords) {

	this.vertexID = vertexID;
	this.environment = environment;
	this.hasLocalCheckpoint = hasLocalCheckpoint;
	this.hasCompleteCheckpoint = hasCompleteCheckpoint;
	this.outputBrokerMap = outputBrokerMap;
	this.failedRecords = failedRecords;
}
	/**
	 * Sets the execution observer for this environment.
	 * 
	 * @param executionObserver
	 *        the execution observer for this environment
	 */
	void setExecutionObserver(final ExecutionObserver executionObserver) {
		this.executionObserver = executionObserver;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.environment.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getTaskConfiguration() {

		return this.environment.getTaskConfiguration();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getJobConfiguration() {

		return this.environment.getJobConfiguration();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getCurrentNumberOfSubtasks() {

		return this.environment.getCurrentNumberOfSubtasks();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getIndexInSubtaskGroup() {

		return this.environment.getIndexInSubtaskGroup();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final Thread userThread) {

		throw new IllegalStateException("Checkpoint replay task called userThreadStarted");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final Thread userThread) {

		throw new IllegalStateException("Checkpoint replay task called userThreadFinished");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplitProvider getInputSplitProvider() {

		throw new IllegalStateException("Checkpoint replay task called getInputSplitProvider");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOManager getIOManager() {

		throw new IllegalStateException("Checkpoint replay task called getIOManager");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MemoryManager getMemoryManager() {

		throw new IllegalStateException("Checkpoint replay task called getMemoryManager");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTaskName() {

		return this.environment.getTaskName();
	}
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTaskNameWithIndex() {

		return this.environment.getTaskNameWithIndex();
	}
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfOutputGates() {

		return this.environment.getNumberOfOutputGates();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputGates() {

		return this.environment.getNumberOfInputGates();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfOutputChannels() {

		return this.environment.getNumberOfOutputChannels();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputChannels() {

		return this.environment.getNumberOfInputChannels();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends Record> OutputGate<T> createAndRegisterOutputGate(final ChannelSelector<T> selector,
			final boolean isBroadcast) {

		throw new IllegalStateException("Checkpoint replay task called createOutputGate");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends Record> InputGate<T> createAndRegisterInputGate(final RecordFactory<T> recordFactory) {

		throw new IllegalStateException("Checkpoint replay task called createInputGate");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDs() {

		return this.environment.getOutputChannelIDs();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDs() {

		return this.environment.getInputChannelIDs();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getOutputGateIDs() {

		return this.environment.getOutputGateIDs();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getInputGateIDs() {

		return this.environment.getInputGateIDs();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDsOfGate(final GateID gateID) {

		return this.environment.getOutputChannelIDsOfGate(gateID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDsOfGate(final GateID gateID) {

		return this.environment.getInputChannelIDsOfGate(gateID);
	}

	/**
	 * Returns the thread which is assigned to executes the replay task
	 * 
	 * @return the thread which is assigned to execute the replay task
	 */
	public ReplayThread getExecutingThread() {

		synchronized (this) {

			if (this.executingThread == null) {
				this.executingThread = new ReplayThread(this.vertexID, this.executionObserver, getTaskName(),
					this.hasLocalCheckpoint, this.hasCompleteCheckpoint, this.outputBrokerMap);
			}

			return this.executingThread;
		}
	}
	public FailureReport getFailureReport() {
		
		return this.environment.getFailureReport();
	}

	@Override
	public OutputGate<? extends Record> getOutputGate(GateID gateID) {
		return this.environment.getOutputGate(gateID);
	}
	
}
