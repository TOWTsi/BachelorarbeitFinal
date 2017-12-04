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
package eu.stratosphere.nephele.jobmanager.scheduler;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.checkpointing.CheckpointUtils;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGate;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult.ReturnCode;
import eu.stratosphere.nephele.taskmanager.TaskCheckpointResult;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * @author marrus
 */
public final class RecoveryLogic {

	/**
	 * The logger to report information and problems.
	 */
	private static final Log LOG = LogFactory.getLog(RecoveryLogic.class);
	private static boolean alreadyDuplicated = true;
	private static boolean usingCL =CheckpointUtils.getUsingCL();
	//Start Bachelorarbeit Vetesi
	private static int numberOfRecoverys = 0;
	//Ende Bachelorarbeit Vetesi
	/**
	 * Private constructor so class cannot be instantiated.
	 */
	private RecoveryLogic() {
	}

	public static boolean recover(final ExecutionVertex failedVertex,
			final Map<ExecutionVertexID, ExecutionVertex> verticesToBeRestarted,
			final Set<ExecutionVertex> assignedVertices, AbstractScheduler scheduler) throws InterruptedException {

		// Perform initial sanity check
		if (failedVertex.getExecutionState() != ExecutionState.FAILED) {
			LOG.error("Vertex " + failedVertex + " is requested to be recovered, but is not failed");
			return false;
		}

		final ExecutionGraph eg = failedVertex.getExecutionGraph();
		synchronized (eg) {

			LOG.info("Starting recovery for failed vertex " + failedVertex);
			final Set<ExecutionVertex> verticesToBeCanceled = new HashSet<ExecutionVertex>();
			//verticesToBeCanceled.add(failedVertex);
			final Set<ExecutionVertex> checkpointsToBeReplayed = new HashSet<ExecutionVertex>();

			findVerticesToRestart(failedVertex, verticesToBeCanceled, checkpointsToBeReplayed);

			// Restart all predecessors without checkpoint
			final Iterator<ExecutionVertex> cancelIterator = verticesToBeCanceled.iterator();
			while (cancelIterator.hasNext()) {

				final ExecutionVertex vertex = cancelIterator.next();
				
				if (vertex.compareAndUpdateExecutionState(ExecutionState.FINISHED, getStateToUpdate(vertex))) {
					LOG.info("Vertex " + vertex + " has already finished and will not be canceled");
					if (vertex.getExecutionState() == ExecutionState.ASSIGNED) {
						assignedVertices.add(vertex);
					}
					continue;
				}

				LOG.info(vertex + " is canceled by recovery logic");
				verticesToBeRestarted.put(vertex.getID(), vertex);
				final TaskCancelResult cancelResult = vertex.cancelTask();
				
				if (cancelResult.getReturnCode() != ReturnCode.SUCCESS
						&& cancelResult.getReturnCode() != ReturnCode.TASK_NOT_FOUND) {

					verticesToBeRestarted.remove(vertex.getID());
					LOG.error("Unable to cancel vertex" + cancelResult.getDescription());
					return false;
				}
			}

			LOG.info("Starting cache invalidation");

			// Invalidate the lookup caches
			if (!invalidateReceiverLookupCaches(failedVertex, verticesToBeCanceled)) {
				return false;
			}

			LOG.info("Cache invalidation complete");

			// Replay all necessary checkpoints
			final Iterator<ExecutionVertex> checkpointIterator = checkpointsToBeReplayed.iterator();

			while (checkpointIterator.hasNext()) {

				final ExecutionVertex checkpoint = checkpointIterator.next();
				//Do not reassing when the vertex is already starting. 
				if(checkpoint.getExecutionState() != ExecutionState.STARTING){
					checkpoint.updateExecutionState(ExecutionState.ASSIGNED);
				}
				assignedVertices.add(checkpoint);
			}


			// Restart failed vertex
			failedVertex.updateExecutionState(getStateToUpdate(failedVertex));

			if(!alreadyDuplicated){
				duplicateVertex(failedVertex, assignedVertices, scheduler, checkpointsToBeReplayed);
				alreadyDuplicated = true;
			}
		}


		if (failedVertex.getExecutionState() == ExecutionState.ASSIGNED) {
			assignedVertices.add(failedVertex);
		}

		//		Iterator<ExecutionVertexID> restartedIter = verticesToBeRestarted.keySet().iterator();
		//		while(restartedIter.hasNext()){
		//			ExecutionVertex restartVertex = verticesToBeRestarted.get(restartedIter.next());
		//			tdd = restartVertex.constructDeploymentDescriptor();
		//		}
		
		//Start Bachelorarbeit Vetesi
		numberOfRecoverys++;
		//Ende Bachelorarbeit Vetesi
		return true;
	}
	
	//Start Bachelorarbeit Vetesi
	public static int getNumberOfRecoverys() {
		return numberOfRecoverys;
	}
	//Ende Bachelorarbeit Vetesi

	private static void duplicateVertex(final ExecutionVertex failedVertex, final Set<ExecutionVertex> assignedVertices,
			AbstractScheduler scheduler, final Set<ExecutionVertex> checkpointsToBeReplayed) {
		ExecutionVertex dupVertex = failedVertex.duplicateRecoverVertex(false);
		alreadyDuplicated = true;

		scheduler.registerAdditionalVertex(dupVertex);
		/*	System.out.println(" RecoveryLogic:");
			System.out.println(" Name " + dupVertex.getName());
			System.out.println(" index " + dupVertex.getIndexInVertexGroup());*/
		dupVertex.getGroupVertex().addVertex(dupVertex);
		ExecutionGraph exg = scheduler.getExecutionGraphByID(dupVertex.getExecutionGraph().getJobID());
		//dupVertex.registerExecutionStateListener(new LocalStateExecutionListener((LocalScheduler) scheduler, dupVertex));

		exg.addEdgeForDuplicateVertex(failedVertex, dupVertex);// Register the listener object which will pass state changes on to the collector
		if(checkpointsToBeReplayed.contains(dupVertex.getPredecessor(0))){
			System.out.println("Predecessor of duplicate vertex will be replayed");
			//					for (int i = 0; i < dupVertex.getNumberOfPredecessors(); i++){
			//						ExecutionVertex pred = dupVertex.getPredecessor(i);
			//						pred.updateChannels();
			//					}
		}
	//	dupVertex.getAllocatedResource().assignVertexToResource(dupVertex);
//		dupVertex.setAllocatedResource(new AllocatedResource(DummyInstance
//			.createDummyInstance(dupVertex.getGroupVertex().getInstanceType()), dupVertex.getGroupVertex().getInstanceType(), null));

		try {
			scheduler.requestInstances(dupVertex.getGroupVertex().getExecutionStage());
		} catch (InstanceException e) {
			e.printStackTrace();
		}

		dupVertex.updateExecutionState(ExecutionState.ASSIGNED, "Started duplicate Vertex");
		if (dupVertex.getExecutionState() == ExecutionState.ASSIGNED) {
			assignedVertices.add(dupVertex);
		}
		//Update Deployment description of following Tasks as we added Edges.
		for (int suc =0; suc < dupVertex.getNumberOfSuccessors(); suc++){
			dupVertex.getSuccessor(suc).constructDeploymentDescriptor();
		}
		exg.repairInstanceAssignment();
	}

	
	static boolean hasInstanceAssigned(final ExecutionVertex vertex) {

		return !(vertex.getAllocatedResource().getInstance() instanceof DummyInstance);
	}

	
	
	private static ExecutionState getStateToUpdate(final ExecutionVertex vertex) {

		if (hasInstanceAssigned(vertex)) {
			return ExecutionState.ASSIGNED;
		}

		return ExecutionState.CREATED;
	}
	/**
	 * 
	 * @param failedVertex The failed Vertex 
	 * @param verticesToBeCanceled The list of verrtices that have to be cancled. It will be filled during the methodrun
	 * @param checkpointsToBeReplayed The list which represent the global consistent checkpoint
	 * @throws InterruptedException 
	 */
	private static void findVerticesToRestart(final ExecutionVertex failedVertex,
			final Set<ExecutionVertex> verticesToBeCanceled,
			final Set<ExecutionVertex> checkpointsToBeReplayed) throws InterruptedException {
		LOG.info("Finding Vertices to restart for " + failedVertex.getNameWithIndex() );
		final Queue<ExecutionVertex> verticesToTest = new ArrayDeque<ExecutionVertex>();
		final Set<ExecutionVertex> visited = new HashSet<ExecutionVertex>();
		verticesToTest.add(failedVertex);

		while (!verticesToTest.isEmpty()) {

			final ExecutionVertex vertex = verticesToTest.poll();
			LOG.info("Testing " + vertex.getNameWithIndex());
			// Predecessors must be either checkpoints or need to be restarted, too
			for (int j = 0; j < vertex.getNumberOfPredecessors(); j++) {

				final ExecutionVertex predecessor = vertex.getPredecessor(j);

				if (hasInstanceAssigned(predecessor)) {

					// At the moment, there no checkpoint decision for this vertex
					if (predecessor.getCheckpointState() == CheckpointState.UNDECIDED) {
						final TaskCheckpointResult result = predecessor.requestCheckpointDecision();
						if (result.getReturnCode() != ReturnCode.SUCCESS) {
							// Assume we do not have a checkpoint in this case
							predecessor.updateCheckpointState(CheckpointState.NONE);
						} else {

							try {
								predecessor.waitForCheckpointStateChange(CheckpointState.UNDECIDED, 60000L);
								
							} catch (InterruptedException e) {
							}

							if (predecessor.getCheckpointState() == CheckpointState.UNDECIDED) {
								LOG.info(predecessor.getNameWithIndex() + " is still undecided");
								predecessor.updateCheckpointState(CheckpointState.NONE);
							}
						}
					}

					final CheckpointState checkpointState = predecessor.getCheckpointState();

					switch (checkpointState) {
					case NONE:
						verticesToBeCanceled.add(predecessor);
						LOG.info("Added " + predecessor +" to be cancled");
						break;
					case COMPLETE:
						//As the task is already finished in this case, put it into the canceled group to trigger the right behaivior
						verticesToBeCanceled.add(predecessor);
						LOG.info("Added " + predecessor +" for replay");
						continue;
					case PARTIAL:
						checkpointsToBeReplayed.add(predecessor);
						LOG.info("Added " + predecessor +" for replay partial");
						continue;
					default:
						LOG.error(predecessor + " has unexpected checkpoint state " + checkpointState);
					}
				}

				if (!visited.contains(predecessor)) {
					verticesToTest.add(predecessor);
				}
			}
			visited.add(vertex);
			
		}
		
		//If we are not using Consumption logging, it is necessary to restart all followers of the restarted vertices
		if(!usingCL){
			
			final Queue<ExecutionVertex> toTest = new ArrayDeque<ExecutionVertex>();
			final Set<ExecutionVertex> alreadyvisited = new HashSet<ExecutionVertex>();
			
			toTest.addAll(checkpointsToBeReplayed);
			while (!toTest.isEmpty()) {

				final ExecutionVertex vertex = toTest.poll();
				//System.out.println("Testing " + vertex.getName());
				// Successors have to be restarted
				for (int j = 0; j < vertex.getNumberOfSuccessors(); j++) {
					ExecutionVertex succ = vertex.getSuccessor(j);
					if(!alreadyvisited.contains(succ) && succ != failedVertex){
						LOG.info("Non CL. Adding: "  + succ.getName() + " for restart");
						verticesToBeCanceled.add(succ);
						
						if(!toTest.contains(succ)){
							toTest.add(succ);
						}
					}
				}
				alreadyvisited.add(vertex);
			}
		}
	}

	private static final boolean invalidateReceiverLookupCaches(final ExecutionVertex failedVertex,
			final Set<ExecutionVertex> verticesToBeCanceled) throws InterruptedException {

		final Map<AbstractInstance, Set<ChannelID>> entriesToInvalidate = new HashMap<AbstractInstance, Set<ChannelID>>();

		collectCacheEntriesToInvalidate(failedVertex, entriesToInvalidate);
		for (final Iterator<ExecutionVertex> it = verticesToBeCanceled.iterator(); it.hasNext();) {
			collectCacheEntriesToInvalidate(it.next(), entriesToInvalidate);
		}

		final Iterator<Map.Entry<AbstractInstance, Set<ChannelID>>> it = entriesToInvalidate.entrySet().iterator();

		while (it.hasNext()) {

			final Map.Entry<AbstractInstance, Set<ChannelID>> entry = it.next();
			final AbstractInstance instance = entry.getKey();

			try {
				instance.invalidateLookupCacheEntries(entry.getValue());
			} catch (IOException ioe) {
				LOG.error(StringUtils.stringifyException(ioe));
				return false;
			}
		}

		return true;
	}

	private static void collectCacheEntriesToInvalidate(final ExecutionVertex vertex,
			final Map<AbstractInstance, Set<ChannelID>> entriesToInvalidate) {

		final int numberOfOutputGates = vertex.getNumberOfOutputGates();
		for (int i = 0; i < numberOfOutputGates; ++i) {

			final ExecutionGate outputGate = vertex.getOutputGate(i);
			for (int j = 0; j < outputGate.getNumberOfEdges(); ++j) {

				final ExecutionEdge outputChannel = outputGate.getEdge(j);
				if (outputChannel.getChannelType() == ChannelType.FILE) {
					// Connected vertex is not yet running
					continue;
				}

				final ExecutionVertex connectedVertex = outputChannel.getInputGate().getVertex();
				if (connectedVertex == null) {
					LOG.error("Connected vertex is null");
					continue;
				}

				final AbstractInstance instance = connectedVertex.getAllocatedResource().getInstance();
				if (instance instanceof DummyInstance) {
					continue;
				}

				Set<ChannelID> channelIDs = entriesToInvalidate.get(instance);
				if (channelIDs == null) {
					channelIDs = new HashSet<ChannelID>();
					entriesToInvalidate.put(instance, channelIDs);
				}

				channelIDs.add(outputChannel.getInputChannelID());
			}
		}

		for (int i = 0; i < vertex.getNumberOfInputGates(); ++i) {

			final ExecutionGate inputGate = vertex.getInputGate(i);
			for (int j = 0; j < inputGate.getNumberOfEdges(); ++j) {

				final ExecutionEdge inputChannel = inputGate.getEdge(j);
				if (inputChannel.getChannelType() == ChannelType.FILE) {
					// Connected vertex is not running anymore
					continue;
				}

				final ExecutionVertex connectedVertex = inputChannel.getOutputGate().getVertex();
				if (connectedVertex == null) {
					LOG.error("Connected vertex is null");
					continue;
				}

				final AbstractInstance instance = connectedVertex.getAllocatedResource().getInstance();
				if (instance instanceof DummyInstance) {
					continue;
				}

				Set<ChannelID> channelIDs = entriesToInvalidate.get(instance);
				if (channelIDs == null) {
					channelIDs = new HashSet<ChannelID>();
					entriesToInvalidate.put(instance, channelIDs);
				}

				channelIDs.add(inputChannel.getOutputChannelID());
			}
		}
	}
}
