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

package eu.stratosphere.nephele.protocols;

import java.io.IOException;
import eu.stratosphere.nephele.client.JobCancelResult;
import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.rpc.RPCProtocol;

/**
 * The JobManagementProtocol specifies methods required to manage
 * Nephele jobs from a job client.
 * 
 * @author warneke
 */
public interface JobManagementProtocol extends RPCProtocol {

	/**
	 * Submits the specified job to the job manager.
	 * 
	 * @param job
	 *        the job to be executed
	 * @return a protocol of the job submission including the success status
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the submit request
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 */
	JobSubmissionResult submitJob(JobGraph job) throws IOException, InterruptedException;

	//Start Bachelorarbeit Vetesi
	
	/**
	 * 
	 * @param vertexName
	 * @throws IOException
	 * @throws InterruptedException
	 * @author vetesi
	 */
	void tryToKillTask(String vertexName) throws IOException, InterruptedException;
	
	void tryToKillInstance(String vertexName) throws IOException, InterruptedException;
	
	boolean getFailureGeneratorIsReady() throws IOException, InterruptedException;
	
	void setFailureGeneratorIsReady(boolean ready) throws IOException, InterruptedException;
	
	int getItemChoice() throws IOException, InterruptedException;
	
	void setDataReport(String finalReport) throws IOException, InterruptedException;
	
	/**
	 * Signals the JobClient that the failureConfiguration is ready and could be started now
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @author vetesi
	 */
	//boolean runFailureGenerator() throws IOException, InterruptedException;
	
	/**
	 * Informs the JobClient, how often the job should be reloaded 
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @author vetesi
	 */
	int getNumberOfReRuns() throws IOException, InterruptedException;
	
	/**
	 * After a DebugJob is finished we set the number of reRuns back
	 * @param reRuns
	 * @throws IOException
	 * @throws InterruptedException
	 * @author vetesi
	 */
	void setNumberOfReRuns(int reRuns) throws IOException, InterruptedException;
	
	//Ende Bachelorarbeit Vetesi
	
	/**
	 * Retrieves the current status of the job specified by the given ID. Consecutive
	 * calls of this method may result in duplicate events. The caller must take care
	 * of this.
	 * 
	 * @param jobID
	 *        the ID of the job
	 * @param minimumSequenceNumber
	 *        the minimum sequence number of the events to get
	 * @return a {@link JobProgressResult} object including the current job progress
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the request
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 */
	JobProgressResult getJobProgress(JobID jobID, long minimumSequenceNumber) throws IOException, InterruptedException;

	/**
	 * Requests to cancel the job specified by the given ID.
	 * 
	 * @param jobID
	 *        the ID of the job
	 * @return a {@link JobCancelResult} containing the result of the cancel request
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the request
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 */
	JobCancelResult cancelJob(JobID jobID) throws IOException, InterruptedException;

	/**
	 * Returns the recommended interval in seconds in which a client
	 * is supposed to poll for progress information.
	 * 
	 * @return the interval in seconds
	 * @throws IOException
	 *         thrown if an error occurred while transmitting the request
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of the remote procedure call
	 */
	int getRecommendedPollingInterval() throws IOException, InterruptedException;
}
