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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.io.RuntimeInputGate;
import eu.stratosphere.nephele.io.RuntimeOutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;
import eu.stratosphere.nephele.types.Record;

public final class CheckpointDecision {

	private static final Log LOG = LogFactory.getLog(CheckpointDecision.class);
	private static final long NANO_TO_MILLISECONDS = 1000 * 1000;
	public static boolean getDecision(final RuntimeTask task) {

		switch (CheckpointUtils.getCheckpointMode()) {
		case NEVER:
			return false;
		case ALWAYS:
			return true;
		case NETWORK:
			return isNetworkTask(task);
		case DYNAMIC:
			return getDynamicDecision(task);
		}

		return false;
	}

	private static boolean getDynamicDecision(RuntimeTask task) {
		RuntimeEnvironment environment = task.getRuntimeEnvironment();
		
		// Get CPU-Usertime in percent
		ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();	
		  final long timestamp = System.currentTimeMillis();
		long threaduserCPU = 	threadBean.getThreadUserTime(environment.getExecutingThread().getId());
		LOG.info("ThreadCPU " + threaduserCPU + " " + environment.getExecutingThread().getName() +" time " + System.currentTimeMillis() + " " + environment.getStartTime());
		long userCPU = (threaduserCPU / NANO_TO_MILLISECONDS) * 100 
				/ (timestamp - environment.getStartTime());
		LOG.info("CPUusage is " + userCPU);
		
		double inputDataSize = getInputAmount(environment);
		if(inputDataSize != 0){
		double outputDataSize = getOutputAmount(environment);
		LOG.info("IN: " + inputDataSize + " OUT: " + outputDataSize);
		//calculate rate of Input and Output
		double outInRate = outputDataSize/inputDataSize;
		LOG.info("OutInrate is " + outInRate);
		if( outInRate <= CheckpointUtils.getLowerBound()){
			LOG.info("CREATING CHECKPOINT");
			return true;
		}
		if( outInRate >= CheckpointUtils.getUpperBound()){
			return false;
		} 
		}
	
		
		if(userCPU >= 80){
			LOG.info("CREATING CHECKPOINT");
			return true;
		}
		//default is no checkpointing
		return false;
	}

	private static double getOutputAmount(RuntimeEnvironment environment) {
		//get amount of data OUTput
		double outputDataSize = 0;
		for(int i=0; i<environment.getNumberOfOutputGates(); i++){
			
			RuntimeOutputGate<? extends Record> outputGate = environment.getOutputGate(i);
			for(int j=0;j<outputGate.getNumberOfOutputChannels(); j++){
					outputDataSize +=outputGate.getOutputChannel(j).getAmountOfDataTransmitted();
				}
			}
		return outputDataSize;
	}

	private static double getInputAmount(RuntimeEnvironment environment) {
		
		//get amount of data INput
		boolean allclosed = true; //if all inputgates are closed the task seems to be a streambreaker
		double inputDataSize = 0;
		for(int i=0; i<environment.getNumberOfInputGates(); i++){
			
			RuntimeInputGate<? extends Record> inputGate = environment.getInputGate(i);
			for(int j=0;j<inputGate.getNumberOfInputChannels(); j++){
				AbstractInputChannel<? extends Record> channel = inputGate.getInputChannel(j);
					try {
						if(!channel.isClosed()){
							
							allclosed=false;
						}
					} catch (IOException e) {
						e.printStackTrace();
					} 
					inputDataSize += channel.getAmountOfDataTransmitted();
				}
			}
		if(allclosed){
			//streambreaker do not use input/output rates
			return 0;
		}
		return inputDataSize;
	}

	private static boolean isNetworkTask(final RuntimeTask task) {

		final RuntimeEnvironment environment = task.getRuntimeEnvironment();

		for (int i = 0; i < environment.getNumberOfOutputGates(); ++i) {

			if (environment.getOutputGate(i).getChannelType() == ChannelType.NETWORK) {
				LOG.info(environment.getTaskNameWithIndex() + " is a network task");
				return true;
			}
		}

		return false;
	}
}
