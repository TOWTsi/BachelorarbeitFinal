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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.stratosphere.nephele.jobmanager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.client.AbstractJobResult;
import eu.stratosphere.nephele.client.JobCancelResult;
import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.client.AbstractJobResult.ReturnCode;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.deployment.TaskDeploymentDescriptor;
import eu.stratosphere.nephele.discovery.DiscoveryException;
import eu.stratosphere.nephele.discovery.DiscoveryService;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGraphIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.executiongraph.JobStatusListener;
import eu.stratosphere.nephele.failuregenerator.ConfigurationHandler;
import eu.stratosphere.nephele.failuregenerator.FailureGenerator;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.DummyInstance;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.local.LocalInstanceManager;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.AbstractJobInputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobOutputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobmanager.scheduler.AbstractScheduler;
import eu.stratosphere.nephele.jobmanager.scheduler.SchedulingException;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitManager;
import eu.stratosphere.nephele.jobmanager.splitassigner.InputSplitWrapper;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGraphIterator;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.multicast.MulticastManager;
import eu.stratosphere.nephele.plugins.JobManagerPlugin;
import eu.stratosphere.nephele.plugins.PluginID;
import eu.stratosphere.nephele.plugins.PluginManager;
import eu.stratosphere.nephele.profiling.JobManagerProfiler;
import eu.stratosphere.nephele.profiling.ProfilingListener;
import eu.stratosphere.nephele.profiling.ProfilingUtils;
import eu.stratosphere.nephele.protocols.ChannelLookupProtocol;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.protocols.InputSplitProviderProtocol;
import eu.stratosphere.nephele.protocols.JobManagementProtocol;
import eu.stratosphere.nephele.protocols.JobManagerProtocol;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.nephele.rpc.ServerTypeUtils;
import eu.stratosphere.nephele.taskmanager.AbstractTaskResult;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskCheckpointState;
import eu.stratosphere.nephele.taskmanager.TaskExecutionState;
import eu.stratosphere.nephele.taskmanager.TaskKillResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;
import eu.stratosphere.nephele.taskmanager.routing.ConnectionInfoLookupResponse;
import eu.stratosphere.nephele.taskmanager.routing.RemoteReceiver;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.types.StringRecord;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * In Nephele the job manager is the central component for communication with clients, creating
 * schedules for incoming jobs and supervise their execution. A job manager may only exist once in
 * the system and its address must be known the clients.
 * Task managers can discover the job manager by means of an UDP broadcast and afterwards advertise
 * themselves as new workers for tasks.
 * 
 * @author warneke
 */
public class JobManager implements DeploymentManager, ExtendedManagementProtocol, InputSplitProviderProtocol,
		JobManagerProtocol, ChannelLookupProtocol, JobStatusListener {

	private static final Log LOG = LogFactory.getLog(JobManager.class);

	private final RPCService rpcService;

	private final JobManagerProfiler profiler;

	private final EventCollector eventCollector;

	private final InputSplitManager inputSplitManager;

	private final AbstractScheduler scheduler;

	private final MulticastManager multicastManager;

	private InstanceManager instanceManager;

	private final Map<PluginID, JobManagerPlugin> jobManagerPlugins;

	private final int recommendedClientPollingInterval;

	private final ExecutorService executorService = Executors.newCachedThreadPool();

	private final static int SLEEPINTERVAL = 1000;

	private final static int FAILURERETURNCODE = -1;

	private final AtomicBoolean isShutdownRequested = new AtomicBoolean(false);

	private final AtomicBoolean isShutdownComplete = new AtomicBoolean(false);

	private boolean killed = false;
	
	//Start Bachelorarbeit Vetesi
	
	private int numberOfInputTasks;
	
	private int numberOfInnerTasks;
	
	private int numberOfOutputTasks;
	
	private final ConfigurationHandler configurationHandler = new ConfigurationHandler();
	
	private final FailureGenerator failureGenerator = new FailureGenerator();
	
	private LinkedList<ManagementVertex> killingList;
	
	private int numberOfReRuns = -1;
	
	private boolean isKillingRequested = false;
	
	private boolean selectionFromGUIisReady = false;
	
	private boolean initiationFromGUIisStarted = false;
	
	private LinkedList<ManagementVertex> verticeslist = new LinkedList<ManagementVertex>();
	
	private LinkedList<String> instanceNames = new LinkedList<String>();
	
	private LinkedList<String> instanceNamesToKill = new LinkedList<String>();
	
	private LinkedList<Integer> selectedConfigFromGUI = new LinkedList<Integer>();
	
	private LinkedList<Integer> selectedDelayTimeFromGUI = new LinkedList<Integer>();
	
	private LinkedList<Integer> delayTime = new LinkedList<Integer>();
	
	private LinkedList<String> eventNames = new LinkedList<String>();
	
	private LinkedList<String> alreadyVisitedEventNames = new LinkedList<String>();
	
	private LinkedList<LinkedList<ManagementVertex>> allDifferentKindOfInstancesDuringJob = new LinkedList<LinkedList<ManagementVertex>>();
	
	private final int TASK_FAILURE = 0;
	
	private final int INSTANCE_FAILURE = 1;
	
	private int failureBranch;
	
	private String filenameToSaveDataLocaly;
	//Ende Bachelorarbeit Vetesi
	
	/**
	 * Constructs a new job manager, starts its discovery service and its IPC service.
	 */
	public JobManager(final String configDir, final String executionMode) {

		// First, try to load global configuration
		GlobalConfiguration.loadConfiguration(configDir);

		final String ipcAddressString = GlobalConfiguration
			.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);

		InetAddress ipcAddress = null;
		if (ipcAddressString != null) {
			try {
				ipcAddress = InetAddress.getByName(ipcAddressString);
			} catch (UnknownHostException e) {
				LOG.fatal("Cannot convert " + ipcAddressString + " to an IP address: "
					+ StringUtils.stringifyException(e));
				System.exit(FAILURERETURNCODE);
			}
		}

		final int ipcPort = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
			ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		// First of all, start discovery manager
		try {
			DiscoveryService.startDiscoveryService(ipcAddress, ipcPort);
		} catch (DiscoveryException e) {
			LOG.fatal("Cannot start discovery manager: " + StringUtils.stringifyException(e));
			System.exit(FAILURERETURNCODE);
		}

		// Read the suggested client polling interval
		this.recommendedClientPollingInterval = GlobalConfiguration.getInteger("jobclient.polling.internval", 1);

		// Load the job progress collector
		this.eventCollector = new EventCollector(this.recommendedClientPollingInterval);

		// Load the input split manager
		this.inputSplitManager = new InputSplitManager();

		// Determine own RPC address
		final InetSocketAddress rpcServerAddress = new InetSocketAddress(ipcAddress, ipcPort);

		// Start job manager's RPC server
		RPCService rpcService = null;
		try {
			final int handlerCount = GlobalConfiguration.getInteger("jobmanager.rpc.numhandler", 3);
			rpcService = new RPCService(rpcServerAddress.getPort(), handlerCount,
				ServerTypeUtils.getRPCTypesToRegister());
		} catch (IOException ioe) {
			LOG.fatal("Cannot start RPC server: " + StringUtils.stringifyException(ioe));
			System.exit(FAILURERETURNCODE);
		}
		this.rpcService = rpcService;

		// Add callback handlers to the RPC service
		this.rpcService.setProtocolCallbackHandler(ChannelLookupProtocol.class, this);
		this.rpcService.setProtocolCallbackHandler(ExtendedManagementProtocol.class, this);
		this.rpcService.setProtocolCallbackHandler(InputSplitProviderProtocol.class, this);
		this.rpcService.setProtocolCallbackHandler(JobManagementProtocol.class, this);
		this.rpcService.setProtocolCallbackHandler(JobManagerProtocol.class, this);

		LOG.info("Starting job manager in " + executionMode + " mode");

		// Load the plugins
		this.jobManagerPlugins = PluginManager.getJobManagerPlugins(configDir);

		// Try to load the instance manager for the given execution mode
		// Try to load the scheduler for the given execution mode
		if ("local".equals(executionMode)) {
			try {
				this.instanceManager = new LocalInstanceManager(configDir, this.rpcService);
			} catch (RuntimeException rte) {
				LOG.fatal("Cannot instantiate local instance manager: " + StringUtils.stringifyException(rte));
				System.exit(FAILURERETURNCODE);
			}
		} else {
			final String instanceManagerClassName = JobManagerUtils.getInstanceManagerClassName(executionMode);
			LOG.info("Trying to load " + instanceManagerClassName + " as instance manager");
			this.instanceManager = JobManagerUtils.loadInstanceManager(instanceManagerClassName, this.rpcService);
			if (this.instanceManager == null) {
				LOG.fatal("Unable to load instance manager " + instanceManagerClassName);
				System.exit(FAILURERETURNCODE);
			}
		}

		// Try to load the scheduler for the given execution mode
		final String schedulerClassName = JobManagerUtils.getSchedulerClassName(executionMode);
		LOG.info("Trying to load " + schedulerClassName + " as scheduler");

		// Try to get the instance manager class name
		this.scheduler = JobManagerUtils.loadScheduler(schedulerClassName, this, this.instanceManager);
		if (this.scheduler == null) {
			LOG.fatal("Unable to load scheduler " + schedulerClassName);
			System.exit(FAILURERETURNCODE);
		}

		// Create multicastManager
		this.multicastManager = new MulticastManager(this.scheduler);

		// Load profiler if it should be used
		if (GlobalConfiguration.getBoolean(ProfilingUtils.ENABLE_PROFILING_KEY, false)) {
			final String profilerClassName = GlobalConfiguration.getString(ProfilingUtils.JOBMANAGER_CLASSNAME_KEY,
				null);
			if (profilerClassName == null) {
				LOG.fatal("Cannot find class name for the profiler");
				System.exit(FAILURERETURNCODE);
			}
			this.profiler = ProfilingUtils.loadJobManagerProfiler(profilerClassName, ipcAddress);
			if (this.profiler == null) {
				LOG.fatal("Cannot load profiler");
				System.exit(FAILURERETURNCODE);
			}
		} else {
			this.profiler = null;
			LOG.debug("Profiler disabled");
		}

		// Add shutdown hook for clean up tasks
		Runtime.getRuntime().addShutdownHook(new JobManagerCleanUp(this));

	}

	/**
	 * This is the main
	 */
	public void runTaskLoop() {

		while (!this.isShutdownRequested.get()) {

			// Sleep
			try {
				Thread.sleep(SLEEPINTERVAL);
			} catch (InterruptedException e) {
				if (this.isShutdownRequested.get()) {
					break;
				}
			}

			// Do nothing here
		}

		synchronized (this.isShutdownComplete) {
			while (!this.isShutdownComplete.get()) {
				try {
					this.isShutdownComplete.wait();
				} catch (InterruptedException e) {
					LOG.debug(StringUtils.stringifyException(e));
					return;
				}
			}
		}
	}

	public void shutDown() {

		if (!this.isShutdownRequested.compareAndSet(false, true)) {
			return;
		}

		// Stop instance manager
		if (this.instanceManager != null) {
			this.instanceManager.shutdown();
		}

		// Stop the discovery service
		DiscoveryService.stopDiscoveryService();

		// Stop profiling if enabled
		if (this.profiler != null) {
			this.profiler.shutdown();
		}

		// Stop RPC service
		if (this.rpcService != null) {
			this.rpcService.shutDown();
		}

		// Stop the executor service
		if (this.executorService != null) {
			this.executorService.shutdown();
			try {
				this.executorService.awaitTermination(5000L, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(StringUtils.stringifyException(e));
				}
			}
		}

		// Stop the plugins
		final Iterator<JobManagerPlugin> it = this.jobManagerPlugins.values().iterator();
		while (it.hasNext()) {
			it.next().shutdown();
		}

		// Stop and clean up the job progress collector
		if (this.eventCollector != null) {
			this.eventCollector.shutdown();
		}

		// Finally, shut down the scheduler
		if (this.scheduler != null) {
			this.scheduler.shutdown();
		}

		LOG.debug("Shutdown of job manager completed");
		synchronized (this.isShutdownComplete) {
			this.isShutdownComplete.set(true);
			this.isShutdownComplete.notifyAll();
		}
	}

	/**
	 * Entry point for the program
	 * 
	 * @param args
	 *        arguments from the command line
	 */
	@SuppressWarnings("static-access")
	public static void main(final String[] args) {

		final Option configDirOpt = OptionBuilder.withArgName("config directory").hasArg()
			.withDescription("Specify configuration directory.").create("configDir");

		final Option executionModeOpt = OptionBuilder.withArgName("execution mode").hasArg()
			.withDescription("Specify execution mode.").create("executionMode");

		final Options options = new Options();
		options.addOption(configDirOpt);
		options.addOption(executionModeOpt);

		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
		} catch (ParseException e) {
			LOG.error("CLI Parsing failed. Reason: " + e.getMessage());
			System.exit(FAILURERETURNCODE);
		}

		final String configDir = line.getOptionValue(configDirOpt.getOpt(), null);
		final String executionMode = line.getOptionValue(executionModeOpt.getOpt(), "local");

		// Create a new job manager object
		JobManager jobManager = new JobManager(configDir, executionMode);

		// Run the main task loop
		jobManager.runTaskLoop();

		// Clean up task are triggered through a shutdown hook
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobSubmissionResult submitJob(JobGraph job) throws IOException {

		// First check if job is null
		if (job == null) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"Submitted job is null!");
			return result;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Submitted job " + job.getName() + " is not null");
		}

		// Check if any vertex of the graph has null edges
		AbstractJobVertex jv = job.findVertexWithNullEdges();
		if (jv != null) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR, "Vertex "
				+ jv.getName() + " has at least one null edge");
			return result;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Submitted job " + job.getName() + " has no null edges");
		}

		// Next, check if the graph is weakly connected
		if (!job.isWeaklyConnected()) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"Job graph is not weakly connected");
			return result;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("The graph of job " + job.getName() + " is weakly connected");
		}

		// Check if job graph has cycles
		if (!job.isAcyclic()) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"Job graph is not a DAG");
			return result;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("The graph of job " + job.getName() + " is acyclic");
		}

		// Check constrains on degree
		jv = job.areVertexDegreesCorrect();
		if (jv != null) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"Degree of vertex " + jv.getName() + " is incorrect");
			return result;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("All vertices of job " + job.getName() + " have the correct degree");
		}

		if (!job.isInstanceDependencyChainAcyclic()) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR,
				"The dependency chain for instance sharing contains a cycle");

			return result;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("The dependency chain for instance sharing is acyclic");
		}

		// Check if the job will be executed with profiling enabled
		boolean jobRunsWithProfiling = false;
		if (this.profiler != null && job.getJobConfiguration().getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)) {
			jobRunsWithProfiling = true;
		}

		// Allow plugins to rewrite the job graph
		Iterator<JobManagerPlugin> it = this.jobManagerPlugins.values().iterator();
		while (it.hasNext()) {

			final JobManagerPlugin plugin = it.next();
			if (plugin.requiresProfiling() && !jobRunsWithProfiling) {
				LOG.debug("Skipping job graph rewrite by plugin " + plugin + " because job " + job.getJobID()
					+ " will not be executed with profiling");
				continue;
			}

			final JobGraph inputJob = job;
			job = plugin.rewriteJobGraph(inputJob);
			if (job == null) {
				if (LOG.isWarnEnabled()) {
					LOG.warn("Plugin " + plugin + " set job graph to null, reverting changes...");
				}
				job = inputJob;
			}
			if (job != inputJob && LOG.isDebugEnabled()) {
				LOG.debug("Plugin " + plugin + " rewrote job graph");
			}
		}

		// Try to create initial execution graph from job graph
		LOG.info("Creating initial execution graph from job graph " + job.getName());
		ExecutionGraph eg = null;

		try {
			eg = new ExecutionGraph(job, this.instanceManager);
		} catch (GraphConversionException gce) {
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR, gce.getMessage());
			return result;
		}

		// Allow plugins to rewrite the execution graph
		it = this.jobManagerPlugins.values().iterator();
		while (it.hasNext()) {

			final JobManagerPlugin plugin = it.next();
			if (plugin.requiresProfiling() && !jobRunsWithProfiling) {
				LOG.debug("Skipping execution graph rewrite by plugin " + plugin + " because job " + job.getJobID()
					+ " will not be executed with profiling");
				continue;
			}

			final ExecutionGraph inputGraph = eg;
			eg = plugin.rewriteExecutionGraph(inputGraph);
			if (eg == null) {
				LOG.warn("Plugin " + plugin + " set execution graph to null, reverting changes...");
				eg = inputGraph;
			}
			if (eg != inputGraph) {
				LOG.debug("Plugin " + plugin + " rewrote execution graph");
			}
		}

		// Register job with the progress collector
		if (this.eventCollector != null) {
			this.eventCollector.registerJob(eg, jobRunsWithProfiling, System.currentTimeMillis());
		}

		// Check if profiling should be enabled for this job
		if (jobRunsWithProfiling) {
			this.profiler.registerProfilingJob(eg);

			if (this.eventCollector != null) {
				this.profiler.registerForProfilingData(eg.getJobID(), this.eventCollector);
			}

			// Allow plugins to register their own profiling listeners for the job
			it = this.jobManagerPlugins.values().iterator();
			while (it.hasNext()) {

				final ProfilingListener listener = it.next().getProfilingListener(eg.getJobID());
				if (listener != null) {
					this.profiler.registerForProfilingData(eg.getJobID(), listener);
				}
			}
		}

		// Register job with the dynamic input split assigner
		this.inputSplitManager.registerJob(eg);

		// Register for updates on the job status
		eg.registerJobStatusListener(this);

		// Schedule job
		if (LOG.isInfoEnabled()) {
			LOG.info("Scheduling job " + job.getName());
		}

		try {
			this.scheduler.schedulJob(eg);
		} catch (SchedulingException e) {
			unregisterJob(eg);
			JobSubmissionResult result = new JobSubmissionResult(AbstractJobResult.ReturnCode.ERROR, e.getMessage());
			return result;
		}
		
		/*
		 * At this point we need to get our configurations...
		 */
		
		numberOfInputTasks = getNumberOfTotalInputSubtasks(job);
		numberOfInnerTasks = getNumberOfTotalInnerSubtasks(job);
		numberOfOutputTasks = getNumberOfTotalOutputSubtasks(job);
		
		ManagementGraphIterator mgi = new ManagementGraphIterator(this.getManagementGraph(job.getJobID()), true);
		verticeslist = jobToManagementList(mgi);
		
		/*if(selectionFromGUIisReady) {
			setupFailureRun();
		}*/
		
		if(numberOfReRuns>=0) {
			setupFailureRun();
		}
		
		// Return on success
		return new JobSubmissionResult(AbstractJobResult.ReturnCode.SUCCESS, null);
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//Start Bachelorarbeit Vetesi
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//TODO Start
	public int getNumberOfTotalInputSubtasks(JobGraph job) {
		Iterator<AbstractJobInputVertex> inputIterator = job.getInputVertices();
		int inputSubtasks = 0; 
		while(inputIterator.hasNext()){
			inputSubtasks += inputIterator.next().getNumberOfSubtasksPerInstance();
		}
		return inputSubtasks;
	}
	
	public int getNumberOfTotalInnerSubtasks(JobGraph job) {
		Iterator<JobTaskVertex> taskIterator = job.getTaskVertices();
		int taskSubtasks = 0; 
		while(taskIterator.hasNext()){
			taskSubtasks += taskIterator.next().getNumberOfSubtasksPerInstance();
		}
		return taskSubtasks;
	}
	
	public int getNumberOfTotalOutputSubtasks(JobGraph job) {
		Iterator<AbstractJobOutputVertex> outputIterator = job.getOutputVertices();
		int outputSubtasks = 0; 
		while(outputIterator.hasNext()){
			outputSubtasks += outputIterator.next().getNumberOfSubtasksPerInstance();
		}
		return outputSubtasks;
	}
	
	public void saveConfigLocalyAndToFile(String configuration, String filename) {
		configurationHandler.saveConfig(numberOfInputTasks, numberOfInnerTasks, numberOfOutputTasks, configuration, filename);
	}
	
	public void setFilenameToSaveDataLocaly(String filename) {
		//TODO In file schreiben!
		filenameToSaveDataLocaly = filename;
	}
	
	public void setDataReport(String finalReport) {
		configurationHandler.saveDataLocaly(finalReport, filenameToSaveDataLocaly);
	}
	
	public void setNumberOfReRuns(int reRuns) {
		numberOfReRuns = reRuns;
	}
	
	public int getNumberOfReRuns() {
		return numberOfReRuns;
	}
	
	public String getFailureSeparator() {
		return configurationHandler.getFailureSeparator();
	}
	
	/*public void setFailureGeneratorIsInitiated(boolean initiated) {
		initiationFromGUIisStarted = initiated;
	}
	
	public boolean getFailureGeneratorIsInitiated() {
		return initiationFromGUIisStarted;
	}*/
	
	public void setFailureGeneratorIsReady(boolean ready) {
		selectionFromGUIisReady = ready;
	}
	
	public boolean getFailureGeneratorIsReady() {
		return selectionFromGUIisReady;
	}
	
	public int getItemChoice() {
		return failureBranch;
	}
	
	public void setConfiguration(String configuration) {
		selectedConfigFromGUI = stringToLinkedListInteger(configuration);
	}
	
	public void setDelayTimes(String delayTimes) {
		selectedDelayTimeFromGUI = stringToLinkedListInteger(delayTimes);
	}
	
	/**
	 * The FailureGenerator need to reconstruct the FailureConfiguration which is selected by the User.
	 * In order to do this properly, we use Strings, because this could be handled by kyro. 
	 * @param stringToConvert
	 * @return 
	 * @author vetesi
	 */
	private LinkedList<Integer> stringToLinkedListInteger(String stringToConvert){
		
		LinkedList<Integer> resultList = new LinkedList<Integer>();
		
		// First cutting of the braces
		if(stringToConvert.contains("[")) {
			stringToConvert = stringToConvert.replace("[", "");
		}
		if(stringToConvert.contains("]")) {
			stringToConvert = stringToConvert.replace("]", "");
		}
		
		// now splitting this String into tokens and put them as Integer in our resultList 
		String[] stringSplitting = stringToConvert.split(", ");
		for(int i = 0; i < stringSplitting.length; i++) {
			resultList.add(Integer.parseInt(stringSplitting[i]));
		}
		
		return resultList;
	}
	
	/**
	 * This function get a List of all available vertices which could be selected
	 * and asks the User, which ones should get killed. If the User try to select
	 * a Vertex which is not in the list of vertices, then we assume, that he has
	 * selected all necessary vertices for his purpose. This function is set up
	 * temporary this way, so that we can test this functionality. In a later stage
	 * of this Tool we want to create a more customer friendly version, which make
	 * it a lot easier to pick all the desired vertices.
	 * @param verticeslist
	 * 		  a List of all vertices which are possible to kill
	 * @return A LinkedList which stores all selected vertices to kill
	 * @author vetesi
	 */
	private LinkedList<ManagementVertex> getAllChoosenVertices() {
		
		LinkedList<ManagementVertex> resultlist = new LinkedList<ManagementVertex>();
		for(int i = 0; i < selectedConfigFromGUI.size(); i++) {
			 resultlist.add(verticeslist.get(selectedConfigFromGUI.get(i)));
		}
		System.out.println("You choose " + resultlist.size() + " vertices.");
		return resultlist;
	}
	
	/**
	 * This function gets a Event from the JobClient and construct the name of the vertex back
	 * @param eventName
	 * @return the Name of the vertex
	 * @author vetesi
	 */
	private String extractVertexName(String eventName) {
		String[] workInProgress = eventName.split(" \\("); 
		String firstPart = workInProgress[0];
		String secondPart = workInProgress[1].split("/")[0];
		int numberToReduce = Integer.parseInt(secondPart);
		numberToReduce--;
		secondPart = Integer.toString(numberToReduce);
		String resultString = firstPart+"_"+secondPart;
		return resultString;
	}
	
	/**
	 * In order to get a opportunity to manipulate our Job to create some failures,
	 * we create a LinkedList of all vertices of the ManagementGraph
	 * @param mgi a ManagementGraphIterator
	 * @return a List of all available Management vertices
	 * @author vetesi 
	 */
	private LinkedList<ManagementVertex> jobToManagementList(ManagementGraphIterator mgi) {
		
		LinkedList<ManagementVertex> verticeslist = new LinkedList<ManagementVertex>();
		
		while (mgi.hasNext()) {
			ManagementVertex mv = mgi.next();
			verticeslist.add(mv);
		}
		
		return verticeslist;
	}
	
	/**
	 * {@inheritDoc}
	 * @author vetesi
	 */
	/*public boolean runFailureGenerator() {
		
		//At this point, the selection wasn't completed yet
		if(!selectionFromGUIisReady) {
			
			return true;
			
		} else {
			
			setupFailureRun();
			return false;
			
		}
		
	}*/
	
	/**
	 * {@inheritDoc}
	 * @author vetesi
	 */
	public String loadConfigAndSendToVisualization(String fileName) {
		
		return configurationHandler.loadConfig(numberOfInputTasks, numberOfInnerTasks, numberOfOutputTasks, fileName);
		
	}
	
	/**
	 * {@inheritDoc}
	 * @author vetesi
	 */
	public String createItemList(int itemChoice){
		
		String allItemNames = "";
		
		if(itemChoice == TASK_FAILURE) {
			
			failureBranch = TASK_FAILURE;
			
			if(!verticeslist.isEmpty()) {
				for(int i = 0; i < verticeslist.size();i++) {
					allItemNames = allItemNames + "Vertex "+ i + " is called: " + verticeslist.get(i).toString()+ "\n";
				}
			} else {
				allItemNames = "There are no Vertices!";
			}
			
		}
		
		if(itemChoice == INSTANCE_FAILURE) {
			
			failureBranch = INSTANCE_FAILURE;
			
			allDifferentKindOfInstancesDuringJob = getAllTasksOfAllInstancesOfThisJob();
			
			for(int i = 0; i<instanceNames.size();i++) {
				allItemNames = allItemNames + instanceNames.get(i) + "\n";
			}
			
		}
		
		return allItemNames;
	}
	
	private LinkedList<LinkedList<ManagementVertex>> getAllTasksOfAllInstancesOfThisJob(){
		LinkedList<LinkedList<ManagementVertex>> resultList = new LinkedList<LinkedList<ManagementVertex>>();
		LinkedList<String> allInstanceNames = new LinkedList<String>();
		
		for(int i = 0; i < verticeslist.size(); i++)	{
			
			//If there are no names in our List jet, we can add them without a problem.
			if(allInstanceNames.isEmpty()) {
				allInstanceNames.add(verticeslist.get(i).getInstanceName());
				LinkedList<ManagementVertex> buildUp = new LinkedList<ManagementVertex>();
				buildUp.add(verticeslist.get(i));
				resultList.add(buildUp);
			} 
			else {
				
				int runs = allInstanceNames.size();
				
				for(int j = 0; j < runs; j++) {
					
					//We check if our Name of instances is already in our List. If it's not the case, then we add this name to our List
					if(!verticeslist.get(i).getInstanceName().equals(allInstanceNames.get(j))) {
						
						allInstanceNames.add(verticeslist.get(i).getInstanceName());
						
						LinkedList<ManagementVertex> buildUp = new LinkedList<ManagementVertex>();
						buildUp.add(verticeslist.get(i));
						resultList.add(buildUp);
					
					//If the Name is already in our List we could skip our search and save some time.
					} else {
						
						//If there is only one Instance, then we are sure, that this ManagementVertex belongs to this instance.
						if(allInstanceNames.size() == 1) {
							
							resultList.getFirst().add(verticeslist.get(i));
							
						} 
						//At this point we have at least one more Instance
						else {
							
							//Now we need to find the instance, where this vertex belongs to
							for(int k = 0; k < allInstanceNames.size(); k++) {
								
								if(resultList.get(k).getFirst().getInstanceName() == verticeslist.get(i).getInstanceName()) {
									
									//We have found our Instance and could add this vertex to the 
									resultList.get(k).add(verticeslist.get(i));
									k = allInstanceNames.size();
									
								}
								
							}
							
						}
						
						j = runs;
						
					}
					
				}
				
			}
			
		}
		
		if(instanceNames.isEmpty()) {
		
			for(int i = 0; i < allInstanceNames.size();i++) {
				instanceNames.add(allInstanceNames.get(i));
			}
			
		}
		
		return resultList;
	}
	
	private LinkedList<String> getAllChoosenInstances(){
		LinkedList<String> resultList = new LinkedList<String>();
		for(int i = 0; i < selectedConfigFromGUI.size(); i++) {
			resultList.add(instanceNames.get(selectedConfigFromGUI.get(i)));
		}
		return resultList;
	}
	
	/**
	 * This function reset and initiate the needed information to run a failure
	 * @author vetesi
	 */
	public void setupFailureRun(){
		
		
		if(getItemChoice() == TASK_FAILURE) {
			//After the flush of the killingList, we create a new one, because in every run we drain out our current killingList until it's empty
			killingList = getAllChoosenVertices();
		}
		
		
		if(getItemChoice() == INSTANCE_FAILURE) {
			//We create a new List of Instance Names, that should be killed
			instanceNamesToKill = getAllChoosenInstances();
			allDifferentKindOfInstancesDuringJob = getAllTasksOfAllInstancesOfThisJob();
		}
		
		//If there are some unexpected delayTimes, we remove them
		int delete = delayTime.size();
		if(delayTime.size() != 0) {
			
			for(int i = 0; i<delete;i++) {
				delayTime.removeFirst();
			}
			
		}

		//After the flush of the delayTime, we fill it up with the selected delayTimes from the GUI
		for(int i = 0; i<selectedDelayTimeFromGUI.size();i++) {
			delayTime.add(selectedDelayTimeFromGUI.get(i));
		}
		
		//At this point we need to clear this Information out, because we start a new run, and we couldn't have reached any Event yet
		delete = alreadyVisitedEventNames.size();
		if(alreadyVisitedEventNames.size() !=0) {
			
			for(int i = 0; i < delete;i++) {
				alreadyVisitedEventNames.removeFirst();
			}
			
		}
		
		//If there are some unexpected vertices which should be killed, but wasn't removed during this process, we remove them now
		delete = eventNames.size();
		if(eventNames.size() !=0) {
			
			for(int i = 0; i < delete;i++) {
				eventNames.removeFirst();
			}
		}
		
	}
		
	/**
	 * This function detects, if the current RUNNING Task is one, that was selected to kill.
	 * If this RUNNING Task matches the desired Task, than kill the Task after the Delay. 
	 * 
	 * @author vetesi
	 */
	public void tryToKillTask(String eventName) {
		
		//If we don't have any vertices to kill, then there is nothing to do
		if(!killingList.isEmpty()) {
			
			//If this event happened already, then we don't kill it any more!
			if(!alreadyVisitedEventNames.contains(eventName)) {
				
				//Is this a new Event? If we called this in a recursive way, then there wouldn't be a "(" in the eventName 
				if(eventName.contains("(")) {
					
					//Now we have something to do. At this point we need to get the real Vertex name:
					String extractedVertexName = extractVertexName(eventName);
				
					//We need to check, if this Vertex is one of the vertices, that are destined to die.  
					if(killingList.toString().contains(extractedVertexName)) {
					
						//This Event is a Vertex, that is destined to die. Is it the First in this List?
						if(killingList.getFirst().toString().equals(extractedVertexName)) {
						
							int killHappened = killingList.size();
						
							//It was the next Vertex, that is destined to die. Now we could kill it! 
							runParaKill();
						
							//In the other Thread we kill this Task. Now we need to make sure, that we remember the killing attempt
							alreadyVisitedEventNames.add(extractedVertexName);
						
							//At this Point we wait until the other Thread has requested the kill of the Vertex
							while(killHappened == killingList.size()) { }
						
							//Are there already passed Events that we could kill now because the killingList has now a other first?
							if(!eventNames.isEmpty()) {
								
								tryToKillTask(eventNames.getFirst());
								
							}
						
						}
						//This Vertex should die at some Point, but not now. 
						else {
							
							eventNames.add(extractedVertexName);
						
						}
					
					}
				
				}
				
				//If we get here, that means, that we called this function in a recursive way. This need a other approach.
				else {
					
					//Now we search, if we have already passed the Vertex, that should be killed next
					for(int i = 0; i < eventNames.size();i++) {
						
						//Is the Vertex, that we have already passed the first in this killingList?
						if(eventNames.get(i).equals(killingList.getFirst().toString())) {
							
							int killHappened = killingList.size();
							
							//We have already passed this Vertex. Now we could try to kill it, if it's not to late
							runParaKill();
							
							//In the other Thread we kill this Task. Now we need to make sure, that we remember the killing attempt
							alreadyVisitedEventNames.add(eventName);
							
							//At this Point we wait until the other Thread has requested the kill of the Vertex
							while(killHappened == killingList.size()) { }
							
							//Now we could remove this Vertex from the passed events
							eventNames.remove(i);
							
							//We don't need to search anymore
							i = eventNames.size()+1;
							
							//Now we need to look, if we can find another event, that happened already
							if(!eventNames.isEmpty()) {
								
								tryToKillTask(eventNames.getFirst());
								
							}
							
						}
						
					}
					
				}
				
			}
			
		}
		
	}
	
	public void runParaKill() {
		Thread paraKill = new Thread(new paraKillThread());
		paraKill.start();
	}
	
	public class paraKillThread implements Runnable {
		@Override
		public void run() {
			
			try {
				
					//We remove the first delayTime, so that the vertex is killed with it's selected Delay
					int getTime = delayTime.getFirst();
					delayTime.removeFirst();
					
					if(failureBranch == TASK_FAILURE) {
						//We remove the first vertex, so that the Job wouldn't try to kill it another time in the other thread 
						ManagementVertex getTaskToKill = killingList.getFirst();
						killingList.removeFirst();
						
						//Now the Task could get killed after the delay. The User is responsible to choose a delayTime in which the Task is still in state RUNNING 
						Thread.sleep((long)(getTime*1000));
						killTask(getTaskToKill.getGraph().getJobID(), getTaskToKill.getID());
					}

					if(failureBranch == INSTANCE_FAILURE) {
						
						String getInstanceTOKill = instanceNamesToKill.getFirst();
						instanceNamesToKill.removeFirst();
						StringRecord instanceToKill = new StringRecord(getInstanceTOKill);
						
						Thread.sleep((long)(getTime*1000));
						killInstance(instanceToKill);
					
					}
					
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	public void tryToKillInstance(String eventName) {
		
		//If we don't have any vertices to kill, then there is nothing to do
		if(!instanceNamesToKill.isEmpty()) {
					
			//If this event happened already, then we don't kill it any more!
			//if(!alreadyVisitedEventNames.contains(eventName)) {
						
				//Is this a new Event? If we called this in a recursive way, then there wouldn't be a "(" in the eventName 
				if(eventName.contains("(")) {
						
					//Now we have something to do. At this point we need to get the real Vertex name:
					String extractedVertexName = extractVertexName(eventName);
					
					for(int i = 0; i< allDifferentKindOfInstancesDuringJob.size(); i++) {
						
						LinkedList<ManagementVertex> searchList = allDifferentKindOfInstancesDuringJob.get(i);
						
						for(int j = 0; j< searchList.size();j++) {
							
							
							if(searchList.get(j).toString().equals(extractedVertexName)) {
								
								//If this event happened already, then we don't kill it any more!
								if(!alreadyVisitedEventNames.contains(searchList.get(j).getInstanceName())) {
								
									//We found now our Vertex! Does it run on one of our desired Instance?
									for(int k = 0; k < instanceNamesToKill.size();k++) {
										
										//We found a vertex, that belongs to a Instance that should be killed. 
										if(instanceNamesToKill.get(k).equals(searchList.get(j).getInstanceName())) {
											
											
											if(k == 0) {
												
												alreadyVisitedEventNames.add(searchList.get(j).getInstanceName());
												
												int killHappened = instanceNamesToKill.size();
												
												//It was the next Vertex, that is destined to die. Now we could kill it! 
												runParaKill();
											
												//At this Point we wait until the other Thread has requested the kill of the Vertex
												while(killHappened == instanceNamesToKill.size()) { }
												
												if(!eventNames.isEmpty()) {
													tryToKillInstance(eventNames.getFirst());
												}
											}
											
											//We couldn't kill it now. We remember the visited Instance!
											else {
												
												eventNames.add(searchList.get(j).getInstanceName());
											}
											
											return;
											
										}
										
									}
									//This vertex wasn't part of any Instance, that we want to kill. We dispatch this List!
									allDifferentKindOfInstancesDuringJob.remove(i);
								
								}
								
							}
							
						}
						
					}
					
				}
				
				// We have a recursive call!
				else {
					
					for(int i = 0; i < eventNames.size(); i++) {
						
						if(instanceNamesToKill.getFirst().equals(eventNames.get(i))) {
							
							int killHappened = instanceNamesToKill.size();
							
							//It was the next Vertex, that is destined to die. Now we could kill it! 
							runParaKill();
						
							//At this Point we wait until the other Thread has requested the kill of the Vertex
							while(killHappened == instanceNamesToKill.size()) { }
							
							eventNames.remove(i);
							
							i = eventNames.size()+1;
							
							if(!eventNames.isEmpty()) {
								tryToKillInstance(eventNames.getFirst());
							}
						}
						
					}
					
				}
				
			}
			
		//}
		
	}
	
		
	//TODO Ende der Arbeit!
	
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//Ende Bachelorarbeit Vetesi
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	/**
	 * This method is a convenience method to unregister a job from all of
	 * Nephele's monitoring, profiling and optimization components at once.
	 * 
	 * @param executionGraph
	 *        the execution graph to remove from the job manager
	 */
	private void unregisterJob(final ExecutionGraph executionGraph) {

		// Shut down the command executor service
		executionGraph.shutdownCommandExecutor();

		// Remove job from profiler (if activated)
		if (this.profiler != null
			&& executionGraph.getJobConfiguration().getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)) {
			this.profiler.unregisterProfilingJob(executionGraph);

			if (this.eventCollector != null) {
				this.profiler.unregisterFromProfilingData(executionGraph.getJobID(), this.eventCollector);
			}
		}

		// Cancel all pending requests for instances
		this.instanceManager.cancelPendingRequests(executionGraph.getJobID()); // getJobID is final member, no
																				// synchronization necessary

		// Remove job from input split manager
		if (this.inputSplitManager != null) {
			this.inputSplitManager.unregisterJob(executionGraph);
		}

		// Unregister job with library cache manager
		try {
			LibraryCacheManager.unregister(executionGraph.getJobID());
		} catch (IOException ioe) {
			if (LOG.isWarnEnabled()) {
				LOG.warn(StringUtils.stringifyException(ioe));
			}
		}

		// Finally, trigger some background tasks that may take a while to complete
		final Runnable runnable = new Runnable() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void run() {
				Thread.currentThread().setName("Remove all checkpoints");
				try {

					// Remove all the checkpoints of the job
					removeAllCheckpoints(executionGraph);

				} catch (Exception e) {
					if (LOG.isWarnEnabled()) {
						LOG.warn(StringUtils.stringifyException(e));
					}
				}
			}
		};

		// Pass background tasks to the executor service
		this.executorService.execute(runnable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void sendHeartbeat(final InstanceConnectionInfo instanceConnectionInfo,
			final HardwareDescription hardwareDescription) {

		// Delegate call to instance manager
		if (this.instanceManager != null) {

			final Runnable heartBeatRunnable = new Runnable() {

				@Override
				public void run() {
					instanceManager.reportHeartBeat(instanceConnectionInfo, hardwareDescription);
				}
			};

			this.executorService.execute(heartBeatRunnable);
		}
	}

	public void registerAdditionalVertex(ExecutionVertex vertex){
		this.eventCollector.registerAdditionalVertex(vertex);
	}
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateTaskExecutionState(final TaskExecutionState executionState) throws IOException {

		// Ignore calls with executionResult == null
		if (executionState == null) {
			LOG.error("Received call to updateTaskExecutionState with executionState == null");
			return;
		}

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(executionState.getJobID());
		if (eg == null) {
			LOG.error("Cannot find execution graph for ID " + executionState.getJobID() + " to change state to "
				+ executionState.getExecutionState());
			return;
		}

		final ExecutionVertex vertex = eg.getVertexByID(executionState.getID());
		if (vertex == null) {
			LOG.error("Cannot find vertex with ID " + executionState.getID() + " of job " + eg.getJobID()
				+ " to change state to " + executionState.getExecutionState());
			return;
		}
		
		// Asynchronously update execute state of vertex
		vertex.updateExecutionStateAsynchronously(executionState.getExecutionState(), executionState.getDescription());

//		if (executionState.getExecutionState()== ExecutionState.RUNNING && vertex.getName().contains("PDF")&& vertex.getIndexInVertexGroup() == 1 && !killed){
//			System.out.println("Killing Task " + vertex.getNameWithIndex());
//			this.killed = true;
//			killTask(executionState.getJobID(), ManagementVertexID.fromOtherID(vertex.getID()));
//		
//		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobCancelResult cancelJob(final JobID jobID) throws IOException {

		LOG.info("Trying to cancel job with ID " + jobID);

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			return new JobCancelResult(ReturnCode.ERROR, "Cannot find job with ID " + jobID);
		}

		final Runnable cancelJobRunnable = new Runnable() {

			@Override
			public void run() {
				Thread.currentThread().setName("Canceling job");
				eg.updateJobStatus(InternalJobStatus.CANCELING, "Job canceled by user");
				try {
					final TaskCancelResult cancelResult = cancelJob(eg);
					if (cancelResult != null) {
						LOG.error(cancelResult.getDescription());
					}
				} catch (InterruptedException ie) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(StringUtils.stringifyException(ie));
					}
				}
			}
		};

		eg.executeCommand(cancelJobRunnable);

		LOG.info("Cancel of job " + jobID + " successfully triggered");

		return new JobCancelResult(AbstractJobResult.ReturnCode.SUCCESS, null);
	}

	/**
	 * Cancels all the tasks in the current and upper stages of the
	 * given execution graph.
	 * 
	 * @param eg
	 *        the execution graph representing the job to cancel.
	 * @return <code>null</code> if no error occurred during the cancel attempt,
	 *         otherwise the returned object will describe the error
	 * @throws InterruptedException
	 *         thrown if the caller is interrupted while waiting for the response of a remote procedure call
	 */
	private TaskCancelResult cancelJob(final ExecutionGraph eg) throws InterruptedException {

		TaskCancelResult errorResult = null;

		/**
		 * Cancel all nodes in the current and upper execution stages.
		 */
		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, eg.getIndexOfCurrentExecutionStage(),
			false, true);

		while (it.hasNext()) {

			final ExecutionVertex vertex = it.next();
			final TaskCancelResult result = vertex.cancelTask();
			if (result.getReturnCode() != AbstractTaskResult.ReturnCode.SUCCESS) {
				errorResult = result;
			}
		}

		return errorResult;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobProgressResult getJobProgress(final JobID jobID, final long minimumSequenceNumber) throws IOException {

		if (this.eventCollector == null) {
			return new JobProgressResult(ReturnCode.ERROR, "JobManager does not support progress reports for jobs",
				null);
		}

		final ArrayList<AbstractEvent> eventList = new ArrayList<AbstractEvent>();
		this.eventCollector.getEventsForJob(jobID, eventList, false, minimumSequenceNumber);

		return new JobProgressResult(ReturnCode.SUCCESS, null, eventList);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ConnectionInfoLookupResponse lookupConnectionInfo(final InstanceConnectionInfo caller, final JobID jobID,
			final ChannelID sourceChannelID) {

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			LOG.error("Cannot find execution graph to job ID " + jobID);
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}

		final InternalJobStatus jobStatus = eg.getJobStatus();
		if (jobStatus == InternalJobStatus.FAILING || jobStatus == InternalJobStatus.CANCELING) {
			return ConnectionInfoLookupResponse.createJobIsAborting();
		}

		final ExecutionEdge edge = eg.getEdgeByID(sourceChannelID);
		if (edge == null) {
			LOG.error("Cannot find execution edge associated with ID " + sourceChannelID);
			return ConnectionInfoLookupResponse.createReceiverNotFound();
		}

		if (sourceChannelID.equals(edge.getInputChannelID())) {
			// Request was sent from an input channel

			final ExecutionVertex connectedVertex = edge.getOutputGate().getVertex();
			if(connectedVertex.getName().contains("Duplicate" )){
				System.out.println("Sending from Duplicate"  + connectedVertex.getNameWithIndex()+"to " +  edge.getInputGate().getVertex().getName()  );
			}
			if(edge.getInputGate().getVertex().getName().contains("Duplicate")){
				System.out.println("Sending to Duplicate from " + edge.getOutputGate().getVertex().getName());
			}
			final AbstractInstance assignedInstance = connectedVertex.getAllocatedResource().getInstance();
			if (assignedInstance == null) {
				LOG.error("Cannot resolve lookup: vertex found for channel ID " + edge.getOutputGateIndex()
					+ " but no instance assigned");
				LOG.info("Created receiverNotReady for " + connectedVertex + " 1");
				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}

			// Check execution state
			final ExecutionState executionState = connectedVertex.getExecutionState();
			if (executionState == ExecutionState.FINISHED) {
				return ConnectionInfoLookupResponse.createReceiverFoundAndReady();
			}

			if (executionState != ExecutionState.RUNNING && executionState != ExecutionState.REPLAYING
				&& executionState != ExecutionState.FINISHING) {
				// LOG.info("Created receiverNotReady for " + connectedVertex + " in state " + executionState + " 2");
				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}

			if (assignedInstance.getInstanceConnectionInfo().equals(caller)) {
				// Receiver runs on the same task manager
				return ConnectionInfoLookupResponse.createReceiverFoundAndReady(edge.getOutputChannelID());
			} else {
				// Receiver runs on a different task manager

				final InstanceConnectionInfo ici = assignedInstance.getInstanceConnectionInfo();
				final InetSocketAddress isa = new InetSocketAddress(ici.getAddress(), ici.getDataPort());

				return ConnectionInfoLookupResponse.createReceiverFoundAndReady(new RemoteReceiver(isa, edge
					.getConnectionID()));
			}
		}

		if (edge.isBroadcast()) {

			return multicastManager.lookupConnectionInfo(caller, jobID, sourceChannelID);

		} else {

			// Find vertex of connected input channel
			final ExecutionVertex targetVertex = edge.getInputGate().getVertex();

			// Check execution state
			final ExecutionState executionState = targetVertex.getExecutionState();

			if (executionState != ExecutionState.RUNNING && executionState != ExecutionState.REPLAYING
				&& executionState != ExecutionState.FINISHING && executionState != ExecutionState.FINISHED) {

				if (executionState == ExecutionState.ASSIGNED) {

					final Runnable command = new Runnable() {

						/**
						 * {@inheritDoc}
						 */
						@Override
						public void run() {
							Thread.currentThread().setName("Assign Vertices");
							scheduler.deployAssignedVertices(targetVertex);
						}
					};

					eg.executeCommand(command);
				}

				// LOG.info("Created receiverNotReady for " + targetVertex + " in state " + executionState + " 3");
				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}

			final AbstractInstance assignedInstance = targetVertex.getAllocatedResource().getInstance();
			if (assignedInstance == null) {
				LOG.error("Cannot resolve lookup: vertex found for channel ID " + edge.getInputChannelID()
					+ " but no instance assigned");
				// LOG.info("Created receiverNotReady for " + targetVertex + " in state " + executionState + " 4");
				return ConnectionInfoLookupResponse.createReceiverNotReady();
			}

			if (assignedInstance.getInstanceConnectionInfo().equals(caller)) {
				// Receiver runs on the same task manager
				return ConnectionInfoLookupResponse.createReceiverFoundAndReady(edge.getInputChannelID());
			} else {
				// Receiver runs on a different task manager
				final InstanceConnectionInfo ici = assignedInstance.getInstanceConnectionInfo();
				final InetSocketAddress isa = new InetSocketAddress(ici.getAddress(), ici.getDataPort());

				return ConnectionInfoLookupResponse.createReceiverFoundAndReady(new RemoteReceiver(isa, edge
					.getConnectionID()));
			}
		}

		// LOG.error("Receiver(s) not found");

		// return ConnectionInfoLookupResponse.createReceiverNotFound();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ManagementGraph getManagementGraph(final JobID jobID) throws IOException {

		final ManagementGraph mg = this.eventCollector.getManagementGraph(jobID);
		if (mg == null) {
			throw new IOException("Cannot find job with ID " + jobID);
		}

		return mg;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkTopology getNetworkTopology(final JobID jobID) throws IOException {

		if (this.instanceManager != null) {
			return this.instanceManager.getNetworkTopology(jobID);
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getRecommendedPollingInterval() throws IOException {

		return this.recommendedClientPollingInterval;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<RecentJobEvent> getRecentJobs() throws IOException {

		final List<RecentJobEvent> eventList = new ArrayList<RecentJobEvent>();

		if (this.eventCollector == null) {
			throw new IOException("No instance of the event collector found");
		}

		this.eventCollector.getRecentJobs(eventList);

		return eventList;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AbstractEvent> getEvents(final JobID jobID, final long minimumSequenceNumber) throws IOException {

		final List<AbstractEvent> eventList = new ArrayList<AbstractEvent>();

		if (this.eventCollector == null) {
			throw new IOException("No instance of the event collector found");
		}

		this.eventCollector.getEventsForJob(jobID, eventList, true, minimumSequenceNumber);

		return eventList;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void killTask(final JobID jobID, final ManagementVertexID id) throws IOException {

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			LOG.error("Cannot find execution graph for job " + jobID);
			return;
		}

		final ExecutionVertex vertex = eg.getVertexByID(ExecutionVertexID.fromManagementVertexID(id));
		if (vertex == null) {
			LOG.error("Cannot find execution vertex with ID " + id);
			return;
		}

		LOG.info("Killing task " + vertex + " of job " + jobID);

		final Runnable runnable = new Runnable() {

			@Override
			public void run() {
				Thread.currentThread().setName("Killing task " + vertex + " of job " + jobID);
				TaskKillResult result;
				try {
					result = vertex.killTask();
				} catch (InterruptedException ie) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(StringUtils.stringifyException(ie));
					}
					return;
				}
				if (result.getReturnCode() != AbstractTaskResult.ReturnCode.SUCCESS) {
					LOG.error(result.getDescription());
				}
			}
		};

		eg.executeCommand(runnable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void killInstance(final StringRecord instanceName) throws IOException {

		final AbstractInstance instance = this.instanceManager.getInstanceByName(instanceName.toString());
		if (instance == null) {
			LOG.error("Cannot find instance with name " + instanceName + " to kill it");
			return;
		}

		LOG.info("Killing task manager on instance " + instance);
		
		final Runnable runnable = new Runnable() {

			@Override
			public void run() {
				Thread.currentThread().setName("KillTaskManager");
				try {
					instance.killTaskManager();
				} catch (IOException ioe) {
					LOG.error(StringUtils.stringifyException(ioe));
				} catch (InterruptedException ie) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(StringUtils.stringifyException(ie));
					}
				}
			}
		};

		// Hand it over to the executor service
		this.executorService.execute(runnable);
	}

	/**
	 * Collects all vertices with checkpoints from the given execution graph and advises the corresponding task managers
	 * to remove those checkpoints.
	 * 
	 * @param executionGraph
	 *        the execution graph from which the checkpoints shall be removed
	 * @throws IOException
	 *         thrown if an I/O error occurs while communicating with the task managers
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while communicating with the task managers
	 */
	private static void removeAllCheckpoints(final ExecutionGraph executionGraph) throws IOException,
			InterruptedException {

		// Group vertex IDs by assigned instance
		final Map<AbstractInstance, ArrayList<ExecutionVertexID>> instanceMap = new HashMap<AbstractInstance, ArrayList<ExecutionVertexID>>();
		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(executionGraph, true);
		while (it.hasNext()) {

			final ExecutionVertex vertex = it.next();
			final AllocatedResource allocatedResource = vertex.getAllocatedResource();
			if (allocatedResource == null) {
				continue;
			}

			final AbstractInstance abstractInstance = allocatedResource.getInstance();
			if (abstractInstance == null) {
				continue;
			}

			ArrayList<ExecutionVertexID> vertexIDs = instanceMap.get(abstractInstance);
			if (vertexIDs == null) {
				vertexIDs = new ArrayList<ExecutionVertexID>();
				instanceMap.put(abstractInstance, vertexIDs);
			}
			vertexIDs.add(vertex.getID());
		}

		// Finally, trigger the removal of the checkpoints at each instance
		final Iterator<Map.Entry<AbstractInstance, ArrayList<ExecutionVertexID>>> it2 = instanceMap
			.entrySet().iterator();
		while (it2.hasNext()) {

			final Map.Entry<AbstractInstance, ArrayList<ExecutionVertexID>> entry = it2.next();
			final AbstractInstance abstractInstance = entry.getKey();
			if (abstractInstance == null) {
				LOG.error("Cannot remove checkpoint: abstractInstance is null");
				continue;
			}

			if (abstractInstance instanceof DummyInstance) {
				continue;
			}

			abstractInstance.removeCheckpoints(entry.getValue());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {

		// Delegate call to the instance manager
		if (this.instanceManager != null) {
			return this.instanceManager.getMapOfAvailableInstanceTypes();
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void jobStatusHasChanged(final ExecutionGraph executionGraph, final InternalJobStatus newJobStatus,
			final String optionalMessage) {

		LOG.info("Status of job " + executionGraph.getJobName() + "(" + executionGraph.getJobID() + ")"
			+ " changed to " + newJobStatus);

		if (newJobStatus == InternalJobStatus.FAILING) {

			// Cancel all remaining tasks
			try {
				cancelJob(executionGraph);
			} catch (InterruptedException ie) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(StringUtils.stringifyException(ie));
				}
			}
		}

		if (newJobStatus == InternalJobStatus.CANCELED || newJobStatus == InternalJobStatus.FAILED
			|| newJobStatus == InternalJobStatus.FINISHED) {
			// Unregister job for Nephele's monitoring, optimization components, and dynamic input split assignment
			unregisterJob(executionGraph);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void logBufferUtilization(final JobID jobID) throws IOException {

		final ExecutionGraph eg = this.scheduler.getExecutionGraphByID(jobID);
		if (eg == null) {
			return;
		}

		final Set<AbstractInstance> allocatedInstance = new HashSet<AbstractInstance>();

		final Iterator<ExecutionVertex> it = new ExecutionGraphIterator(eg, true);
		while (it.hasNext()) {

			final ExecutionVertex vertex = it.next();
			final ExecutionState state = vertex.getExecutionState();
			if (state == ExecutionState.RUNNING || state == ExecutionState.FINISHING) {
				final AbstractInstance instance = vertex.getAllocatedResource().getInstance();

				if (instance instanceof DummyInstance) {
					LOG.error("Found instance of type DummyInstance for vertex " + vertex.getName() + " (state "
						+ state + ")");
					continue;
				}

				allocatedInstance.add(instance);
			}
		}

		// Send requests to task managers from separate thread
		final Runnable requestRunnable = new Runnable() {

			@Override
			public void run() {

				final Iterator<AbstractInstance> it2 = allocatedInstance.iterator();

				try {
					while (it2.hasNext()) {
						it2.next().logBufferUtilization();
					}
				} catch (IOException ioe) {
					LOG.error(StringUtils.stringifyException(ioe));
				} catch (InterruptedException ie) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(StringUtils.stringifyException(ie));
					}
				}

			}
		};

		// Hand over to the executor service
		this.executorService.execute(requestRunnable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deploy(final JobID jobID, final AbstractInstance instance,
			final List<ExecutionVertex> verticesToBeDeployed) {

		if (verticesToBeDeployed.isEmpty()) {
			LOG.error("Method 'deploy' called but list of vertices to be deployed is empty");
			return;
		}

		for (final ExecutionVertex vertex : verticesToBeDeployed) {

			// Check vertex state
			if (vertex.getExecutionState() != ExecutionState.READY) {
				LOG.error("Expected vertex " + vertex + " to be in state READY but it is in state "
					+ vertex.getExecutionState());
			}

			vertex.updateExecutionState(ExecutionState.STARTING, null);
		}

		// Create a new runnable and pass it the executor service
		final Runnable deploymentRunnable = new Runnable() {

			/**
			 * {@inheritDoc}
			 */
			@Override
			public void run() {

				// Check if all required libraries are available on the instance
				try {
					instance.checkLibraryAvailability(jobID);
				} catch (IOException ioe) {
					LOG.error("Cannot check library availability: " + StringUtils.stringifyException(ioe));
				} catch (InterruptedException ie) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(StringUtils.stringifyException(ie));
					}
					return;
				}

				final List<TaskDeploymentDescriptor> submissionList = new ArrayList<TaskDeploymentDescriptor>();

				// Check the consistency of the call
				for (final ExecutionVertex vertex : verticesToBeDeployed) {

				TaskDeploymentDescriptor tdd  = vertex.constructDeploymentDescriptor();
					submissionList.add(tdd);
					if(vertex.getCheckpointState() == CheckpointState.COMPLETE || vertex.getCheckpointState() == CheckpointState.PARTIAL)
					LOG.info("Starting task " + vertex + " on " + vertex.getAllocatedResource().getInstance());
				}

				List<TaskSubmissionResult> submissionResultList = null;

				try {
					submissionResultList = instance.submitTasks(submissionList);
				} catch (IOException ioe) {
					final String errorMsg = StringUtils.stringifyException(ioe);
					for (final ExecutionVertex vertex : verticesToBeDeployed) {
						vertex.updateExecutionStateAsynchronously(ExecutionState.FAILED, errorMsg);
					}
					return;
				} catch (InterruptedException ie) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(StringUtils.stringifyException(ie));
					}
					return;
				}

				if (verticesToBeDeployed.size() != submissionResultList.size()) {
					LOG.error("size of submission result list does not match size of list with vertices to be deployed");
				}

				int count = 0;
				for (final TaskSubmissionResult tsr : submissionResultList) {

					ExecutionVertex vertex = verticesToBeDeployed.get(count++);
					if (!vertex.getID().equals(tsr.getVertexID())) {
						LOG.error("Expected different order of objects in task result list");
						vertex = null;
						for (final ExecutionVertex candVertex : verticesToBeDeployed) {
							if (tsr.getVertexID().equals(candVertex.getID())) {
								vertex = candVertex;
								break;
							}
						}

						if (vertex == null) {
							LOG.error("Cannot find execution vertex for vertex ID " + tsr.getVertexID());
							continue;
						}
					}

					if (tsr.getReturnCode() != AbstractTaskResult.ReturnCode.SUCCESS) {
						// Change the execution state to failed and let the scheduler deal with the rest
						vertex.updateExecutionStateAsynchronously(ExecutionState.FAILED, tsr.getDescription());
					}
				}
			}
		};

		this.executorService.execute(deploymentRunnable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplitWrapper requestNextInputSplit(final JobID jobID, final ExecutionVertexID vertexID,
			final int sequenceNumber) throws IOException {

		final ExecutionGraph graph = this.scheduler.getExecutionGraphByID(jobID);
		if (graph == null) {
			LOG.error("Cannot find execution graph to job ID " + jobID);
			return null;
		}

		final ExecutionVertex vertex = graph.getVertexByID(vertexID);
		if (vertex == null) {
			LOG.error("Cannot find execution vertex for vertex ID " + vertexID);
			return null;
		}

		return new InputSplitWrapper(jobID, this.inputSplitManager.getNextInputSplit(vertex, sequenceNumber));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void updateCheckpointState(final TaskCheckpointState taskCheckpointState) throws IOException {

		// Get the graph object for this
		final JobID jobID = taskCheckpointState.getJobID();
		final ExecutionGraph executionGraph = this.scheduler.getExecutionGraphByID(jobID);
		if (executionGraph == null) {
			LOG.error("Cannot find execution graph for job " + taskCheckpointState.getJobID()
				+ " to update checkpoint state");
			return;
		}

		final ExecutionVertex vertex = executionGraph.getVertexByID(taskCheckpointState.getVertexID());
		if (vertex == null) {
			LOG.error("Cannot find vertex with ID " + taskCheckpointState.getVertexID()
				+ " to update checkpoint state");
			return;
		}

		final Runnable taskStateChangeRunnable = new Runnable() {

			@Override
			public void run() {

				vertex.updateCheckpointState(taskCheckpointState.getCheckpointState());
			}
		};

		// Hand over to the executor service, as this may result in a longer operation with several IPC operations
		executionGraph.executeCommand(taskStateChangeRunnable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getPriority() {

		return 0;
	}
	
	public ExecutionGraph getExecutionGraph(JobID jobID) throws IOException {
		final ExecutionGraph executionGraph = this.scheduler.getExecutionGraphByID(jobID);
		if (executionGraph == null) {
			throw new IOException("Cannot find job with ID " + jobID);
		}
		return executionGraph;
	}
}
