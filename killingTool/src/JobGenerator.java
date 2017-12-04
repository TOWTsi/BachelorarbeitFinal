//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.fail;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.annotations.ForceCheckpoint;
import eu.stratosphere.nephele.checkpointing.FailingJobITCase;
import eu.stratosphere.nephele.checkpointing.FailingJobITCase.FailingJobRecord;
//import eu.stratosphere.nephele.checkpointing.FailingJobITCase.InnerTask;
//import eu.stratosphere.nephele.checkpointing.FailingJobITCase.InputTask;
//import eu.stratosphere.nephele.checkpointing.FailingJobITCase.OutputTask;
import eu.stratosphere.nephele.checkpointing.FailingJobITCase.InnerTask;
import eu.stratosphere.nephele.checkpointing.FailingJobITCase.InputTask;
import eu.stratosphere.nephele.checkpointing.FailingJobITCase.OutputTask;

//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.Test;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.library.FileLineReader;
import eu.stratosphere.nephele.io.library.FileLineWriter;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericInputVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.jobmanager.ForwardTask;
import eu.stratosphere.nephele.util.JarFileCreator;
import eu.stratosphere.nephele.util.ServerTestUtils;
import eu.stratosphere.nephele.util.StringUtils;


public class JobGenerator{

	// if wanted - fixed seed value
	//private static final String INPUT_DIRECTORY = "testDirectory";

	private static JobManagerThread JOB_MANAGER_THREAD = null;

	private static Configuration CONFIGURATION;
	/**
	 * The directory containing the Nephele configuration for this integration test.
	 */
	private static final String CONFIGURATION_DIRECTORY = "correct-conf";
	
	/**
	 * The system property key to retrieve the user directory.
	 */
	private static final String USER_DIR_KEY = "user.dir";

	/**
	 * The directory containing the correct configuration file to be used during the tests.
	 */
	private static final String CORRECT_CONF_DIR = "/correct-conf";

	/**
	 * The directory the configuration directory is expected in when test are executed using Eclipse.
	 */
	private static final String ECLIPSE_PATH_EXTENSION = "/src/test/resources";
	
	private static final Set<String> FAILED_ONCE = new HashSet<String>();
	private static final String FAILED_AFTER_RECORD_KEY = "failure.after.record";
	private static final int RECORDS_TO_GENERATE = 128 * 1024;
	private static final String FAILURE_INDEX_KEY = "failure.index";
	private static final int DEGREE_OF_PARALLELISM = 4;
	
	private static final class JobManagerThread extends Thread {

		/**
		 * The job manager instance.
		 */
		private final JobManager jobManager;

		/**
		 * Constructs a new job manager thread.
		 * 
		 * @param jobManager
		 *        the job manager to run in this thread.
		 */
		private JobManagerThread(final JobManager jobManager) {

			this.jobManager = jobManager;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void run() {

			// Run task loop
			this.jobManager.runTaskLoop();
		}

		/**
		 * Shuts down the job manager.
		 */
		public void shutDown() {
			this.jobManager.shutDown();
		}
	}
	
	/*public static String getConfigDir() {

		// This is the correct path for Maven-based tests
		String configDir = System.getProperty(USER_DIR_KEY) + CORRECT_CONF_DIR;
		if (new File(configDir).exists()) {
			return configDir;
		}

		configDir = System.getProperty(USER_DIR_KEY) + ECLIPSE_PATH_EXTENSION + CORRECT_CONF_DIR;
		if (new File(configDir).exists()) {
			return configDir;
		}

		return null;
	}*/
	
	/**
	 * Sets up Nephele in local mode.
	 */
	@BeforeClass
	public static void startNephele() {

		if (JOB_MANAGER_THREAD == null) {

			// create the job manager
			JobManager jobManager = null;

			try {

				// Try to find the correct configuration directory
				final String userDir = System.getProperty("user.dir");
				String configDir = userDir + File.separator + CONFIGURATION_DIRECTORY;
				if (!new File(configDir).exists()) {
					configDir = userDir + CONFIGURATION_DIRECTORY;
				}

				final Constructor<JobManager> c = JobManager.class.getDeclaredConstructor(new Class[] { String.class,
					String.class });
				c.setAccessible(true);
				jobManager = c.newInstance(new Object[] { configDir,
					new String("local") });
				
			} catch (SecurityException e) {
				fail(e.getMessage());
			} catch (NoSuchMethodException e) {
				fail(e.getMessage());
			} catch (IllegalArgumentException e) {
				fail(e.getMessage());
			} catch (InstantiationException e) {
				fail(e.getMessage());
			} catch (IllegalAccessException e) {
				fail(e.getMessage());
			} catch (InvocationTargetException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}

			CONFIGURATION = GlobalConfiguration
				.getConfiguration(new String[] { ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY });

			// Start job manager thread
			if (jobManager != null) {
				JOB_MANAGER_THREAD = new JobManagerThread(jobManager);
				JOB_MANAGER_THREAD.start();
			}

			// Wait for the local task manager to arrive
			try {
				ServerTestUtils.waitForJobManagerToBecomeReady(jobManager);
			} catch (Exception e) {
				fail(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * Shuts Nephele down.
	 */
	@AfterClass
	public static void stopNephele() {

		if (JOB_MANAGER_THREAD != null) {
			JOB_MANAGER_THREAD.shutDown();

			try {
				JOB_MANAGER_THREAD.join();
			} catch (InterruptedException ie) {
			}
		}
	}
	
	

	/**
	 * Executes the simulation.
	 * @throws Exception 
	 */
	public void test(final int limit) {

		JobClient jobClient = null;
		File inputFile = null;
		File outputFile = null;
		File jarFile = null;

		try {

			// Get name of the forward class
			final String forwardClassName = ForwardTask.class.getSimpleName();

			// Create input and jar files
			inputFile = ServerTestUtils.createInputFile(limit);
			outputFile = new File(ServerTestUtils.getTempDir() + File.separator
				+ ServerTestUtils.getRandomFilename());
			jarFile = ServerTestUtils.createJarFile(forwardClassName);

			// Create job graph
			final JobGraph jg = new JobGraph("Job Graph 1");

			// input vertex
			final JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
			i1.setFileInputClass(FileLineReader.class);
			i1.setNumberOfSubtasks(2);
			i1.setFilePath(new Path(inputFile.toURI()));
			/*
			// input vertex
			final JobFileInputVertex i2 = new JobFileInputVertex("Input 2", jg);
			i2.setFileInputClass(FileLineReader.class);
			i2.setFilePath(new Path(inputFile.toURI()));

			// input vertex
			final JobFileInputVertex i3 = new JobFileInputVertex("Input 3", jg);
			i3.setFileInputClass(FileLineReader.class);
			i3.setFilePath(new Path(inputFile.toURI()));			
*/
			// task vertex 1
			final JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
			t1.setTaskClass(ForwardTask.class);
			t1.setNumberOfSubtasks(4);

			// task vertex 2
			final JobTaskVertex t2 = new JobTaskVertex("Task 2", jg);
			t2.setTaskClass(ForwardTask.class);
			
			// task vertex 2
			final JobTaskVertex t3 = new JobTaskVertex("Task 3", jg);
			t3.setTaskClass(ForwardTask.class);

			// output vertex
			JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
			o1.setFileOutputClass(FileLineWriter.class);
			o1.setNumberOfSubtasks(3);
			o1.setFilePath(new Path(outputFile.toURI()));

			t1.setVertexToShareInstancesWith(i1);
			t2.setVertexToShareInstancesWith(i1);
			t3.setVertexToShareInstancesWith(i1);
			o1.setVertexToShareInstancesWith(i1);

			// connect vertices
			try {
				i1.connectTo(t1, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
				//i1.connectTo(t2, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
				t1.connectTo(t2, ChannelType.FILE, CompressionLevel.NO_COMPRESSION);
				//t2.connectTo(o1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
				t2.connectTo(t3, ChannelType.FILE, CompressionLevel.NO_COMPRESSION);
				t3.connectTo(o1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			} catch (JobGraphDefinitionException e) {
				e.printStackTrace();
			}

			// add jar
			jg.addJar(new Path(new File(ServerTestUtils.getTempDir() + File.separator + forwardClassName + ".jar")
				.toURI()));

			// Create job client and launch job
			jobClient = new JobClient(jg, CONFIGURATION);
			try {
				jobClient.submitJobAndWait();
			} catch (JobExecutionException e) {
			//	fail(e.getMessage());
			}

			// Finally, compare output file to initial number sequence
			final BufferedReader bufferedReader = new BufferedReader(new FileReader(outputFile));
			for (int i = 0; i < limit; i++) {
				final String number = bufferedReader.readLine();
				//try {
				//	assertEquals(i, Integer.parseInt(number));
				//} catch (NumberFormatException e) {
				//	fail(e.getMessage());
				//}
			}

			bufferedReader.close();

		} catch (IOException ioe) {
			ioe.printStackTrace();
			//fail(ioe.getMessage());
		} catch (InterruptedException ie) {
			//fail(ie.getMessage());
		} finally {

			// Remove temporary files
			if (inputFile != null) {
				inputFile.delete();
			}

			if (outputFile != null) {
				outputFile.delete();
			}

			if (jarFile != null) {
				jarFile.delete();
			}

			if (jobClient != null) {
				jobClient.close();
			}
		}
	}
	public void testExecutionWithZeroSizeInputFile() {
		test(0);
	}

	@ForceCheckpoint(checkpoint = true)
	public final static class InputTask extends AbstractGenericInputTask {

		private RecordWriter<FailingJobRecord> recordWriter;

		private volatile boolean isCanceled = false;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<FailingJobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			
			boolean failing = false;

			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
			}

			final FailingJobRecord record = new FailingJobRecord();
			for (int i = 0; i < RECORDS_TO_GENERATE; ++i) {
				this.recordWriter.emit(record);
				Thread.sleep(1);
				if (i == failAfterRecord && failing) {
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}

				if (this.isCanceled) {
					break;
				}
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void cancel() {
			this.isCanceled = true;
		}
	}

	
	public final static class InnerTask extends AbstractTask {

		private MutableRecordReader<FailingJobRecord> recordReader;

		private RecordWriter<FailingJobRecord> recordWriter;

		private volatile boolean isCanceled = false;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<FailingJobRecord>(this);
			this.recordReader = new MutableRecordReader<FailingJobITCase.FailingJobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			
			final FailingJobRecord record = new FailingJobRecord();

			boolean failing = false;
		
			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
				
			}

			int count = 0;

			while (this.recordReader.next(record)) {
				Thread.sleep(1);
				this.recordWriter.emit(record);
				if (count++ == failAfterRecord && failing) {
					
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskNameWithIndex());
				}
				if (this.isCanceled) {
					break;
				}
			}
			if(this.getEnvironment().getTaskName().contains("Inner vertex 2")){
				System.out.println(this.getEnvironment().getTaskName() + " received " + count + " records");
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void cancel() {
			this.isCanceled = true;
		}
	}

	@ForceCheckpoint(checkpoint = true)
	public final static class NoCheckpointInnerTask extends AbstractTask {

		private MutableRecordReader<FailingJobRecord> recordReader;

		private RecordWriter<FailingJobRecord> recordWriter;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<FailingJobRecord>(this);
			this.recordReader = new MutableRecordReader<FailingJobITCase.FailingJobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			
			final FailingJobRecord record = new FailingJobRecord();

			boolean failing = false;

			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
			}

			int count = 0;

			while (this.recordReader.next(record)) {
				Thread.sleep(1);
				this.recordWriter.emit(record);
				if (count++ == failAfterRecord && failing) {
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}
			}
		}
	}

	public final static class RefailingInnerTask extends AbstractTask {

		private MutableRecordReader<FailingJobRecord> recordReader;

		private RecordWriter<FailingJobRecord> recordWriter;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<FailingJobRecord>(this);
			this.recordReader = new MutableRecordReader<FailingJobITCase.FailingJobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			
			final FailingJobRecord record = new FailingJobRecord();

			boolean failing = false;

			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(FAILURE_INDEX_KEY, -1));

			int count = 0;

			while (this.recordReader.next(record)) {
				Thread.sleep(1);
				this.recordWriter.emit(record);
				if (count++ == failAfterRecord && failing) {
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}
			}
		}
	}

	public static final class OutputTask extends AbstractOutputTask {

		private MutableRecordReader<FailingJobRecord> recordReader;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordReader = new MutableRecordReader<FailingJobITCase.FailingJobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			
			boolean failing = false;

			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
			}

			int count = 0;

			final FailingJobRecord record = new FailingJobRecord();
			while (this.recordReader.next(record)) {
				Thread.sleep(1);
				if (count++ == failAfterRecord && failing) {

					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}
			}
		}

	}
	
	/*
	@Test
	public void testRecoveryFromFileChannels() {

		final JobGraph jobGraph = new JobGraph("Job with file channels");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		output.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 153201);
		output.getConfiguration().setInteger(FAILURE_INDEX_KEY, 1);
		

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(output, ChannelType.FILE, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Reset the FAILED_ONCE flags
		synchronized (FAILED_ONCE) {
			FAILED_ONCE.clear();
		}

		// Create job client and launch job
		JobClient jobClient = null;
		try {
			jobClient = new JobClient(jobGraph, CONFIGURATION);
			//jobClient.submitJobAndWait();
			jobClient.debugJob();
		} catch (Exception e) {
			fail(StringUtils.stringifyException(e));
		} finally {
			if (jobClient != null) {
				jobClient.close();
			}
		}
	}*/
	
	public class paraTest implements Runnable {
		@Override
		public void run() {
			//Hier den Parallelen Code ausführen
			//testRecoveryFromFileChannels();
			testNoFailingInternalVertex();
			stopNephele();
		}
	}
	
	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a failing inner vertex.
	 */
	@Test
	public void testNoFailingInternalVertex() {

		final JobGraph jobGraph = new JobGraph("Job inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(InnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex3 = new JobTaskVertex("Inner vertex 3", jobGraph);
		innerVertex3.setTaskClass(InnerTask.class);
		innerVertex3.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex3.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		innerVertex2.setVertexToShareInstancesWith(input);
		innerVertex3.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(innerVertex2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex2.connectTo(innerVertex3, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex3.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Reset the FAILED_ONCE flags
		synchronized (FAILED_ONCE) {
			FAILED_ONCE.clear();
		}
				
		// Create job client and launch job
		JobClient jobClient = null;
		try {
			jobClient = new JobClient(jobGraph, CONFIGURATION);
			jobClient.submitJobAndWait();
			//jobClient.debugJob();
		} catch (Exception e) {
			fail(StringUtils.stringifyException(e));
		} finally {
			if (jobClient != null) {
				jobClient.close();
			}
		}
	}
	
	public void runParaTest() {
		Thread test = new Thread(new paraTest());
		test.start();
	}
	
}
