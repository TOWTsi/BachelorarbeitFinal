package eu.stratosphere.nephele.checkpointing;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.annotations.ForceCheckpoint;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobGenericInputVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.nephele.util.ServerTestUtils;
import eu.stratosphere.nephele.util.StringUtils;

public class RecordSkippingTest {


	/**
	 * The directory containing the Nephele configuration for this integration test.
	 */
	private static final String CONFIGURATION_DIRECTORY = "correct-conf";

	/**
	 * The number of records to generate by each producer.
	 */
	private static final int RECORDS_TO_GENERATE = 512 * 1024;


	/**
	 * Configuration key to access the number of records after which the execution failure shall occur.
	 */
	private static final String FAILED_AFTER_RECORD_KEY = "failure.after.record";

	/**
	 * The degree of parallelism for the job.
	 */
	private static final int DEGREE_OF_PARALLELISM = 3;

	/**
	 * The key to access the index of the subtask which is supposed to fail.
	 */
	private static final String FAILURE_INDEX_KEY = "failure.index";
	/**
	 * The number of records, that has been generated at output
	 */
	private static int numberOfOutputRecords = 0;
	private static long outputChecksum;
	/**
	 * The number of records, that has been generated at input
	 */
	private static int numberOfInputRecords = 0;
	
	private static long inputChecksum = 0;
	/**
	 * The thread running the job manager.
	 */
	private static JobManagerThread JOB_MANAGER_THREAD = null;

	/**
	 * The configuration for the job client;
	 */
	private static Configuration CONFIGURATION;

	/**
	 * Global flag to indicate if a task has already failed once.
	 */
	private static final Set<String> FAILED_ONCE = new HashSet<String>();

	
	
	/**
	 * This is an auxiliary class to run the job manager thread.
	 * 
	 * @author marrus
	 */
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
			CONFIGURATION.setInteger("job.execution.retries", 4);
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

	/**
	 * Sets up Nephele in local mode.
	 */
	@BeforeClass
	public static void startNephele() {

		if (JOB_MANAGER_THREAD == null) {

			// Create the job manager
			JobManager jobManager = null;

			try {

				// Try to find the correct configuration directory
				final String userDir = System.getProperty("user.dir");
				String configDir = userDir + File.separator + CONFIGURATION_DIRECTORY;
				if (!new File(configDir).exists()) {
					configDir = userDir + "/src/test/resources/" + CONFIGURATION_DIRECTORY;
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

	@ForceCheckpoint(checkpoint = true)
	public final static class InputTask extends AbstractGenericInputTask {

		private static AtomicInteger recordInput = new AtomicInteger(0);
		private static AtomicLong checksum = new AtomicLong(0);
		private RecordWriter<IntegerRecord> recordWriter;

		private volatile boolean isCanceled = false;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {
			this.recordWriter = new RecordWriter<IntegerRecord>(this);
			
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			
			final IntegerRecord record = new IntegerRecord();
			for (int j = 0; j < RECORDS_TO_GENERATE; j++) {
				record.setValue(recordInput.incrementAndGet());
				checksum.addAndGet(record.getValue());
				this.recordWriter.emit(record);

				if (this.isCanceled) {
					break;
				}
			}
			
			
			if(	recordInput.get() >= (RECORDS_TO_GENERATE * this.getCurrentNumberOfSubtasks())){
				System.out.println(" Checksum Input " + checksum + " " + recordInput.get());
				inputChecksum = checksum.get();
				checksum.set(0);
				numberOfInputRecords = 	recordInput.get();
				recordInput.set(0);

			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void cancel() {
			recordInput.set(0);
			checksum.set(0);
			this.isCanceled = true;

		}
	}

	@ForceCheckpoint(checkpoint = true)
	public final static class InnerTask extends AbstractTask {

		private MutableRecordReader<IntegerRecord> recordReader;

		private RecordWriter<IntegerRecord> recordWriter;
		
		private static int numFaults = 0;

		private volatile boolean isCanceled = false;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<IntegerRecord>(this);
			this.recordReader = new MutableRecordReader<IntegerRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			final IntegerRecord record = new IntegerRecord();

			boolean failing = false;

			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() ==( getTaskConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && numFaults < getTaskConfiguration().getInteger("failure.number", 0));
			}
			
			int count = 0;

			while (this.recordReader.next(record)) {
//				if(record.getValue() == failAfterRecord){
//					System.out.println("received record with value " + record.getValue() + " at " + count);
//				}
				this.recordWriter.emit(record);
				if (count++ == failAfterRecord && failing) {
					numFaults++;
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup() + " failed after record " + count);
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

	@ForceCheckpoint(checkpoint = false)
	public final static class NoCheckpointInnerTask extends AbstractTask {

		private MutableRecordReader<IntegerRecord> recordReader;

		private RecordWriter<IntegerRecord> recordWriter;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<IntegerRecord>(this);
			this.recordReader = new MutableRecordReader<IntegerRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			final IntegerRecord record = new IntegerRecord();

			boolean failing = false;

			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
			}

			int count = 0;

			while (this.recordReader.next(record)) {

				this.recordWriter.emit(record);
				if (count++ == failAfterRecord && failing) {
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}
			}
		}
	}
	@ForceCheckpoint(checkpoint = false)
	public final static class RefailingInnerTask extends AbstractTask {

		private MutableRecordReader<IntegerRecord> recordReader;

		private RecordWriter<IntegerRecord> recordWriter;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<IntegerRecord>(this);
			this.recordReader = new MutableRecordReader<IntegerRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			final IntegerRecord record = new IntegerRecord();

			//boolean failing = false;

			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			//failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(FAILURE_INDEX_KEY, -1));

			int i = 0;
			while (this.recordReader.next(record)) {
				i++;
				this.recordWriter.emit(record);
				if (record.getValue() == failAfterRecord) {
					System.err.println("received record with value " + record.getValue() + " at " + i);
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup() + "  RecordNumber " + record.getValue() + " positon " + i);
				}
				if(	record.getValue() > (RECORDS_TO_GENERATE*DEGREE_OF_PARALLELISM)){
					System.err.println("Received to big record " + record.getValue());
				}
			}
		}
	}
	/**
	 * This task fails at different records
	 * @author marrus
	 *
	 */
	public final static class RandomInnerTask extends AbstractTask {

		private MutableRecordReader<IntegerRecord> recordReader;

		private RecordWriter<IntegerRecord> recordWriter;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<IntegerRecord>(this);
			this.recordReader = new MutableRecordReader<IntegerRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			final IntegerRecord record = new IntegerRecord();

			int count = 0;


			
			boolean failing = false;

			int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1) +ThreadLocalRandom.current().nextInt(0, 4);
			failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(FAILURE_INDEX_KEY, -1));

			while (this.recordReader.next(record)) {

				this.recordWriter.emit(record);
				count++;
				if (count == failAfterRecord && failing) {
					failAfterRecord = failAfterRecord +ThreadLocalRandom.current().nextInt(0, 4);
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup() + "  RecordNumber " + record.getValue());
					
				}
			}
		}
	}
	public static final class OutputTask extends AbstractOutputTask {

		private MutableRecordReader<IntegerRecord> recordReader;
		int count = 0;
		private static AtomicInteger recordCount = new AtomicInteger(0);
		private static AtomicLong checksum = new AtomicLong(0);
		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordReader = new MutableRecordReader<IntegerRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			recordCount = new AtomicInteger(0);
			checksum = new AtomicLong(0);
			
			final IntegerRecord record = new IntegerRecord();
			while (this.recordReader.next(record)) {
				checksum.addAndGet(record.getValue());
				recordCount.incrementAndGet();
			}
			numberOfOutputRecords = recordCount.get();
			outputChecksum = checksum.get();
			System.out.println("Checksum Output " + checksum);
			System.out.println("RecordCount Output " + numberOfOutputRecords);
		}
		
	}

	
	/**

	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a successively failing one inner vertices.
	 */
	@Test
	public void testRepeatedlyFailingSameRecordInnerVertex() {
		
		numberOfInputRecords = 0;
		numberOfOutputRecords = 0;
		outputChecksum = 0;
		inputChecksum=0;

		final JobGraph jobGraph = new JobGraph("Job with repeatedly failing inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(1);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		// Using re-failing inner task
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);


		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(RefailingInnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex2.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 145613);
		innerVertex2.getConfiguration().setInteger(FAILURE_INDEX_KEY, 2);

		final JobTaskVertex innerVertex3 = new JobTaskVertex("Inner vertex 3", jobGraph);
		innerVertex3.setTaskClass(InnerTask.class);
		innerVertex3.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex3.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(1);
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
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (InterruptedException ie) {
			fail(StringUtils.stringifyException(ie));
		} catch (JobExecutionException e) {
			// This is a 
			fail("Job expected be running");
		} catch (Exception e) {
			fail(StringUtils.stringifyException(e));
		}
			finally {
			if (jobClient != null) {
				jobClient.close();
			}
		}
		System.out.println("output " + numberOfOutputRecords);
		System.out.println("inital    " + numberOfInputRecords);
		System.out.println("output checksum" + outputChecksum);
		System.out.println("inital  checksum  " + inputChecksum);
		System.out.println("Diff " + (inputChecksum - outputChecksum));
		
		assertTrue(numberOfOutputRecords == numberOfInputRecords -1);
		assertTrue(outputChecksum == inputChecksum - 145613);
		

		
	}
	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a successively failing one inner vertices.
	 */
	@Test
	public void testRepeatedlyFailingSameRecordInnerNoCPVertex() {
		
		numberOfInputRecords = 0;
		numberOfOutputRecords = 0;
		outputChecksum = 0;
		inputChecksum=0;

		final JobGraph jobGraph = new JobGraph("Job with repeatedly failing inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(1);
		//input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		// Using re-failing inner task
		innerVertex1.setTaskClass(RefailingInnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex1.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 145613);
		innerVertex1.getConfiguration().setInteger(FAILURE_INDEX_KEY, 2);

		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(NoCheckpointInnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex3 = new JobTaskVertex("Inner vertex 3", jobGraph);
		innerVertex3.setTaskClass(InnerTask.class);
		innerVertex3.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex3.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(1);
		//output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		innerVertex2.setVertexToShareInstancesWith(input);
		innerVertex3.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(innerVertex3, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex2.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
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
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (InterruptedException ie) {
			fail(StringUtils.stringifyException(ie));
		} catch (JobExecutionException e) {
			// This is a 
			fail("Job expected be running");
			
			return;
		} finally {
			if (jobClient != null) {
				jobClient.close();
			}
		}
		System.out.println("output " + numberOfOutputRecords);
		System.out.println("inital    " + numberOfInputRecords);
		System.out.println("output checksum" + outputChecksum);
		System.out.println("inital  checksum  " + inputChecksum);
		System.out.println("Diff " + (inputChecksum - outputChecksum));
		
		assertTrue(numberOfInputRecords == RECORDS_TO_GENERATE * input.getNumberOfSubtasks());
		assertTrue(numberOfOutputRecords == numberOfInputRecords -1);
		assertTrue(outputChecksum == inputChecksum - 145613);
	}
	
	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a successively failing one inner vertices.
	 */
	@Test
	public void testRepeatedlyFailingDifferentRecordInnerVertex() {
		numberOfInputRecords = 0;
		numberOfOutputRecords = 0;
		outputChecksum = 0;
		inputChecksum=0;

		final JobGraph jobGraph = new JobGraph("Job with repeatedly failing inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(1);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		// Using re-failing inner task
		innerVertex1.setTaskClass(RandomInnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex1.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, (RECORDS_TO_GENERATE / DEGREE_OF_PARALLELISM) /2);
		innerVertex1.getConfiguration().setInteger(FAILURE_INDEX_KEY, 2);

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
		output.setNumberOfSubtasks(1);
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

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (InterruptedException ie) {
			fail(StringUtils.stringifyException(ie));
		} catch (JobExecutionException e) { 
			assert (e.isJobCanceledByUser() == false);
			return;
		} finally {
			if (jobClient != null) {
				jobClient.close();
			}
		}

		fail("Job expected to be cancled");
	}

	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a successively failing one inner vertices.
	 */
	@Test
	public void testNoFailures() {
		numberOfInputRecords = 0;
		numberOfOutputRecords = 0;
		outputChecksum = 0;
		inputChecksum=0;
		final JobGraph jobGraph = new JobGraph("Job with repeatedly failing inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(1);
		input.setNumberOfSubtasksPerInstance(2);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		// Using re-failing inner task
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
		output.setNumberOfSubtasks(1);
		
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
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		} catch (InterruptedException ie) {
			fail(StringUtils.stringifyException(ie));
		} catch (JobExecutionException e) {
			// This is a 
			fail("Job expected be running");
		} catch (Exception e) {
			fail(StringUtils.stringifyException(e));
		}
			finally {
			if (jobClient != null) {
				jobClient.close();
			}
		}
	
		System.out.println("output " + numberOfOutputRecords);
		System.out.println("inital    " + numberOfInputRecords);
		System.out.println("output checksum" + outputChecksum);
		System.out.println("inital  checksum  " + inputChecksum);
		System.out.println("Diff " + (inputChecksum - outputChecksum));
		
		assertTrue(numberOfOutputRecords == numberOfInputRecords);
		assertTrue(numberOfOutputRecords == RECORDS_TO_GENERATE * input.getNumberOfSubtasks());
		assertTrue(outputChecksum == inputChecksum);
	}
	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a failing inner vertex.
	 */
	@Test
	public void testFailingInternalVertex() {
		numberOfInputRecords = 0;
		numberOfOutputRecords = 0;
		outputChecksum = 0;
		inputChecksum=0;

		final JobGraph jobGraph = new JobGraph("Job with failing inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(1);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(InnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);
		innerVertex2.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 95490);
		innerVertex2.getConfiguration().setInteger(FAILURE_INDEX_KEY, 1);
		innerVertex2.getConfiguration().setInteger("failure.number", 0);

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
		} catch (Exception e) {
			fail(StringUtils.stringifyException(e));
		} finally {
			if (jobClient != null) {
				jobClient.close();
			}
		}
		System.out.println("output " + numberOfOutputRecords);
		System.out.println("inital    " + numberOfInputRecords);
		System.out.println("output checksum" + outputChecksum);
		System.out.println("inital  checksum  " + inputChecksum);
		System.out.println("Diff " + (inputChecksum - outputChecksum));
		
		assertTrue(numberOfOutputRecords == numberOfInputRecords);
		assertTrue(numberOfOutputRecords == RECORDS_TO_GENERATE * input.getNumberOfSubtasks());
		assertTrue(outputChecksum == inputChecksum);

	}

}
