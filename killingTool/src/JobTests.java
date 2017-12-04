//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;



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
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.io.library.FileLineReader;
import eu.stratosphere.nephele.io.library.FileLineWriter;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.jobmanager.ForwardTask;
import eu.stratosphere.nephele.util.JarFileCreator;
import eu.stratosphere.nephele.util.ServerTestUtils;
import eu.stratosphere.nephele.util.StringUtils;


public class JobTests{

	// if wanted - fixed seed value
	private static final String INPUT_DIRECTORY = "testDirectory";

	private static JobManagerThread JOB_MANAGER_THREAD = null;

	private static Configuration CONFIGURATION;
	
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
	
	public void startNephele() {

		if (JOB_MANAGER_THREAD == null) {

			// create the job manager
			JobManager jobManager = null;

			try {

				Constructor<JobManager> c = JobManager.class.getDeclaredConstructor(new Class[] { String.class,
					String.class });
				c.setAccessible(true);
				jobManager = c.newInstance(new Object[] { ServerTestUtils.getConfigDir(), new String("local") });

			} catch (SecurityException e) {
				//fail(e.getMessage());
			} catch (NoSuchMethodException e) {
				//fail(e.getMessage());
			} catch (IllegalArgumentException e) {
				//fail(e.getMessage());
			} catch (InstantiationException e) {
				//fail(e.getMessage());
			} catch (IllegalAccessException e) {
				//fail(e.getMessage());
			} catch (InvocationTargetException e) {
				e.printStackTrace();
				//fail(e.getMessage());
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
				//fail(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * Shuts Nephele down.
	 */
	
	public void stopNephele() {

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
			//final JobGraph jg2 = new JobGraph("Job Graph 2");
			//final JobGraph jg3 = new JobGraph("Job Graph 3");

			// input vertex
			final JobFileInputVertex i1 = new JobFileInputVertex("Input 1", jg);
			i1.setFileInputClass(FileLineReader.class);
			i1.setFilePath(new Path(inputFile.toURI()));
			
			// input vertex
			final JobFileInputVertex i2 = new JobFileInputVertex("Input 2", jg);
			i2.setFileInputClass(FileLineReader.class);
			i2.setFilePath(new Path(inputFile.toURI()));

			// input vertex
			final JobFileInputVertex i3 = new JobFileInputVertex("Input 3", jg);
			i3.setFileInputClass(FileLineReader.class);
			i3.setFilePath(new Path(inputFile.toURI()));			

			// task vertex 1
			final JobTaskVertex t1 = new JobTaskVertex("Task 1", jg);
			t1.setTaskClass(ForwardTask.class);

			// task vertex 2
			final JobTaskVertex t2 = new JobTaskVertex("Task 2", jg);
			t2.setTaskClass(ForwardTask.class);
			
			// task vertex 2
			final JobTaskVertex t3 = new JobTaskVertex("Task 3", jg);
			t3.setTaskClass(ForwardTask.class);

			// output vertex
			JobFileOutputVertex o1 = new JobFileOutputVertex("Output 1", jg);
			o1.setFileOutputClass(FileLineWriter.class);
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

	
}
