import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import eu.stratosphere.nephele.checkpointing.FailingJobITCase;
import eu.stratosphere.nephele.checkpointing.FailingJobITCase.FailingJobRecord;
import eu.stratosphere.nephele.checkpointing.FailingJobITCase.InnerTask;
import eu.stratosphere.nephele.checkpointing.FailingJobITCase.InputTask;
import eu.stratosphere.nephele.checkpointing.FailingJobITCase.OutputTask;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericInputVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGraphIterator;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.rpc.RPCEnvelope;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.nephele.rpc.ServerTypeUtils;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.util.StringUtils;

public class Main {

	private static Configuration CONFIGURATION;
	private static final Set<String> FAILED_ONCE = new HashSet<String>();
	private static final int DEGREE_OF_PARALLELISM = 2;
	private static final String FAILED_AFTER_RECORD_KEY = "failure.after.record";
	private static final String FAILURE_INDEX_KEY = "failure.index";
	private static final int RECORDS_TO_GENERATE = 512 * 1024;
	//private static final JobManager jobManager;
	private final ExtendedManagementProtocol jobManager = null;
	
	/*
	 * Tests und eigene Spielereien
	 * 
	AbstractJobVertex[] testArray = jobGraph.getAllJobVertices();
	ManagementGraph test = new ManagementGraph(jobGraph.getJobID());
	List<ManagementGroupVertex> helper = test.getGroupVerticesInTopologicalOrder();
	//helper.get(0).getGroupMember(0).getID();
	//killTask(jobGraph.getJobID(), helper.get(0).getGroupMember(0).getID());
	
	System.out.println("The Number of Subtasks:"+testArray[0].getNumberOfSubtasks());
	System.out.println("The ID:"+testArray[0].getID());
	System.out.println("The JobID:"+jobGraph.getJobID());
	System.out.println("The JobID by Management:"+test.getJobID());
	System.out.println("Number of Stages:"+test.getNumberOfStages());
	System.out.println("The Number of Arrayslots:"+testArray.length);
	System.out.println("The Number of Listelements:"+helper.size());
	*/
	
	public static LinkedList<ManagementVertex> jobToManagementList(JobGraph jobGraph) {
		
		ManagementGraph mg = new ManagementGraph(jobGraph.getJobID());
		ManagementGraphIterator mgi = new ManagementGraphIterator(mg, true);
		LinkedList<ManagementVertex> verticeslist = new LinkedList<ManagementVertex>();
		
		System.out.println("Testing this Segment");
		while (mgi.hasNext()) {
			System.out.println("Test is a success");
			ManagementVertex mv = mgi.next();
			verticeslist.add(mv);
		}
		
		return verticeslist;
	}
	
	public static LinkedList<ManagementVertex> getAllChoosenVertices(LinkedList<ManagementVertex> verticeslist) {
		System.out.println("There are "+ verticeslist.size() + " total Tasks to kill. Which ones should be killed?");
		boolean chooseMore = true;
		Scanner scanner = new Scanner(System.in);
		LinkedList<Integer> result = new LinkedList<Integer>();
		while(chooseMore) { 
			System.out.println("Chose vertices to kill. You can chose with the Index of the vertices from 0 to " + (verticeslist.size()-1));
			int selectedNumber = scanner.nextInt();
			if (0 <= selectedNumber && selectedNumber < (verticeslist.size()) ) {
				System.out.println("Your selected index: " + selectedNumber);
				if (result.size() == 0) {
					result.add(selectedNumber);
					System.out.println("resultsize = " + result.size());
				} else {
					/*
					 * We save our result.size(), because if we get to the last step, in which our Vertex
					 * wasn't found and we add this Vertex to our List, then our result.size() will 
					 * increase and a bug occur, because our new listed Vertex is now part of our List
					 * and would be falsely announced that it's already in our verticeslist!
					 */
					int runs = result.size();
					for(int i = 0; i < runs; i++) {
						/*
						 * In this part of this loop, we search for already selected vertices.
						 * If its Vertex is already selected, we skip this loop and ask for another Vertex which
						 * we add to our result list. So we don't choose the same Vertex more than once.
						 */
						if (result.get(i) == selectedNumber) {
							System.out.println("result get = " + result.get(i));
							System.out.println("This Vertex is already selected!");
							i = result.size();
						}
						/*
						 * If we are in the last part of our search and didn't find that this Vertex is already selected,
						 * then we could add this Vertex to our list of vertices, which should be killed!
						 */
						if(i == runs-1 && result.get(i) != selectedNumber) {
							result.add(selectedNumber);
							System.out.println("resultsize = " + result.size());
						}
						
					}
				}
			} else {
				/*
				 * At this point we get a summary of all selected Indices
				 */
				System.out.print("All selected indices are:");
				for(int i=0;i < result.size();i++) {
					System.out.print(" " + result.get(i));
				}
				System.out.println(".");
				chooseMore = false;
			}
		}
		LinkedList<ManagementVertex> resultlist = new LinkedList<ManagementVertex>();
		for(int i = 0; i < result.size(); i++) {
			 resultlist.add(i, verticeslist.get(i));
		}
		System.out.println("You choose " + resultlist.size() + " vertices.");
		return resultlist;
	}
	
	public void killAllTasks(JobGraph jobGraph, LinkedList<ManagementVertex> verticeslist) {
		//this.jobManager = jobManager;
		for(int i = 0; i < verticeslist.size(); i++) {
			//jobManager.killTask(jobGraph.getJobID(), verticeslist.get(i).getID());
		}
		
	}
	
	
	
	public static void testJob() {

			final JobGraph jobGraph = new JobGraph("Job with failing inner vertex");

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
			innerVertex2.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 95490);
			innerVertex2.getConfiguration().setInteger(FAILURE_INDEX_KEY, 2);

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
			//AbstractJobVertex[] testArray = jobGraph.getAllJobVertices();
			
			
			
			// Create job client and launch job
			JobClient jobClient = null;
			try {
				jobClient = new JobClient(jobGraph, CONFIGURATION);
				jobClient.submitJobAndWait();
			} catch (Exception e) {
				fail(StringUtils.stringifyException(e));
			} finally {
				//LinkedList<ManagementVertex> verticeslist = jobToManagementList(jobGraph);
				//System.out.println("Size of the verticeslist: "+verticeslist.size());
				if (jobClient != null) {
					jobClient.close();
				}
			}		
	}


	public static void main(String[] args) throws Exception {
		/*
		 * Testing, if all Vertices get 	
		
		LinkedList<ManagementVertex> verticeslist = new LinkedList<ManagementVertex>();
		verticeslist.add(null);
		verticeslist.add(null);
		//System.out.println(verticeslist.getFirst().getID());
		getAllChoosenVertices(verticeslist);*/
		
//		final String JOBMANAGER_ADDRESS = "130.149.249.5";
//		final int JOBMANAGER_PORT = 6127;
		final String JOBMANAGER_ADDRESS = "127.0.0.1";
		final int JOBMANAGER_PORT = 6123;

		final InetSocketAddress inetaddr = new InetSocketAddress(JOBMANAGER_ADDRESS, JOBMANAGER_PORT);
		ExtendedManagementProtocol jobManager = null;

		try {

			RPCService rpc = new RPCService(ServerTypeUtils.getRPCTypesToRegister());;
			jobManager = (ExtendedManagementProtocol) rpc.getProxy(inetaddr, ExtendedManagementProtocol.class);
			

		} catch (IOException e) {

			e.printStackTrace();
			System.exit(1);
			return;
		}
		
		JobFailureSimulator sim = new JobFailureSimulator(jobManager);
		/*
		System.out.println("0");
		JobTests datTest = new JobTests();
		System.out.println("1");
		datTest.startNephele();
		System.out.println("2");
		datTest.testExecutionWithZeroSizeInputFile();
		System.out.println("3");
		datTest.stopNephele();
		System.out.println("4");
		*/
		//testJob();
		
		JobGenerator datJob = new JobGenerator();
		datJob.startNephele();
		//setJobGenerator(datJob);
		//this.jobGenerator = datJob;
		//datJob.test(0);
		datJob.runParaTest();
		Thread.sleep(1000);
		//runParaTest();
		sim.go();
		
		//datJob.stopNephele();
		
		//testJob();
		//sim.go();
		
		
		
		
	}
}
