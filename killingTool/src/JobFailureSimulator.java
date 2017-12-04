import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.CheckpointStateChangeEvent;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.event.job.VertexAssignmentEvent;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionStateListener;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGraphIterator;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.types.StringRecord;

public class JobFailureSimulator implements ExecutionStateListener{

	// if wanted - fixed seed value
	private Random r = new Random();

	private ExtendedManagementProtocol jobManager = null;

	// tracks the current job ID. needed for polling new jobs..
	private JobID currentJob = null;
	
	private boolean debuglogging = false;

	// the number of job runs (at start) that should be ignored.
	private int jobrunstoignore = 0;

	private InstanceManager instanceManager;
	
	private ManagementGraph mg;
	
	
	public JobFailureSimulator(ExtendedManagementProtocol jobManager) {
		this.jobManager = jobManager;

	}

	/*
	 * Bachelorarbeit Vetesi
	 */
	
	/**
	 * In order to get a opportunity to manipulate our Job to create some failures,
	 * we create a LinkedList of all vertices of the ManagementGraph
	 * @param mgi a ManagementGraphIterator
	 * @return a List of all available Management vertices
	 * @author vetesi 
	 */
	public LinkedList<ManagementVertex> jobToManagementList(ManagementGraphIterator mgi) {
		
		LinkedList<ManagementVertex> verticeslist = new LinkedList<ManagementVertex>();
		
		while (mgi.hasNext()) {
			ManagementVertex mv = mgi.next();
			verticeslist.add(mv);
		}
		
		return verticeslist;
	}
	
	/**
	 * This function asks the User which Items of it's generic input list should be marked
	 * as possible ones, that should be killed
	 * @param list
	 * @return a List of all Indices which should be killed
	 * @author vetesi
	 */
	public LinkedList<Integer> createKillingList(LinkedList<?> list){
		System.out.println("Select with a Input from 0 to "+ (list.size()-1));
		LinkedList<Integer> killingList = new LinkedList<Integer>();
		boolean chooseMore = true;
		Scanner scanner = new Scanner(System.in);
		while(chooseMore) { 
			int selectedNumber = scanner.nextInt();
			if (0 <= selectedNumber && selectedNumber < (list.size()) ) {
				System.out.println("Your selected index: " + selectedNumber);
				if (killingList.size() == 0) {
					killingList.add(selectedNumber);
					System.out.println("resultsize = " + killingList.size());
				} else {
					/*
					 * We save our killingList.size(), because if we get to the last step, in which our Vertex
					 * wasn't found and we add this Vertex to our List, then our killingList.size() will 
					 * increase and a bug occur, because our new listed Vertex is now part of our List
					 * and would be announced that it's already in our killingList!
					 */
					int runs = killingList.size();
					for(int i = 0; i < runs; i++) {
						/*
						 * In this part of this loop, we search for already selected vertices.
						 * If its Vertex is already selected, we skip this loop and ask for another Vertex which
						 * we add to our result list. So we don't choose the same Vertex more than once.
						 */
						if (killingList.get(i) == selectedNumber) {
							System.out.println("result get = " + killingList.get(i));
							System.out.println("This index is already selected!");
							i = killingList.size();
						}
						/*
						 * If we are in the last part of our search and didn't find that this Vertex is already selected,
						 * then we could add this Vertex to our list of vertices, which should be killed!
						 */
						if(i == runs-1 && killingList.get(i) != selectedNumber) {
							killingList.add(selectedNumber);
							System.out.println("resultsize = " + killingList.size());
						}
					}
				}
			} else {
				/*
				 * At this point we get a summary of all selected Indices
				 */
				System.out.print("All selected indices are:");
				for(int i=0;i < killingList.size();i++) {
					System.out.print(" " + killingList.get(i));
				}
				System.out.println(".");
				chooseMore = false;
			}
		}
		return killingList;
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
	public LinkedList<ManagementVertex> getAllChoosenVertices(LinkedList<ManagementVertex> verticeslist) {
		for(int i = 0; i < verticeslist.size(); i++) {
			System.out.println("Vertex number " + i + " is called: " + verticeslist.get(i).toString() );
		}
		System.out.println("There are "+ verticeslist.size() + " total Tasks to kill. Which ones should be killed?");
		
		LinkedList<Integer> result = createKillingList(verticeslist);
		
		LinkedList<ManagementVertex> resultlist = new LinkedList<ManagementVertex>();
		for(int i = 0; i < result.size(); i++) {
			 resultlist.add(verticeslist.get(result.get(i)));
		}
		System.out.println("You choose " + resultlist.size() + " vertices.");
		return resultlist;
	}
	
	/**
	 * Here we get a list of all vertices which should be killed and we try to kill them all
	 * @param verticeslist
	 * 		  a List of all vertices which should be killed
	 * @author vetesi
	 */
	public void killAllSelectedVertices(LinkedList<ManagementVertex> verticeslist) {
		if(verticeslist.size() > 0) {	
			JobID jobID = verticeslist.getFirst().getGraph().getJobID();
			for(int i = 0; i < verticeslist.size(); i++) {
				try {
					log("killing task " + verticeslist.get(i).getName() + " index in task group: " + (verticeslist.get(i).getIndexInGroup() +1));
					this.jobManager.killTask(jobID,verticeslist.get(i).getID());
					Thread.sleep(100000);
				} catch (IOException e) {
					
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * At this point we want to get all Instances of our Job and put their Names into a List of killable Instances
	 * @param verticeslist
	 * 		  a List of all available Management vertices
	 * @return a List of Names of instances that could be killed
	 * @author vetesi
	 */
	public LinkedList<String> getAllInstances(LinkedList<ManagementVertex> verticeslist) {
		LinkedList<String> instanceNames = new LinkedList<String>(); 
		for(int i = 0; i < verticeslist.size(); i++)	{
			/*
			 * If there are no Names in our List jet, we can add them without a problem.
			 */
			if(instanceNames.isEmpty()) {
				instanceNames.add(verticeslist.get(i).getInstanceName());
			} else {
				/*
				 * We define our number of runs outside, because if we add a new Name to our List
				 * then we increase our instanceNames.size() and we have to check one more.
				 * This should save a little bit of time, because we would already know that this
				 * Name is part of the List, because we added it earlier. 
				 */
				int runs = instanceNames.size();
				for(int j = 0; j < runs; j++) {
					/*
					 * We check if our Name of instances is already in our List. If it's not the
					 * case, then we add this name to our List
					 */
					if(!verticeslist.get(i).getInstanceName().equals(instanceNames.get(j))) {
						instanceNames.add(verticeslist.get(i).getInstanceName());
					} else {
						/*
						 * If the Name is already in our List we could skip our search and save some time.
						 */
						j = runs;
					}
				}	
			}
		}
		
		return instanceNames;
	}
	
	/**
	 * In this function we select all Instances we want to kill.
	 * @param instanceNames
	 * @return a List of all instance Names, that should be killed
	 * @author vetesi
	 */
	public LinkedList<String> selectInstancesToKill(LinkedList<String> instanceNames){
		LinkedList<String> instanceNamesToKill = new LinkedList<String>();
		System.out.println("We have " + instanceNames.size() + " total instances!");
		for(int i = 0; i < instanceNames.size(); i++) {
			System.out.println("Instance number "+ i + " is called: " + instanceNames.get(i));
		}
		System.out.println("Which Instances should be killed?");
		LinkedList<Integer> result = createKillingList(instanceNames);
		for(int i = 0; i < result.size(); i++) {
			 instanceNamesToKill.add(instanceNames.get(result.get(i)));
		}
			
		return instanceNamesToKill;
	}
	
	/**
	 * 
	 * @param instanceNames a List of all Names of Instances in our Job
	 * @author vetesi 
	 */
	public void killAllSelectedInstances(LinkedList<String> instanceNames) {
		System.out.println("We have " + instanceNames.size() + " total instances!");
		for(int i = 0; i < instanceNames.size(); i++) {
			StringRecord instanceToKill = new StringRecord(instanceNames.get(i));
			try {
				this.jobManager.killInstance(instanceToKill);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	/*public void testExecution(JobGraph jobGraph, LinkedList<String> instanceNames) {
		
		try {
			ExecutionGraph exGraph = new ExecutionGraph(jobGraph, this.instanceManager);
			System.out.println(exGraph.toString());
		} catch (GraphConversionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}*/
	/*
	public void readConfigFromFile(String filename) {
		//this.filename = System.getProperty("user.dir") + "/failure-configuration/" +"test.log";
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String line;
			while((line = br.readLine()) != null){
				System.out.println(line);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/
	
	/*
	 * Kurzer Ablauf:
	 * (Wenn das File nicht existiert, dann erstelle es!) 
	 * Wenn das File existiert, dann überprüfe, ob die Konfiguration existiert
	 * Wenn die Konfiguration nicht exisitert, dann erstelle sie
	 * Ansonsten mache nichts
	 */
	/*
	public void writeConfigToFile(int numberOfInputTasks, int numberOfInnerTasks, int numberOfOutputTasks, LinkedList<Integer> list, String filename) {
		
		appendToFile("InputTasks: "+ numberOfInputTasks + " InnerTasks: " + numberOfInnerTasks + " OutputTasks: " + numberOfOutputTasks + " Tasks to Kill: " + list, filename);
		
	}*/
	
	/**
	 * Creates the String which is stored to our configuration File.
	 * @param numberOfInputTasks
	 * @param numberOfInnerTasks
	 * @param numberOfOutputTasks
	 * @param list
	 * @return the generated String of our configuration
	 * @author vetesi
	 */
	public String configGenerator(int numberOfInputTasks, int numberOfInnerTasks, int numberOfOutputTasks, LinkedList<Integer> list) {
		
		String configEntry = "InputTasks: "+ numberOfInputTasks + " InnerTasks: " + numberOfInnerTasks + " OutputTasks: " + numberOfOutputTasks + " TasksToKill: " + list;
		
		return configEntry;
	}
	
	
	/**
	 * If the configuration File doesn't exist we create it. If it already exists we check, if the new
	 * configuration is stored in our configuration File. It will be stored in our configuration file
	 * in the case, that it's not stored already. There is no point to store a configuration in case of 
	 * a empty LinkedList<Integer>. 
	 * @param numberOfInputTasks
	 * @param numberOfInnerTasks
	 * @param numberOfOutputTasks
	 * @param list a LinkedList<Integer> of Tasks, that should be killed!
	 * @param filename
	 * @author vetesi
	 */
	public void saveConfig(int numberOfInputTasks, int numberOfInnerTasks, int numberOfOutputTasks, LinkedList<Integer> list, String filename) {
		
		if(list.isEmpty()) {
			System.out.println("A empty configuration wouldn't be stored!");
		} else {
			
			String newConfig = configGenerator(numberOfInputTasks, numberOfInnerTasks, numberOfOutputTasks, list); 
			boolean existAlready = false;
			File file = new File(filename);
			
			if(file.exists()) {
			
				try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
					String line;
					while((line = br.readLine()) != null){
						
						if(line.contains(newConfig)) {
							existAlready = true;
						}
						
					}
					if(!existAlready) {
						appendToFile(newConfig,filename);
					}
					
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			} else {
				appendToFile(newConfig,filename);
			}
		}
		
	}
	
	public LinkedList<LinkedList<Integer>> loadConfig(int numberOfInputTasks, int numberOfInnerTasks, int numberOfOutputTasks, String filename) {
		
		String searchForConfig = configGenerator(numberOfInputTasks, numberOfInnerTasks, numberOfOutputTasks, null); 
		String[] checkForConfig = searchForConfig.split(" TasksToKill: ");
		
		LinkedList<LinkedList<Integer>> resultList = new LinkedList<LinkedList<Integer>>();
		
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String line;
			while((line = br.readLine()) != null){
				
				if(line.contains(checkForConfig[0])) {
					
					String workOnProgress = line.split(checkForConfig[0])[1].split(" TasksToKill: ")[1];
					resultList.add(stringToLinkedListInteger(workOnProgress)); 
				}
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return resultList;
		
	}
	
	
	/**
	 * This function get a String of a already stored LinkedList and converts it back to a LinkedList
	 * @param stringToConvert
	 * @return
	 * @author vetesi
	 */
	public LinkedList<Integer> stringToLinkedListInteger(String stringToConvert){
		
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
	
	/*
	 * Ende Bachelorarbeit Vetesi
	 */
	
	
	
	/**
	 * Executes the simulation.
	 * @throws Exception 
	 */
	public void go() throws Exception {
		/*
		String filename = System.getProperty("user.dir") + "/failure-configuration/" + "test.conf";
		if(filename.isEmpty()) {
			System.out.println("Test");
		}
		//readConfigFromFile(filename);
		
		LinkedList<Integer> testList = new LinkedList<Integer>();
		saveConfig(1,1,1,testList,filename);
		testList.addFirst(0);
		testList.addLast(1);
		testList.addLast(6);
		//writeConfigToFile(1,1,1,testList,filename);
		saveConfig(1,1,1,testList,filename);
		loadConfig(1,1,1,filename);
		loadConfig(2,1,1,filename);
*/
			for(int i = 0; i < jobrunstoignore; i++){
				// wait for first n jobs to finish...
				JobID actualid = pollNextJob();
				log("ignoring " + (i+1) + "th job...");
				long result = waitForJobToFinish(actualid);
				if(result != -1){
					log("job finished...");
				}else{
					log("job failed ");
				}
			}
			
			// the duration of the job without errors (first job)
			// minus threshold (2000 ms.. due to polling overhead etc..)
			int jobduration;
			JobID actualid;
			long timestamp ;
			long timestamp2;
//
//			log("waiting for first job to arrive...");
//
//			JobID actualid = pollNextJob();
//			long timestamp = waitForJobToRun(actualid);
//
//			if(debuglogging){
//				TaskDebugOutputThread t = new TaskDebugOutputThread(actualid);
//				t.start();
//			}
//			
//			log("first job running. waiting for it to finish...");
//			long timestamp2 = waitForJobToFinish(actualid);
			//long duration = (timestamp2 - timestamp);
			long duration = 419797;
			//log("first job finished. duration: " + (duration) + " ms. -> " + duration/60000 + " minutes ");
			//logToFile(String.valueOf(duration));
			jobduration = (int) (duration - 2000);

			//log("set failure interval to [0 .. " + jobduration + "] ms");
			
			// now we handle all following jobs...
			while (true) {
				actualid = pollNextJob();
				
				timestamp = waitForJobToRun(actualid);

				//log("job arrived. start time: " + timestamp);
				this.mg = this.jobManager.getManagementGraph(actualid);
				
				@SuppressWarnings("unused")
				ManagementGraphIterator mgi = new ManagementGraphIterator(mg, true);

				if(debuglogging){
					TaskDebugOutputThread t = new TaskDebugOutputThread(actualid);
					t.start();
				}
				
				/*
				 * Bachelorarbeit Testbereich!
				 */
				
				//System.out.println(this.jobManager.getMapOfAvailableInstanceTypes().toString());
				LinkedList<ManagementVertex> verticeslist = jobToManagementList(mgi);
				
				//ExecutionVertexID exID = fromManagementVertexID(verticeslist.getFirst().getID());
				//ExecutionGraph eg = this.jobManager.getExecutionGraphByID(actualid);
				//ExecutionVertex vertex = getVertexByID(verticeslist.getFirst().getID());
				//System.out.println("The current ExecutionState is: "+verticeslist.getFirst().getExecutionState().toString());
				//ExecutionVertexID evID = ExecutionVertexID.fromManagementVertexID(verticeslist.getFirst().getID());
				//ExecutionGraph eg= null;
				//ExecutionVertex ev = getVertexByID(evID);
				//ExecutionGraph eg1 = ev.getExecutionGraph();
				//final ExecutionGraph eg = getVertexByID(evID).getExecutionGraph();
				
				
				
				//JobManager jobManagerForExecution;
				//jobManagerForExecution.getExecutionGraph(actualid);
				//System.out.println(jobManagerForExecution.getExecutionGraph(actualid).toString());
				
				//testExecution(, instanceManager);
				/*createKillingList(verticeslist);
				createKillingList(getAllChoosenVertices(verticeslist));
				*/
				//selectInstancesToKill(getAllInstances(verticeslist));
				//killAllSelectedInstances(selectInstancesToKill(getAllInstances(verticeslist)));
				//killAllSelectedVertices(getAllChoosenVertices(verticeslist));
				//System.out.println(getAllChoosenVertices(verticeslist).toString());
				
				//281337 281873 318253 302271 318409 314337
				
				
				
				/*
				 * Ende Bachelorarbeit Testbereich!
				 */
				
				
//				int interval = r.nextInt(jobduration/2);
//				interval=interval+(jobduration/2);
				int interval = (jobduration/2);
				//log("scheduling task failure in " + interval + " ms. -> " + interval/60000 + " minutes" );
				//logToFile("scheduling task failure in " + interval + " ms.");
				// generate a random interval and start a new task killer thread
				TaskKillerThread t = new TaskKillerThread(actualid, interval);
				//t.start();

				timestamp2 = waitForJobToFinish(actualid);
				if(timestamp2 != -1){
					log("job finished. duration: " + (timestamp2 - timestamp));
					logToFile(String.valueOf(timestamp2 - timestamp));
				}else{
					log("job faiiled.");
					logToFile("job failed");
				}

			}

		
	}

	/**
	 * Waits until a now job arrived and returns the job ID.
	 * 
	 * @return
	 * @throws Exception
	 */
	private JobID pollNextJob() throws Exception {
		int retries =  5;
		while (true) {
			if(retries > 0){
			try {
				if (jobManager.getRecentJobs().size() > 0) {
					//RecentJobEvent rje = jobManager.getRecentJobs().get(jobManager.getRecentJobs().size() - 1);
					RecentJobEvent rje = jobManager.getRecentJobs().get(0);
					if (currentJob == null || !currentJob.equals(rje.getJobID())) {
						// new job here!
						currentJob = rje.getJobID();
						return currentJob;
					}
				}
				Thread.sleep(100);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Retries " + retries);
				retries--;
			}
		}
		}

	}

	/**
	 * Waits for a given job to start (RUNNING). Returns the timestamp..
	 * 
	 * @param jobid
	 * @return
	 * @throws Exception
	 */
	private long waitForJobToRun(JobID jobid) throws Exception {
		long i = 0;
		while (true) {
			JobProgressResult progress = jobManager.getJobProgress(jobid, i);
			final Iterator<AbstractEvent> it = progress.getEvents();
			while (it.hasNext()) {
				AbstractEvent actualevent = it.next();
				if(i < actualevent.getSequenceNumber()){
					i = actualevent.getSequenceNumber();
				}
				if (actualevent instanceof JobEvent) {
					if (((JobEvent) actualevent).getCurrentJobStatus() == JobStatus.RUNNING) {
						return actualevent.getTimestamp();
					}
				}
			}
			Thread.sleep(100);
		}
	}

	/**
	 * Waits for a given job to start (RUNNING). Returns the timestamp..
	 * 
	 * @param jobid
	 * @return
	 * @throws Exception
	 */
	private long waitForJobToFinish(JobID jobid) throws Exception {
		long i = 0;
		while (true) {
			JobProgressResult progress = jobManager.getJobProgress(jobid, i);
			final Iterator<AbstractEvent> it = progress.getEvents();
			while (it.hasNext()) {
				AbstractEvent actualevent = it.next();
				if(i < actualevent.getSequenceNumber()){
					i = actualevent.getSequenceNumber();
				}
				if (actualevent instanceof JobEvent) {
					if (((JobEvent) actualevent).getCurrentJobStatus() == JobStatus.FINISHED) {
						return actualevent.getTimestamp();
					}
					if (((JobEvent) actualevent).getCurrentJobStatus() == JobStatus.FAILED) {
						return -1;
					}
				}
			}
			Thread.sleep(200);
		}
	}

	/**
	 * Randomly chooses a (RUNNING) vertex and kills the task.
	 * 
	 * @param jobId
	 */
	private boolean killRandomTask(JobID jobId) throws Exception {
		
		mg = this.jobManager.getManagementGraph(jobId);
		ManagementGraphIterator mgi = new ManagementGraphIterator(mg, true);

		LinkedList<ManagementVertex> verticeslist = new LinkedList<ManagementVertex>();

		while (mgi.hasNext()) {
			ManagementVertex mv = mgi.next();
			if (mv.getExecutionState() == ExecutionState.RUNNING /*&& mv.getName().contains("PDF")*/) {
				verticeslist.add(mv);
			}else{
				System.out.println(mv.getName() + " is in state " + mv.getExecutionState());
			}
		}
		
		if (verticeslist.size() <= 0) {
			log("tried to kill a running PDF task, but found no one in state RUNNING.");
			return false;
		} else {
			
			int index = r.nextInt(verticeslist.size() - 1);
			log("killing task " + verticeslist.get(index).getName() + " index in task group: " + (verticeslist.get(index).getIndexInGroup() +1) + " running on instance: " + verticeslist.get(index).getInstanceName() );
			logToFile("killing task " + verticeslist.get(index).getName() + " index in task group: " + (verticeslist.get(index).getIndexInGroup() +1) + " running on instance: " + verticeslist.get(index).getInstanceName());
			System.out.println("killing task " + verticeslist.get(index).getName() + " index in task group: " + (verticeslist.get(index).getIndexInGroup() +1) + " running on instance: " + verticeslist.get(index).getInstanceName());
			this.jobManager.killTask(jobId, verticeslist.get(index).getID());
			return true;
		}

	}

	// This thread logs all events of a given jobid to a file
	class TaskDebugOutputThread extends Thread {

		private JobID jobId;
		int currentEventIndex = -1;
		private String filename = null;

		public TaskDebugOutputThread(JobID jobId) {
			this.jobId = jobId;
		}

		public void run() {
			// TODO Ein ordentlichen Ordner für die Files anlegen DONE
			// this.filename = System.getProperty("user.dir") + "/failure-configuration/" +"test.log";
			// this.filename = "/home/marrus/tmp/" + jobId.toString() + ".log";
			//this.filename = "/home/theo/tmp/" + jobId.toString() + ".log";
			this.filename = "/home/theo/tmp/" + "test" + ".log";
			
			System.out.println("debug event logging to file " + this.filename);
			long i = 0;
			while (true) {
				try {
					sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					List<AbstractEvent> events = jobManager.getEvents(jobId, i );
					
					while(events.size() > (currentEventIndex + 1)){
						currentEventIndex++;
						AbstractEvent actualevent = events.get(currentEventIndex);
						if(i<actualevent.getSequenceNumber()){
							i = actualevent.getSequenceNumber();
						}
						
						if(actualevent instanceof CheckpointStateChangeEvent){
							appendToFile("event is checkpoint event", filename);
							
							final CheckpointStateChangeEvent checkpointStateChangeEvent = (CheckpointStateChangeEvent) actualevent;
							final ManagementGraph graph = jobManager.getManagementGraph(jobId);
							final ManagementVertex vertex = graph.getVertexByID(checkpointStateChangeEvent.getVertexID());
							
							appendToFile(vertex.getInstanceName() + " changed checkpoint state to " + checkpointStateChangeEvent.getNewCheckpointState() , filename);
						}else if(actualevent instanceof VertexAssignmentEvent){
							final VertexAssignmentEvent vertexAssignmentEvent = (VertexAssignmentEvent) actualevent;
							final ManagementGraph graph = jobManager.getManagementGraph(jobId);
							final ManagementVertex vertex = graph.getVertexByID(vertexAssignmentEvent.getVertexID());
							if(vertex != null){						
								appendToFile("Vertex " + vertex.getName() + " (" + vertex.getIndexInGroup() + ") assigned to instance " + vertexAssignmentEvent.getInstanceName(), filename);
							}
						}else if (actualevent instanceof JobEvent){
							if(((JobEvent)actualevent).getCurrentJobStatus() == JobStatus.FINISHED || ((JobEvent)actualevent).getCurrentJobStatus()  == JobStatus.FAILED || ((JobEvent)actualevent).getCurrentJobStatus() == JobStatus.CANCELED){
								// job is failed or done..
								appendToFile("Job finished. State " + ((JobEvent)actualevent).getCurrentJobStatus().toString(), filename);
								appendToFile("exiting logging-thread", filename);
								// end this thread..
								return;
							}
						}
						
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			

		}

	}

	class TaskKillerThread extends Thread {
		private JobID jobId;

		private int interval;

		public TaskKillerThread(JobID jobId, int interval) {
			this.jobId = jobId;
			this.interval = interval;
		}

		public void run() {
			try {
				sleep(interval/2);
				
				log("scheduling task failure in " + interval/2 + " ms. -> " + (interval/2)/60000 + " minutes" );
				sleep(interval/2);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			boolean killed = false;
			while(!killed){
				try {
					sleep(2000);
				killed=	killRandomTask(jobId);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	public static void log(String s) {
		final String DATE_FORMAT_NOW = "HH:mm:ss";
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);

		System.out.println("(" + sdf.format(cal.getTime()) + ") " + s);
	}

	public static void logToFile(String s) {
		try {
			// TODO Ein ordentlichen Ordner für die Logs anlegen
			// PrintWriter pw = new PrintWriter(new FileWriter(new File("/home/marrus/logs/measuredvalues.txt"), true));
			PrintWriter pw = new PrintWriter(new FileWriter(new File("/home/theo/logs/measuredvalues.txt"), true));
			pw.println(s);
			pw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void appendToFile(String s, String filename) {
		try {
			PrintWriter pw = new PrintWriter(new FileWriter(new File(filename), true));
			
			final String DATE_FORMAT_NOW = "HH:mm:ss";
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);

			s = "(" + sdf.format(cal.getTime()) + ") " + s;
			pw.println(s);
			pw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	public int getPriority() {
		// TODO Auto-generated method stub
		return 0;
	}

@Override
	public void executionStateChanged(JobID jobID, ExecutionVertexID vertexID,
			ExecutionState newExecutionState, String optionalMessage) {
		if(this.mg.getJobID() == jobID){
		ManagementVertex vertex = this.mg.getVertexByID(vertexID.toManagementVertexID());
		System.out.println("Vertex " + vertex.getName() + " changed to " + newExecutionState);
		if(vertex.getExecutionState() != newExecutionState){
			vertex.setExecutionState(newExecutionState);
		}
		}
	}


	public void userThreadStarted(JobID jobID, ExecutionVertexID vertexID,
			Thread userThread) {
		// TODO Auto-generated method stub
		
	}


	public void userThreadFinished(JobID jobID, ExecutionVertexID vertexID,
			Thread userThread) {
		// TODO Auto-generated method stub
		
	}


	
}
