import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import eu.stratosphere.nephele.client.JobProgressResult;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.CheckpointStateChangeEvent;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.event.job.VertexAssignmentEvent;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionStateListener;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGraphIterator;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;

public class JobFailureSimulator implements ExecutionStateListener{

	// if wanted - fixed seed value
	private Random r = new Random();

	private ExtendedManagementProtocol jobManager = null;

	// tracks the current job ID. needed for polling new jobs..
	private JobID currentJob = null;
	
	private boolean debuglogging = false;

	// the number of job runs (at start) that should be ignored.
	private int jobrunstoignore = 0;

	private ManagementGraph mg;
	
	public JobFailureSimulator(ExtendedManagementProtocol jobManager) {
		this.jobManager = jobManager;

	}

	/**
	 * Executes the simulation.
	 */
	public void go() {
		try {

			for(int i = 0; i < jobrunstoignore; i++){
				// wait for first n jobs to finish...
				JobID actualid = pollNextJob();
				log("ignoring " + (i+1) + "th job...");
				waitForJobToFinish(actualid);
				log("job finished...");
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
			log("first job finished. duration: " + (duration) + " ms. -> " + duration/60000 + " minutes ");
			logToFile(String.valueOf(duration));
			jobduration = (int) (duration - 2000);

			log("set failure interval to [0 .. " + jobduration + "] ms");

			// now we handle all following jobs...
			while (true) {
				actualid = pollNextJob();
				timestamp = waitForJobToRun(actualid);

				log("job arrived. start time: " + timestamp);
				this.mg = this.jobManager.getManagementGraph(actualid);
				@SuppressWarnings("unused")
				ManagementGraphIterator mgi = new ManagementGraphIterator(mg, true);

				if(debuglogging){
					TaskDebugOutputThread t = new TaskDebugOutputThread(actualid);
					t.start();
				}
				
//				int interval = r.nextInt(jobduration/2);
//				interval=interval+(jobduration/2);
				int interval = (jobduration/2);
				log("scheduling task failure in " + interval + " ms. -> " + interval/60000 + " minutes" );
				logToFile("scheduling task failure in " + interval + " ms.");
				System.out.println("scheduling task failure in " + interval + " ms.");
				// generate a random interval and start a new task killer thread
				TaskKillerThread t = new TaskKillerThread(actualid, interval);
				t.start();

				timestamp2 = waitForJobToFinish(actualid);
				log("job finished. duration: " + (timestamp2 - timestamp));
				logToFile(String.valueOf(timestamp2 - timestamp));

			}

		} catch (Exception e) {
			e.printStackTrace();
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
			if (mv.getExecutionState() == ExecutionState.RUNNING && mv.getName().contains("PDF")) {
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
			this.filename = "/home/marrus/tmp/" + jobId.toString() + ".log"; 
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
			PrintWriter pw = new PrintWriter(new FileWriter(new File("/home/marrus/logs/measuredvalues.txt"), true));
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
