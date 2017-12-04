package eu.stratosphere.nephele.failuregenerator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Scanner;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.managementgraph.ManagementGraphIterator;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * This class build the Heart of this FailureGenerator
 * @author vetesi
 *
 */
public class FailureGenerator {
	
	public FailureGenerator() {
		
	}
	
	private ExtendedManagementProtocol jobManager = null;
	
	public FailureGenerator(ExtendedManagementProtocol jobManager) {
		this.jobManager = jobManager;

	}
	
	/**
	 * This function asks the User which Items of it's generic input list should be marked
	 * as possible ones, that should be killed
	 * @param list
	 * @return a List of all Indices which should be killed
	 * @author vetesi
	 */
	private LinkedList<Integer> createKillingList(LinkedList<?> list){
		System.out.println("Possible choices are: ");
		for(int i = 0; i< list.size();i++) {
			System.out.println("Choice " + i + " is called " + list.get(i).toString());
		}
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
		scanner.close();
		return killingList;
	}
	
	
	
	
	/**
	 * Here we get a list of all vertices which should be killed and we try to kill them all
	 * @param verticeslist
	 * 		  a List of all vertices which should be killed
	 * @author vetesi
	 */
	private void killAllSelectedVertices(LinkedList<ManagementVertex> verticeslist) {
		if(verticeslist.size() > 0) {	
			JobID jobID = verticeslist.getFirst().getGraph().getJobID();
			for(int i = 0; i < verticeslist.size(); i++) {
				try {
					//log("killing task " + verticeslist.get(i).getName() + " index in task group: " + (verticeslist.get(i).getIndexInGroup() +1));
					this.jobManager.killTask(jobID,verticeslist.get(i).getID());
					Thread.sleep(100000);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
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
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
