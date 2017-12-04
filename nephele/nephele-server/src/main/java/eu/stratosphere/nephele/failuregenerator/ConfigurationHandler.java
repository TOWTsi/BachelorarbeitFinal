package eu.stratosphere.nephele.failuregenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;


/**
 * This configuration handler is needed to crate, store and load configurations
 * for the failure Generator.
 * @author vetesi
 *
 */
public class ConfigurationHandler {
	
	private final String FAILURE_SEPARATOR = "fconfvet";
	
	public ConfigurationHandler() {
		
	}
	
	/**
	 * Creates the String which is stored to our configuration File.
	 * @param numberOfInputTasks
	 * @param numberOfInnerTasks
	 * @param numberOfOutputTasks
	 * @param configuration
	 * @return the generated String of our configuration
	 * @author vetesi
	 */
	private String configGenerator(int numberOfInputTasks, int numberOfInnerTasks, int numberOfOutputTasks, String configuration) {
		
		String configEntry = "InputTasks: "+ numberOfInputTasks + " InnerTasks: " + numberOfInnerTasks + " OutputTasks: " + numberOfOutputTasks + " TasksToKill: " + configuration;
		
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
	 * @param configuration a String of Tasks, that should be killed!
	 * @param filename
	 * @author vetesi
	 */
	public void saveConfig(int numberOfInputTasks, int numberOfInnerTasks, int numberOfOutputTasks, String configuration, String filename) {

		String newConfig = configGenerator(numberOfInputTasks, numberOfInnerTasks, numberOfOutputTasks, configuration); 
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
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
				
		} else {
			appendToFile(newConfig,filename);
		}
	}
	
	/**
	 * The Configurations are Seperated by a String, so that kyro could handle it in the visualization.
	 * Because of easier handling of this configurations, we declare this separator 
	 * @return
	 * @author vetesi
	 */
	public String getFailureSeparator() {
		return FAILURE_SEPARATOR;
	}
	
	/**
	 * This Function try to load already stored configurations for a given graph.
	 * @param numberOfInputTasks
	 * @param numberOfInnerTasks
	 * @param numberOfOutputTasks
	 * @param filename
	 * @return
	 * @author vetesi
	 */
	public String loadConfig(int numberOfInputTasks, int numberOfInnerTasks, int numberOfOutputTasks, String filename) {
		
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
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String resultToSend = "";
		for(int i = 0; i < resultList.size(); i++) {
			//System.out.println("Configuration number " + i + " is: " + allConfigs.get(i).toString());
			resultToSend = resultToSend + FAILURE_SEPARATOR + resultList.get(i).toString();
		}
		
		if(resultList.isEmpty()) {
			resultToSend = "There are no Configs to load!";
		}
		
		return resultToSend;
		
	}
	
	
	/**
	 * This function get a String of a already stored LinkedList and converts it back to a LinkedList
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
	
	public void saveDataLocaly(String data, String filename) {
		appendToFile(data,filename);
	}
	
	private static void appendToFile(String s, String filename) {
		try {
			PrintWriter pw = new PrintWriter(new FileWriter(new File(filename), true));
			
			final String DATE_FORMAT_NOW = "HH:mm:ss";
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);

			s = "(" + sdf.format(cal.getTime()) + ") " + s;
			pw.println(s);
			pw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
