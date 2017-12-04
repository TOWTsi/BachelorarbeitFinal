package eu.stratosphere.nephele.visualization.swt;

import java.util.LinkedList;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Spinner;

public class FailureGeneratorVisualization {
	
	private static String selectedConfiguration;
	private static String allItemNames;
	private static Shell shellToThread; 
	private static String[] configToThread;
	private static int reRuns = -1;
	private static LinkedList<Integer> selectedVerticesList = new LinkedList<Integer>();
	private static LinkedList<Integer> selectedDelayTimesList = new LinkedList<Integer>();
	private static final int NUMBER_OF_MAXIMUM_RERUNS = 100;
	private static final int NUMBER_OF_MAXIMUM_SECONDS = 300;

	public FailureGeneratorVisualization() {
		
	}
	
	public static void startLoadingBranch(Shell shell, String[] config, String allNames) {
		
		allItemNames = allNames;
		shellToThread = shell;
		configToThread = config;
		
		shell.getDisplay().syncExec(new Runnable() {
			public void run(){
				listForLoadConfiguration(configToThread);
				reorderFailurePattern();
				rerunConfiguration(selectedConfiguration);
			}
		});
		
	}
	
	/**
	 * Get a Configuration and convert it to a printable Label
	 * @param stringToConvert
	 * @return
	 */
	private static String labelGenerator(String stringToConvert) {
		
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
		
		stringSplitting = allItemNames.split("\n");
		String resultLable = "";
		for(int i = 0; i < resultList.size(); i++) {
			resultLable = resultLable + stringSplitting[resultList.get(i)]+"\n";
		}
		
		return resultLable;
	}
	
	private static void listForLoadConfiguration(String[] config) {
		final Shell childShell = new Shell(shellToThread);
		
		final String[] confTest = config;
		//childShell.setLayout(new FillLayout(SWT.VERTICAL));
		
		final GridLayout gridLayout = new GridLayout(2,false); 
		childShell.setLayout(gridLayout);
		childShell.setText("Choose a Configuration to load");
		
		final GridData gridData = new GridData(GridData.FILL_HORIZONTAL);
		gridData.grabExcessVerticalSpace = true;
		
		final List single = new List(childShell, SWT.BORDER| SWT.SINGLE | SWT.V_SCROLL | SWT.H_SCROLL);
		single.setLayoutData(gridData);
		
		for(int i = 1; i<config.length;i++) {
			single.add(config[i]);
		}
		
		final Label selectionLabel = new Label(childShell,SWT.NONE);
		selectionLabel.setText(allItemNames);
		gridData.widthHint = 300;
		selectionLabel.setLayoutData(gridData);
		
		final Button loadButton = new Button(childShell,SWT.PUSH);
		loadButton.setText("Load this Configuration");
		
		loadButton.addListener(SWT.Selection, new Listener() {
			@Override
			public void handleEvent(Event arg0) { 
				
				selectedConfiguration = confTest[(single.getSelectionIndex()+1)];
				
				String stringToConvert = selectedConfiguration;
				
				if(stringToConvert.contains("[")) {
					stringToConvert = stringToConvert.replace("[", "");
				}
				if(stringToConvert.contains("]")) {
					stringToConvert = stringToConvert.replace("]", "");
				}
				
				// now splitting this String into tokens and put them as Integer in our resultList 
				String[] stringSplitting = stringToConvert.split(", ");
				for(int i = 0; i < stringSplitting.length; i++) {
					selectedVerticesList.add(Integer.parseInt(stringSplitting[i]));
				}
				
				single.dispose();	
				loadButton.dispose();
				childShell.close();

			}	
		});
		
		single.addSelectionListener(new SelectionListener() {
			
			@Override
			public void widgetDefaultSelected(SelectionEvent arg0) {
				
			}

			@Override
			public void widgetSelected(SelectionEvent arg0) {
				
				selectionLabel.setText(labelGenerator(confTest[single.getSelectionIndex()+1]));
				
			}
		});
		
		
		childShell.open();
		while(!childShell.isDisposed()){
			if(!shellToThread.getDisplay().readAndDispatch()) {
				shellToThread.getDisplay().sleep();
			}
		}
		
	}
	
	/**
	 * This function sets, how often a configuration should run.
	 * @param parentShell
	 * @param selectedConfig
	 */
	private static void rerunConfiguration(String selectedConfig) {
		
		final Shell childShell = new Shell(shellToThread);
		
		GridLayout gridLayout = new GridLayout(1, false);
		childShell.setLayout(gridLayout);
		childShell.setText("How many times do you want to run this Configuration?");
		
		GridData gridData = new GridData(GridData.FILL_HORIZONTAL);
		gridData.grabExcessVerticalSpace = true;
		
		new Label(childShell,SWT.NONE).setText("You choose: "+selectedConfig+"\n"+labelGenerator(selectedConfig));
		new Label(childShell,SWT.NONE).setText("How many times do you want to run this Configuration?");
		
		final Spinner spinner = new Spinner(childShell, SWT.BORDER);
		spinner.setMinimum(0);
		spinner.setMaximum(NUMBER_OF_MAXIMUM_RERUNS);
		spinner.setIncrement(1);
		
		final Button rerunButton = new Button(childShell,SWT.PUSH);
		rerunButton.setText("Start Job with this Configuration");
		
		rerunButton.addListener(SWT.Selection, new Listener() {
			@Override
			public void handleEvent(Event arg0) {

				reRuns = spinner.getSelection();
				
				spinner.dispose();
				rerunButton.dispose();
				childShell.close();
			}
		});
		childShell.open();
		while(!childShell.isDisposed()){
			if(!shellToThread.getDisplay().readAndDispatch()) {
				shellToThread.getDisplay().sleep();
			}
		}
	}
	
	public static int getNumberOfReRuns() {
		int runs = reRuns;
		return runs;
	}
	
	public static String getSelectedConfiguration() {
		String configuration = selectedConfiguration;
		return configuration;
	} 
	
	public static String getDelayTimes() {
		return selectedDelayTimesList.toString();
	}
	
	public static void startSavingBranch(Shell shell, String allNames) {
		allItemNames = allNames;
		shellToThread = shell;
		
		shell.getDisplay().syncExec(new Runnable() {
			public void run(){
				createANewFailure();
				reorderFailurePattern();
				rerunConfiguration(selectedConfiguration);
			}
		});
		
	}
	
	private static void createANewFailure() {
		
		final Shell childShell = new Shell(shellToThread);
		
		final LinkedList<Integer> resultList = new LinkedList<Integer>();
		String firstLabel = "All selected Items so far are: \n";
		
		final GridLayout gridLayout = new GridLayout(2,false); 
		childShell.setLayout(gridLayout);
		childShell.setText("Choose Items to kill");
		
		final GridData gridData = new GridData(GridData.FILL_HORIZONTAL);
		gridData.grabExcessVerticalSpace = true;
		
		final List multi = new List(childShell, SWT.BORDER| SWT.MULTI | SWT.V_SCROLL);
		multi.setLayoutData(gridData);
		
		String[] stringSplitting = allItemNames.split("\n");
		
		for(int i = 0; i < stringSplitting.length; i++) {
			//multi.add(Integer.toString(i));
			multi.add(stringSplitting[i]);
			firstLabel = firstLabel + "\n";
		}
		
		
		final Label selectionLabel = new Label(childShell,SWT.NONE);
		selectionLabel.setText(firstLabel);
		gridData.widthHint = 300;
		selectionLabel.setLayoutData(gridData);
		
		final Button chooseButton = new Button(childShell,SWT.PUSH);
		chooseButton.setText("Choose this Item");
		
		chooseButton.addListener(SWT.Selection, new Listener() {
			@Override
			public void handleEvent(Event arg0) {
				
				if(resultList.isEmpty()) {
					
					if(multi.getSelectionIndex() == -1) {
						final MessageBox messageBox = new MessageBox(childShell, SWT.OK |SWT.ICON_ERROR);
						messageBox.setText("Error");
						messageBox.setMessage("You have to select a Item to add it into the List!");
						messageBox.open();
					} else {
						resultList.add(multi.getSelectionIndex());
						selectionLabel.setText("All selected Items so far are: \n"+ labelGenerator(resultList.toString()) +"\n");
						selectionLabel.setLayoutData(gridData);
					}
				} else {
					
					int runs = resultList.size();
					for(int i = 0; i< runs ;i++) {
						
						if(multi.getSelectionIndex() == resultList.get(i)) {
							
							final MessageBox messageBox = new MessageBox(childShell, SWT.YES | SWT.NO |SWT.ICON_ERROR);
							messageBox.setText("Error");
							messageBox.setMessage("You can't select the same Item twice!\nDo you want to remove this Item?");
							
							if(messageBox.open() == SWT.YES) {
								resultList.remove(i);
								if(resultList.isEmpty()) {
									selectionLabel.setText("All selected Items so far are: \n");
									selectionLabel.setLayoutData(gridData);
								} else {
									selectionLabel.setText("All selected Items so far are: \n"+ labelGenerator(resultList.toString()) +"\n");
									selectionLabel.setLayoutData(gridData);
								}
							}
							
							i = runs;
							
						} else if(i == runs-1 && multi.getSelectionIndex() != resultList.get(i)) {
							
							resultList.add(multi.getSelectionIndex());
							selectionLabel.setText("All selected Items so far are: \n"+ labelGenerator(resultList.toString()) +"\n");
							selectionLabel.setLayoutData(gridData);
						}
						
					}	
					
				}
				
			}	
		});
		
		final Button endButton = new Button(childShell,SWT.PUSH);
		endButton.setText("End Selection");
		
		endButton.addListener(SWT.Selection, new Listener() {
			@Override
			public void handleEvent(Event arg0) {
				
				if(resultList.isEmpty()) {
					
					final MessageBox messageBox = new MessageBox(childShell, SWT.OK |SWT.ICON_ERROR);
					messageBox.setText("Error");
					messageBox.setMessage("You need to choose at least one Item!");
					messageBox.open();
					
				} else {
					
					selectedVerticesList = resultList;
					selectedConfiguration = resultList.toString();
					multi.dispose();	
					chooseButton.dispose();
					endButton.dispose();
					childShell.close();
					
				}

			}	
		});
		
		childShell.open();
		while(!childShell.isDisposed()){
			if(!shellToThread.getDisplay().readAndDispatch()) {
				shellToThread.getDisplay().sleep();
			}
		}
	}
	
	private static void reorderFailurePattern() {
		final Shell childShell = new Shell(shellToThread);
		
		final GridLayout gridLayout = new GridLayout(4,false); 
		childShell.setLayout(gridLayout);
		childShell.setText("Set more Configurations to this failure pattern");
		
		final GridData gridData = new GridData(GridData.FILL_HORIZONTAL);
		gridData.grabExcessVerticalSpace = true;
		
		final String[] comboItems = (labelGenerator(selectedConfiguration)).split("\n");
		final LinkedList<Combo> combos = new LinkedList<Combo>();
		final LinkedList<Spinner> spinners = new LinkedList<Spinner>();
		gridData.widthHint = 300;
		
		
		for(int i = 0; i < selectedVerticesList.size(); i++) {
			
			combos.add(new Combo(childShell, SWT.DROP_DOWN));
			combos.get(i).setItems(comboItems);
			combos.get(i).select(i);
			combos.get(i).setLayoutData(gridData);
						
			new Label(childShell, SWT.NONE).setText("Failure should occure after: ");
			
			spinners.add(new Spinner(childShell, SWT.BORDER));
			spinners.get(i).setMinimum(0);
			spinners.get(i).setMaximum(NUMBER_OF_MAXIMUM_SECONDS);
			spinners.get(i).setIncrement(1);
			
			new Label(childShell, SWT.NONE).setText(" seconds");
		}
		
		for(int i = 0; i < selectedVerticesList.size(); i++) {
			final int index = i;
			combos.get(i).addSelectionListener(new SelectionListener() {
			
				@Override
				public void widgetDefaultSelected(SelectionEvent arg0) {
					
				}

				@Override
				public void widgetSelected(SelectionEvent arg0) {
					
					
					int swapInList = selectedVerticesList.get(index);
					int swapInList2 = selectedVerticesList.get(combos.get(index).getSelectionIndex());
					
					selectedVerticesList.remove(index);
					selectedVerticesList.add(index, swapInList2);
					selectedVerticesList.remove(combos.get(index).getSelectionIndex());
					selectedVerticesList.add(combos.get(index).getSelectionIndex(), swapInList);
					
					String swap = comboItems[index];
					comboItems[index] = comboItems[combos.get(index).getSelectionIndex()];
					comboItems[combos.get(index).getSelectionIndex()] = swap;
					for(int i = 0; i < selectedVerticesList.size(); i++) {
						combos.get(i).setItems(comboItems);
						combos.get(i).select(i);
					}
					
					
				}
			
			});
		}
		
		final Button runButton = new Button(childShell,SWT.PUSH);
		runButton.setText("Apply this Configuration");
		
		runButton.addListener(SWT.Selection, new Listener() {
			@Override
			public void handleEvent(Event arg0) {
				
				for(int i = 0; i < selectedVerticesList.size(); i++) {
					selectedDelayTimesList.add(spinners.get(i).getSelection());
					spinners.get(i).dispose();
					combos.get(i).dispose();
				}
				
				selectedConfiguration = selectedVerticesList.toString();
				runButton.dispose();
				childShell.close();
			}
		});
		
		
		childShell.open();
		while(!childShell.isDisposed()){
			if(!shellToThread.getDisplay().readAndDispatch()) {
				shellToThread.getDisplay().sleep();
			}
		}
	}
	
	
	
	public static void startInstanceBranch(Shell shell, String allNames) {
		allItemNames = allNames;
		shellToThread = shell;
		
		shell.getDisplay().syncExec(new Runnable() {
			public void run(){
				createANewFailure();
				reorderFailurePattern();
				rerunConfiguration(selectedConfiguration);
			}
		});
		
	}
	
}