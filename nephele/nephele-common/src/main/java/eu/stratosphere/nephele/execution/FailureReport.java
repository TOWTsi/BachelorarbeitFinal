package eu.stratosphere.nephele.execution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;


public class FailureReport implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private ArrayList<Long> failedRecords;

	private StackTraceElement[] stacktrace;

	private String message;

	private int failureIndex;


	public FailureReport() {
		super();
	}

	public FailureReport(ArrayList<Long> failedRecords, StackTraceElement[] stackTraceElements) {
		this.failedRecords = failedRecords;
		this.stacktrace = stackTraceElements;
	}

	public ArrayList<Long> getFailedRecords() {
		return failedRecords;
	}

	public void setFailedRecords(ArrayList<Long> failedRecords) {
		if (failedRecords == null)
			throw new NullPointerException("failedRecords must not be null");

		this.failedRecords = failedRecords;
	}

	public StackTraceElement[] getStacktrace() {
		return stacktrace;
	}

	public void setStacktrace(StackTraceElement[] stacktrace) {
		if (stacktrace == null)
			throw new NullPointerException("stacktrace must not be null");

		this.stacktrace = stacktrace;
	}

	public String toString() {

		String output = "Failure Report \n";
		Iterator<Long> failedRecordsIter = failedRecords.iterator();
		int i = 0;
		while( failedRecordsIter.hasNext()){
			output += " \n Failed Record for Gate " + i + " [" ;
			output += failedRecordsIter.next()+"] ";
			i++;
		}
		if(stacktrace.length > 0){
			output += " \n Stacktrace: \n" ;
			for(int j = 0; j < stacktrace.length; j++){
				output += stacktrace[j].toString() + "\n";
			}
		}else{
			output += " \n Stacktrace is empty";
		}
		output += " \n";
		return output;

	}

	public void setMessage(String optionalMessage) {
		this.message = optionalMessage;
	}
	public String getMessage() {
		return this.message;
	}

	public int getFailureIndex() {
		return failureIndex;
	}

	public void setFailureIndex(int failureIndex) {
		this.failureIndex = failureIndex;
	}
}
