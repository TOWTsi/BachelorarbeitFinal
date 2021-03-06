/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.common.io.statistics;

/**
 * Interface describing the basic statistics that can be obtained from the input.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface BaseStatistics
{
	/**
	 * A constant indicating a value is unknown.
	 */
	public static final int UNKNOWN = -1;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the total size of the input.
	 *   
	 * @return The total size of the input, in bytes.
	 */
	public long getTotalInputSize();
	
	/**
	 * Gets the number of records in the input (= base cardinality).
	 * 
	 * @return The number of records in the input.
	 */
	public long getNumberOfRecords();
	
	/**
	 * Gets the average width of a record, in bytes.
	 * 
	 * @return The average width of a record in bytes.
	 */
	public float getAverageRecordWidth();
}
