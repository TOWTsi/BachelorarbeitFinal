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

package eu.stratosphere.nephele.io.channels.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

/**
 * @author Stephan Ewen
 */
public class ShortType implements SerializationTestType
{
	private short value;
	

	public ShortType()
	{
		this.value = (short) 0;
	}
	
	private ShortType(short value)
	{
		this.value = value;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.channels.serialization.SerializationTestType#getRandom(java.util.Random)
	 */
	@Override
	public ShortType getRandom(Random rnd)
	{
		return new ShortType((short) rnd.nextInt(65536));
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeShort(this.value);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException
	{
		this.value = in.readShort();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.value;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof ShortType) {
			ShortType other = (ShortType) obj;
			return this.value == other.value;
		} else {
			return false;
		}
	}
}
