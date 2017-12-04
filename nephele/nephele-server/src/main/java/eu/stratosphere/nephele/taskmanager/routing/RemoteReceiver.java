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

package eu.stratosphere.nephele.taskmanager.routing;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Objects of this class uniquely identify a connection to a remote {@link TaskManager}.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class RemoteReceiver implements KryoSerializable {

	/**
	 * The address of the connection to the remote {@link TaskManager}.
	 */
	private InetSocketAddress connectionAddress;

	/**
	 * The index of the connection to the remote {@link TaskManager}.
	 */
	private int connectionIndex;

	/**
	 * Constructs a new remote receiver object.
	 * 
	 * @param connectionAddress
	 *        the address of the connection to the remote {@link TaskManager}
	 * @param connectionIndex
	 *        the index of the connection to the remote {@link TaskManager}
	 */
	public RemoteReceiver(final InetSocketAddress connectionAddress, final int connectionIndex) {

		if (connectionAddress == null) {
			throw new IllegalArgumentException("Argument connectionAddress must not be null");
		}

		if (connectionIndex < 0) {
			throw new IllegalArgumentException("Argument connectionIndex must be a non-negative integer number");
		}

		this.connectionAddress = connectionAddress;
		this.connectionIndex = connectionIndex;
	}

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private RemoteReceiver() {
		this.connectionAddress = null;
		this.connectionIndex = -1;
	}

	/**
	 * Returns the address of the connection to the remote {@link TaskManager}.
	 * 
	 * @return the address of the connection to the remote {@link TaskManager}
	 */
	public InetSocketAddress getConnectionAddress() {

		return this.connectionAddress;
	}

	/**
	 * Returns the index of the connection to the remote {@link TaskManager}.
	 * 
	 * @return the index of the connection to the remote {@link TaskManager}
	 */
	public int getConnectionIndex() {

		return this.connectionIndex;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return this.connectionAddress.hashCode() + (31 * this.connectionIndex);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof RemoteReceiver)) {
			return false;
		}

		final RemoteReceiver rr = (RemoteReceiver) obj;
		if (!this.connectionAddress.equals(rr.connectionAddress)) {
			return false;
		}

		if (this.connectionIndex != rr.connectionIndex) {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		return this.connectionAddress + " (" + this.connectionIndex + ")";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final Kryo kryo, final Output output) {

		output.writeInt(this.connectionIndex);
		output.writeInt(this.connectionAddress.getPort());
		final byte[] addr = this.connectionAddress.getAddress().getAddress();
		output.writeInt(addr.length);
		output.write(addr);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final Kryo kryo, final Input input) {

		this.connectionIndex = input.readInt();
		final int port = input.readInt();
		final int addrLength = input.readInt();
		final byte[] addr = new byte[addrLength];
		input.read(addr);

		InetAddress ia;
		try {
			ia = InetAddress.getByAddress(addr);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}

		this.connectionAddress = new InetSocketAddress(ia, port);
	}
}
