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

package eu.stratosphere.nephele.rpc;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.minlog.Log;

import eu.stratosphere.nephele.util.NumberUtils;

/**
 * The network thread is responsible for (reliably) transmitting a sequence of datagram packets to a receiver.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
final class NetworkThread extends Thread {

	/**
	 * The maximum number of retransmissions before a sequence of datagram packets is considered to be lost.
	 */
	private static final int MAXIMUM_NUMBER_OF_RETRANSMISSIONS = 20;

	/**
	 * The timeout in milliseconds before a retransmission is triggered.
	 */
	private static final int RETRANSMISSION_TIMEOUT = 100;

	/**
	 * Auxiliary class to store the last acknowledged packet of an outstanding transmission.
	 * <p>
	 * This class is not thread-safe.
	 * 
	 * @author warneke
	 */
	private static final class OutstandingTransmission {

		/**
		 * The last acknowledged packet of an outstanding transmission.
		 */
		private int lastAckedPacket = -1;
	}

	/**
	 * Reference to the RPC service.
	 */
	private final RPCService rpcService;

	/**
	 * The datagram socket to send and receive data.
	 */
	private final DatagramSocket socket;

	/**
	 * A map of all outstanding transmissions, i.e. transmissions that still require an acknowledgment.
	 */
	private final ConcurrentHashMap<Integer, OutstandingTransmission> outstandingTransmissions = new ConcurrentHashMap<Integer, OutstandingTransmission>();

	/**
	 * A map of all incompletely received sequences of datagram packets.
	 */
	private final ConcurrentHashMap<Integer, MultiPacketInputStream> incompleteInputStreams = new ConcurrentHashMap<Integer, MultiPacketInputStream>();

	/**
	 * Stores whether the thread has been requested to stop and shut down.
	 */
	private volatile boolean shutdownRequested = false;

	/**
	 * Initializes a new network thread.
	 * 
	 * @param rpcService
	 *        reference to the RPC service
	 * @param rpcPort
	 *        the port to bind the datagram socket to, <code>-1</code> for an arbitrary port
	 * @throws IOException
	 *         thrown if the datagram socket cannot be created
	 */
	NetworkThread(final RPCService rpcService, final int rpcPort) throws IOException {
		super("RPC Network Thread");

		this.rpcService = rpcService;
		if (rpcPort == -1) {
			this.socket = new DatagramSocket();
		} else {
			this.socket = new DatagramSocket(rpcPort);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		byte[] dataBuf = new byte[RPCMessage.MAXIMUM_MSG_SIZE + RPCMessage.METADATA_SIZE];
		DatagramPacket dataDP = new DatagramPacket(dataBuf, dataBuf.length);

		final byte[] ackBuf = new byte[6];
		final DatagramPacket ackDP = new DatagramPacket(ackBuf, ackBuf.length);

		while (!this.shutdownRequested) {

			try {
				this.socket.receive(dataDP);
			} catch (SocketException se) {
				if (this.shutdownRequested) {
					return;
				}
				Log.error("Shutting down receiver thread due to error: ", se);
				return;
			} catch (IOException ioe) {
				Log.error("Shutting down receiver thread due to error: ", ioe);
				return;
			}

			final byte[] dbbuf = dataDP.getData();
			int length = dataDP.getLength();

			// Check if packet is an ACK
			if (length < RPCMessage.METADATA_SIZE) {
				// Process ACK
				final int messageID = NumberUtils.byteArrayToInteger(dbbuf, 0);
				final int ackedPacket = NumberUtils.byteArrayToShort(dbbuf, 4);

				final Integer msgID = Integer.valueOf(messageID);
				final OutstandingTransmission outstandingTransmission = this.outstandingTransmissions.get(msgID);
				if (outstandingTransmission != null) {
					synchronized (outstandingTransmission) {
						if (outstandingTransmission.lastAckedPacket < ackedPacket) {
							outstandingTransmission.lastAckedPacket = ackedPacket;
						}
						outstandingTransmission.notify();
					}
				}

				continue;
			}

			// Adjust length
			length = length - RPCMessage.METADATA_SIZE;
			final short numberOfPackets = NumberUtils.byteArrayToShort(dbbuf, length + 2);
			final int messageID = NumberUtils.byteArrayToInteger(dbbuf, length + 4);

			if (numberOfPackets == 1) {

				// Generate and send acknowledgment
				final InetSocketAddress remoteSocketAddress = (InetSocketAddress) dataDP.getSocketAddress();
				NumberUtils.integerToByteArray(messageID, ackBuf, 0);
				NumberUtils.shortToByteArray((short) 0, ackBuf, 4);
				ackDP.setSocketAddress(remoteSocketAddress);
				try {
					this.socket.send(ackDP);
				} catch (IOException ioe) {
					if (this.shutdownRequested) {
						return;
					}
					Log.error("Shutting down receiver thread due to error: ", ioe);
					return;
				}

				// Allocate new memory for next packet
				dataBuf = new byte[RPCMessage.MAXIMUM_MSG_SIZE + RPCMessage.METADATA_SIZE];
				dataDP = new DatagramPacket(dataBuf, dataBuf.length);

				// Handle single packet request
				this.rpcService.processIncomingRPCMessage(remoteSocketAddress, new Input(new SinglePacketInputStream(
					dbbuf, length)));

			} else {

				// Get data structure to store the sequence of packets
				final Integer msgID = Integer.valueOf(messageID);
				MultiPacketInputStream mpis = this.incompleteInputStreams.get(msgID);
				if (mpis == null) {
					mpis = new MultiPacketInputStream(numberOfPackets);
					final MultiPacketInputStream oldVal = this.incompleteInputStreams.putIfAbsent(msgID, mpis);
					if (oldVal != null) {
						mpis = oldVal;
					}
				}

				final short packetIndex = NumberUtils.byteArrayToShort(dbbuf, length);
				final short expectedIndex = mpis.addPacket(packetIndex, dataDP);
				if (packetIndex != expectedIndex) {
					// Generate acknowledgment for last received packet (works like NACK)
					final InetSocketAddress remoteSocketAddress = (InetSocketAddress) dataDP.getSocketAddress();
					NumberUtils.integerToByteArray(messageID, ackBuf, 0);
					NumberUtils.shortToByteArray((short) (expectedIndex - 1), ackBuf, 4);
					ackDP.setSocketAddress(remoteSocketAddress);
					try {
						this.socket.send(ackDP);
					} catch (IOException ioe) {
						if (this.shutdownRequested) {
							return;
						}
						Log.error("Shutting down receiver thread due to error: ", ioe);
						return;
					}
					continue;
				}

				// Acknowledge every 10th packet and last packet of the sequence
				if (((packetIndex - 1) % 10 == 0) || (packetIndex == (numberOfPackets - 1))) {
					final InetSocketAddress remoteSocketAddress = (InetSocketAddress) dataDP.getSocketAddress();
					NumberUtils.integerToByteArray(messageID, ackBuf, 0);
					NumberUtils.shortToByteArray(packetIndex, ackBuf, 4);
					ackDP.setSocketAddress(remoteSocketAddress);
					try {
						this.socket.send(ackDP);
					} catch (IOException ioe) {
						if (this.shutdownRequested) {
							return;
						}
						Log.error("Shutting down receiver thread due to error: ", ioe);
						return;
					}
				}

				// Allocate new memory for next packet
				final InetSocketAddress remoteSocketAddress = (InetSocketAddress) dataDP.getSocketAddress();
				dataBuf = new byte[RPCMessage.MAXIMUM_MSG_SIZE + RPCMessage.METADATA_SIZE];
				dataDP = new DatagramPacket(dataBuf, dataBuf.length);

				if (mpis.isComplete()) {
					this.incompleteInputStreams.remove(msgID);
					this.rpcService.processIncomingRPCMessage(remoteSocketAddress, new Input(mpis));
				}
			}
		}
	}

	/**
	 * Shuts down the network thread.
	 * 
	 * @throws InterruptedException
	 *         thrown if the calling thread is interrupted while waiting for the network thread to shut down
	 */
	void shutdown() throws InterruptedException {
		this.shutdownRequested = true;
		this.socket.close();
		interrupted();
		join();
	}

	/**
	 * Reliably sends the given sequence of datagram packets to the receiver.
	 * 
	 * @param packets
	 *        the sequence of packets to send
	 * @return the number of retries required to send the packet
	 * @throws IOException
	 *         thrown if the sequence of packets could not be delivered within the defined time
	 * @throws InterruptedException
	 *         thrown if the calling thread is interrupted while waiting for the acknowledgments
	 */
	int send(final DatagramPacket[] packets) throws IOException, InterruptedException {
		return send(packets, true);
	}

	/**
	 * Sends the given sequence of datagram packets to the receiver.
	 * 
	 * @param packets
	 *        the sequence of packets to send
	 * @param waitForAck
	 *        <code>true</code> to wait for the acknowledgments of the packets, <code>false</code> otherwise
	 * @return the number of retries required to send the packet
	 * @throws IOException
	 *         thrown if the sequence of packets could not be delivered within the defined time
	 * @throws InterruptedException
	 *         thrown if the calling thread is interrupted while waiting for the acknowledgments
	 */
	int send(final DatagramPacket[] packets, final boolean waitForAck) throws IOException, InterruptedException {

		if (packets.length == 0) {
			return 0;
		}

		final OutstandingTransmission outstandingTransmission = new OutstandingTransmission();
		final int messageID = NumberUtils.byteArrayToInteger(packets[0].getData(), packets[0].getLength()
			- RPCMessage.METADATA_SIZE + 4);
		final Integer msgID = Integer.valueOf(messageID);

		if (waitForAck) {
			this.outstandingTransmissions.put(msgID, outstandingTransmission);
		}
		int lastAckedPacket = -1;
		int retryCounter = 0;

		try {

			while (retryCounter < MAXIMUM_NUMBER_OF_RETRANSMISSIONS) {

				// Send unacknowledged packets
				for (int j = lastAckedPacket + 1; j < packets.length; ++j) {
					this.socket.send(packets[j]);
				}

				if (!waitForAck) {
					return 0;
				}

				synchronized (outstandingTransmission) {

					while (true) {
						lastAckedPacket = outstandingTransmission.lastAckedPacket;
						if (lastAckedPacket == (packets.length - 1)) {
							break;
						}

						outstandingTransmission.wait(RETRANSMISSION_TIMEOUT);

						if (lastAckedPacket == outstandingTransmission.lastAckedPacket) {
							// We did not receive a single ACK during the last wait period
							break;
						}
					}
				}

				if (lastAckedPacket == (packets.length - 1)) {
					break;
				}

				++retryCounter;
			}

		} finally {

			if (waitForAck) {
				// Remove outstanding transmission
				this.outstandingTransmissions.remove(msgID);
			}
		}

		if (lastAckedPacket != (packets.length - 1) && waitForAck) {
			throw new IOException("Unable to send RPC request to " + packets[0].getSocketAddress());
		}

		return retryCounter;
	}

	/**
	 * Cleans up stale state information as a result of packet loss.
	 */
	void cleanUpStaleState() {

		final long now = System.currentTimeMillis();
		final Iterator<MultiPacketInputStream> it = this.incompleteInputStreams.values().iterator();
		while (it.hasNext()) {

			if ((it.next().getCreationTime() + RPCService.CLEANUP_INTERVAL) < now) {
				it.remove();
			}
		}
	}
}
