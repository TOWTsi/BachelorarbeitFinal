import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.rpc.RPCEnvelope;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.nephele.rpc.ServerTypeUtils;

public class Main {

	// private final ExtendedManagementProtocol jobManager;

	public static void main(String[] args) throws Exception {


		final String JOBMANAGER_ADDRESS = "130.149.249.5";
		final int JOBMANAGER_PORT = 6127;

		final InetSocketAddress inetaddr = new InetSocketAddress(JOBMANAGER_ADDRESS, JOBMANAGER_PORT);
		ExtendedManagementProtocol jobManager = null;

		try {

			RPCService rpc = new RPCService(ServerTypeUtils.getRPCTypesToRegister());;
			jobManager = (ExtendedManagementProtocol) rpc.getProxy(inetaddr, ExtendedManagementProtocol.class);
			

		} catch (IOException e) {

			e.printStackTrace();
			System.exit(1);
			return;
		}
		
		JobFailureSimulator sim = new JobFailureSimulator(jobManager);
		sim.go();

	}
}
