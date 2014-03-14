/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus. 
 */
package org.aesop.bootstrap;

import org.aesop.relay.PersonConsumer;

import com.linkedin.databus.client.CheckpointMessage;
import com.linkedin.databus.client.DatabusHttpClientImpl;
import com.linkedin.databus.client.DatabusSourcesConnection;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.async.LifecycleMessage;

/**
 * A sample client that consumes snapshots served by a {@link GenericBootstrapHttpServerMain} or similar Databus bootstrap server accessible at localhost:11111. 
 * Also connects and consumes change events as stream from a Relay running at localhost:11115 Note that the parameters passed for fetching snapshots is not externalized but rather hard-coded in this class.
 * Value of Parameter such as "bootstrap_target_scn" would need to be changed depending on values found in column 'windowscn' in the MySQL table 'bootstrap_producer_state'. 
 * 
 * Note: Include all jars in "lib" in the classpath before executing this class. 
 * 
 * <pre>
 * java -cp .:lib/*.jar org.aesop.bootstrap.PersonBootstrapClientMain
 * 
 * <pre>
 * 
 * @author Regunath B
 *
 */
public class PersonBootstrapClientMain {

	public static final String PERSON_SOURCE = "org.aesop.events.example.person.Person";

	public static void main(String[] args) throws Exception {
		
		DatabusHttpClientImpl.Config configBuilder = new DatabusHttpClientImpl.Config();
		// Try to connect to a bootstrap on localhost
		configBuilder.getRuntime().getBootstrap().getService("1").setName("bst1");
		configBuilder.getRuntime().getBootstrap().getService("1").setHost("localhost");
		configBuilder.getRuntime().getBootstrap().getService("1").setPort(11111);
		configBuilder.getRuntime().getBootstrap().getService("1").setSources(PERSON_SOURCE);
		
		// Try to connect to a relay on localhost
		configBuilder.getRuntime().getRelay("1").setHost("localhost");
		configBuilder.getRuntime().getRelay("1").setPort(11115);
		configBuilder.getRuntime().getRelay("1").setSources(PERSON_SOURCE);
		
		DatabusHttpClientImpl client = DatabusHttpClientImpl.createFromCli(
				args, configBuilder);
		try {
			// Instantiate a client using command-line parameters if any
			// register callbacks
			PersonConsumer personConsumer = new PersonConsumer();
			client.registerDatabusStreamListener(personConsumer, null,
					PERSON_SOURCE);
			client.registerDatabusBootstrapListener(personConsumer, null,
					PERSON_SOURCE);

			// fire off the Databus client
			client.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// wait for a while and simulate a slow consumer requesting a bootstrap snapshot
		
		Thread.sleep(2000);
		
		for (DatabusSourcesConnection con : client.getRelayConnections()) {
			Checkpoint cp = new Checkpoint();
			cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_SNAPSHOT);
			/*
			cp.setConsumptionMode(DbusClientMode.BOOTSTRAP_CATCHUP);
			cp.setCatchupSource("org.aesop.events.example.person.Person");
			cp.setWindowOffset(-1L);
			cp.setSnapshotOffset(-1L);
			cp.setPrevScn(-1L);
			cp.setWindowScn(0L);
			cp.setBootstrapTargetScn(900L);		
			*/					
			CheckpointMessage cpM = CheckpointMessage.createSetCheckpointMessage(cp);
			con.getBootstrapPuller().doExecuteAndChangeState(cpM);
			con.getBootstrapPuller().enqueueMessage(LifecycleMessage.createStartMessage());
		}
		
	}
	
}
