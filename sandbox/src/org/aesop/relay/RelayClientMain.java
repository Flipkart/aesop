/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus. 
 */

package org.aesop.relay;

import com.linkedin.databus.client.DatabusHttpClientImpl;

/**
 * Brings up a Databus Relay Client for change events of a specific type i.e. org.aesop.events.example.person.Person. Note that this client connects to a Relay
 * that is assumed to be running on host and port as defined in the {@link #main(String[])} method. 
 * 
 * Note: Include all jars in "lib" in the classpath before executing this class.
 * 
 * <pre>
 *  java -cp .:lib/*.jar org.aesop.relay.RelayClientMain
 * <pre>
 * 
 * @author Regunath B
 *
 */
public class RelayClientMain extends Thread {
	static final String PERSON_SOURCE = "com.linkedin.events.example.person.Person";

	private int identifier;
	private DatabusHttpClientImpl client;

	public RelayClientMain(int identifier, DatabusHttpClientImpl client) {
		this.identifier = identifier;
		this.client = client;
	}

	public void run() {
		try {
			// Instantiate a client using command-line parameters if any
			// register callbacks
			PersonConsumer personConsumer = new PersonConsumer(this.identifier);
			client.registerDatabusStreamListener(personConsumer, null,
					PERSON_SOURCE);
			client.registerDatabusBootstrapListener(personConsumer, null,
					PERSON_SOURCE);

			// fire off the Databus client
			client.startAndBlock();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		DatabusHttpClientImpl.Config configBuilder = new DatabusHttpClientImpl.Config();

		// Try to connect to a relay on localhost
		configBuilder.getRuntime().getRelay("1").setHost("localhost");
		configBuilder.getRuntime().getRelay("1").setPort(11115);
		configBuilder.getRuntime().getRelay("1").setSources(PERSON_SOURCE);

		DatabusHttpClientImpl client = DatabusHttpClientImpl.createFromCli(
				args, configBuilder);

		new RelayClientMain(1, client).start();
	}

}
