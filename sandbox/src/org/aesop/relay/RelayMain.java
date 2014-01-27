/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus. 
 */

package org.aesop.relay;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.aesop.events.example.person.Person;
import org.aesop.relay.hbase.WALEditRelayMain;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.container.netty.HttpRelay.Cli;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.relay.DatabusRelayMain;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/**
 * Brings up a Databus Relay for change events of a specific type i.e.
 * {@link Person} The custom change event producer is
 * also statically registered with this relay. Uses code as-is, modified or in
 * parts from the Databus sample or main codebase.
 * 
 * Note: Include all jars in "lib" in the classpath before executing this class.
 * Also preserve relative location of directories like "conf".
 * 
 * <pre>
 * java -cp .:lib/*.jar org.aesop.relay.RelayMain -p conf/relay_person.properties
 * 
 * <pre>
 * 
 * @author Regunath B
 * 
 */
public class RelayMain extends DatabusRelayMain {

	public RelayMain() throws IOException, InvalidConfigException,
			DatabusException {
		this(new HttpRelay.Config(), null);
	}

	public RelayMain(HttpRelay.Config config,
			PhysicalSourceStaticConfig[] pConfigs) throws IOException,
			InvalidConfigException, DatabusException {
		this(config.build(), pConfigs);
	}

	public RelayMain(HttpRelay.StaticConfig config,
			PhysicalSourceStaticConfig[] pConfigs) throws IOException,
			InvalidConfigException, DatabusException {
		super(config, pConfigs);

	}

	protected void customInitProducers(PhysicalSourceStaticConfig pConfig)
			throws Exception {
		// add our producer
		DbusEventBufferAppendable dbusEventBuffer = getEventBuffer()
				.getDbusEventBufferAppendable(101);
		PersonEventProducer producer = new PersonEventProducer(dbusEventBuffer,
				getMaxSCNReaderWriter(pConfig),
				_inBoundStatsCollectors.getStatsCollector("statsCollector"),
				getSchemaRegistryService(), pConfig);
		producer.start(getMaxSCNReaderWriter(pConfig).getMaxScn());
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		 Cli cli = new Cli();
		 cli.setDefaultPhysicalSrcConfigFiles("conf/sources-person.json");
		 cli.processCommandLineArgs(args);
		 cli.parseRelayConfig();
		 // Process the startup properties and load configuration
		 PhysicalSourceStaticConfig[] pStaticConfigs = cli.getPhysicalSourceStaticConfigs();
		 HttpRelay.StaticConfig staticConfig = cli.getRelayConfigBuilder().build();
		
		 // Create and initialize the server instance
		 RelayMain relay = new RelayMain(staticConfig, pStaticConfigs);
		
		 relay.initProducers();
		 relay.customInitProducers(pStaticConfigs[0]);
		 relay.registerShutdownHook();
		 relay.startAndBlock();
		 
	}

}
