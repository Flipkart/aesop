/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus. 
 */

package org.aesop.relay;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.container.netty.HttpRelay;
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
 * org.aesop.events.example.person.Person. The custom change event producer is
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

	private void customInitProducers(PhysicalSourceStaticConfig pConfig)
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

		_dbRelayConfigFiles = new String[] { "conf/sources-person.json" };

		String[] leftOverArgs = processLocalArgs(args);

		// Process the startup properties and load configuration
		Properties startupProps = ServerContainer
				.processCommandLineArgs(leftOverArgs);
		Config config = new Config();
		ConfigLoader<StaticConfig> staticConfigLoader = new ConfigLoader<StaticConfig>(
				"databus.relay.", config);

		// read physical config files
		ObjectMapper mapper = new ObjectMapper();
		PhysicalSourceConfig[] physicalSourceConfigs = new PhysicalSourceConfig[_dbRelayConfigFiles.length];
		PhysicalSourceStaticConfig[] pStaticConfigs = new PhysicalSourceStaticConfig[physicalSourceConfigs.length];

		int i = 0;
		for (String file : _dbRelayConfigFiles) {
			LOG.info("processing file: " + file);
			File sourcesJson = new File(file);
			PhysicalSourceConfig pConfig = mapper.readValue(sourcesJson,
					PhysicalSourceConfig.class);
			pConfig.checkForNulls();
			physicalSourceConfigs[i] = pConfig;
			pStaticConfigs[i] = pConfig.build();

			// Register all sources with the static config
			for (LogicalSourceConfig lsc : pConfig.getSources()) {
				config.setSourceName("" + lsc.getId(), lsc.getName());
			}
			i++;
		}

		HttpRelay.StaticConfig staticConfig = staticConfigLoader
				.loadConfig(startupProps);

		// Create and initialize the server instance
		RelayMain relay = new RelayMain(staticConfig, pStaticConfigs);

		relay.initProducers();
		relay.customInitProducers(pStaticConfigs[0]);

		relay.startAndBlock();
	}

}
