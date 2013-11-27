/*
 * WARNING : This code is ugly. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus. 
 */

package org.aesop.relay;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.container.netty.HttpRelay;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.data_model.PhysicalPartition;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.seq.MultiServerSequenceNumberHandler;
import com.linkedin.databus2.core.seq.SequenceNumberHandlerFactory;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.relay.DatabusRelayMain;
import com.linkedin.databus2.relay.config.LogicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/**
 * Brings up a Databus Relay for change events of a specific type i.e. org.aesop.events.example.person.Person.
 * The custom change event producer is also statically registered with this relay.
 * 
 * @author Regunath B
 *
 */
public class RelayMain extends DatabusRelayMain {
	
  public static final String MODULE = RelayMain.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);
  static final String FULLY_QUALIFIED_PERSON_EVENT_NAME = "org.aesop.events.example.person.Person";
  static final int PERSON_SRC_ID = 40;

  MultiServerSequenceNumberHandler _maxScnReaderWriters;
  protected Map<PhysicalPartition, EventProducer> _producers;

  public RelayMain() throws IOException, InvalidConfigException, DatabusException
  {
    this(new HttpRelay.Config(), null);
  }

  public RelayMain(HttpRelay.Config config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    this(config.build(), pConfigs);
  }

  public RelayMain(HttpRelay.StaticConfig config, PhysicalSourceStaticConfig [] pConfigs)
  throws IOException, InvalidConfigException, DatabusException
  {
    super(config, pConfigs);
	SequenceNumberHandlerFactory handlerFactory = _relayStaticConfig
			.getDataSources().getSequenceNumbersHandler().createFactory();
	_maxScnReaderWriters = new MultiServerSequenceNumberHandler(
			handlerFactory);    

  }
  
  public void customInitProducers(PhysicalSourceStaticConfig pConfig) throws Exception {
	 // add our producer
	 DbusEventBufferAppendable dbusEventBuffer = getEventBuffer().getDbusEventBufferAppendable(101);
	 PersonEventProducer producer = new PersonEventProducer(dbusEventBuffer,
			 getMaxSCNReaderWriter(pConfig),_inBoundStatsCollectors.getStatsCollector("statsCollector"),
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
		RelayMain serverContainer = new RelayMain(staticConfig, pStaticConfigs);

		serverContainer.initProducers();
		serverContainer.customInitProducers(pStaticConfigs[0]);

		serverContainer.startAndBlock();
	}
  
}
