/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus. 
 */
package org.aesop.bootstrap;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import com.linkedin.databus.bootstrap.producer.BootstrapProducerConfig;
import com.linkedin.databus.bootstrap.producer.BootstrapProducerStaticConfig;
import com.linkedin.databus.bootstrap.producer.DatabusBootstrapProducer;
import com.linkedin.databus.client.pub.DatabusClientException;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.container.request.BootstrapDBException;

/**
 * A sub-type of the {@link DatabusBootstrapProducer} that does nothing additional for now. Brings up a Bootstrap producer that
 * consumes off a relay (specified in the startup config file) and writes the change events to a MySQL data store
 * 
 * Note: Include all jars in "lib" in the classpath before executing this class. Ensure you create a database on MySQL with name and credentials as specified
 * in conf/databus-bst-producer.properties. Once the database is ready, create tables as defined in database/bootstrap/schema/cdsddl.tab
 * 
 * Also preserve relative location of directories like "conf".
 * 
 * <pre>
 * java -cp .:lib/*.jar org.aesop.bootstrap.GenericBootstrapProducerMain -p conf/databus-bst-producer.properties
 * 
 * <pre>
 * 
 * @author Regunath B
 *
 */
public class GenericBootstrapProducerMain extends DatabusBootstrapProducer {

	public GenericBootstrapProducerMain(BootstrapProducerConfig config)
			throws IOException, InvalidConfigException, InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException,
			DatabusClientException, DatabusException, BootstrapDBException {
		super(config);
	}
	public GenericBootstrapProducerMain(BootstrapProducerStaticConfig bootstrapProducerStaticConfig)
			throws IOException, InvalidConfigException, InstantiationException, IllegalAccessException,
		    ClassNotFoundException, SQLException, DatabusClientException, DatabusException,
		    BootstrapDBException {
		super(bootstrapProducerStaticConfig);
	}

	public static void main(String[] args) throws Exception {
		Properties startupProps = ServerContainer.processCommandLineArgs(args);

		BootstrapProducerConfig producerConfig = new BootstrapProducerConfig();
		ConfigLoader<BootstrapProducerStaticConfig> staticProducerConfigLoader = new ConfigLoader<BootstrapProducerStaticConfig>(
				"databus.bootstrap.", producerConfig);
		BootstrapProducerStaticConfig staticProducerConfig = staticProducerConfigLoader.loadConfig(startupProps);

		GenericBootstrapProducerMain bootstrapProducer = new GenericBootstrapProducerMain(staticProducerConfig);
		bootstrapProducer.registerShutdownHook();
		bootstrapProducer.startAndBlock();
	}

}
