/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus. 
 */
package com.flipkart.aesop.bootstrap;

import java.io.IOException;
import java.util.Properties;

import com.linkedin.databus.bootstrap.server.BootstrapHttpServer;
import com.linkedin.databus.bootstrap.server.BootstrapServerConfig;
import com.linkedin.databus.bootstrap.server.BootstrapServerStaticConfig;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;

/**
 * A sub-type of the {@link BootstrapHttpServer} that does nothing additional for now. Brings up a Bootstrap Http server that processes
 * Bootstrap requests from consumers. Bootstrap is often used by slow (or) new consumers that run catchup queries to apply changes from inception before
 * they get into streaming mode for consuming live updates.
 * 
 * Note: Include all jars in "lib" in the classpath before executing this class. 
 * 
 * Also preserve relative location of directories like "conf".
 * 
 * <pre>
 * java -cp .:lib/*.jar com.flipkart.aesop.bootstrap.GenericBootstrapHttpServerMain -p conf/databus-bst-server.properties
 * 
 * <pre>
 * 
 * @author Regunath B
 *
 */

public class GenericBootstrapHttpServerMain extends BootstrapHttpServer {

	public GenericBootstrapHttpServerMain(
			BootstrapServerStaticConfig bootstrapServerConfig)
			throws IOException, InvalidConfigException, DatabusException {
		super(bootstrapServerConfig);
	}

	public static void main(String[] args) throws Exception {
		// use server container to pass the command line
		Properties startupProps = ServerContainer.processCommandLineArgs(args);

		BootstrapServerConfig config = new BootstrapServerConfig();

		ConfigLoader<BootstrapServerStaticConfig> configLoader = new ConfigLoader<BootstrapServerStaticConfig>(
				"databus.bootstrap.", config);

		BootstrapServerStaticConfig staticConfig = configLoader
				.loadConfig(startupProps);

		GenericBootstrapHttpServerMain bootstrapServer = new GenericBootstrapHttpServerMain(
				staticConfig);

		// Bind and start to accept incoming connections.
		try {
			bootstrapServer.registerShutdownHook();
			bootstrapServer.startAndBlock();
		} catch (Exception e) {
			LOG.error("Error starting the bootstrap server", e);
		}
		LOG.info("Exiting bootstrap server");
	}

}
