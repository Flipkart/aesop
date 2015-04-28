package com.flipkart.aesop.runtime.bootstrap.producer.registeration;

import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.Properties;

/**
 * This class is the place holder for Physical Producer Configs
 */
public class ProducerRegistration implements InitializingBean
{

	/** The EventProducer to be registered*/
	private EventProducer eventProducer;

	/** The physical databus source configuration*/
	private PhysicalSourceConfig physicalSourceConfig;

    /* Load AdditionalProperties if Its Present */
    private Properties properties;

	/**
	 * Interface method implementation. Ensures that a EventProducer and PhysicalSourceConfig is set
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.eventProducer, "'eventProducer' cannot be null. An EventProducer must be specified");
		Assert.notNull(this.physicalSourceConfig,"'physicalSourceConfig' cannot be null. A PhysicalSourceConfig must be specified");
	}

	/** Getter/Setter methods*/
	public EventProducer getEventProducer() {
		return this.eventProducer;
	}
	public void setEventProducer(EventProducer eventProducer) {
		this.eventProducer = eventProducer;
	}
	public PhysicalSourceConfig getPhysicalSourceConfig() {
		return this.physicalSourceConfig;
	}
	public void setPhysicalSourceConfig(PhysicalSourceConfig physicalSourceConfig) {
		this.physicalSourceConfig = physicalSourceConfig;
	}
	public Properties getProperties() {
		return properties;
	}
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
}

