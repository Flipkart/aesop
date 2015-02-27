package com.flipkart.aesop.consoleappenderdatalayer.delete;

import org.springframework.beans.factory.FactoryBean;

/**
 * Generates objects of {@link ConsoleAppenderDeleteDataLayer} and ensures that it is singleton.
 * @author Jagadeesh Huliyar
 */
public class ConsoleAppenderDeleteDataLayerFactory  implements FactoryBean<ConsoleAppenderDeleteDataLayer>
{

	public ConsoleAppenderDeleteDataLayer getObject() throws Exception
    {
	    return new ConsoleAppenderDeleteDataLayer();
    }

	public Class<?> getObjectType()
    {
	    return ConsoleAppenderDeleteDataLayer.class;
    }

	public boolean isSingleton()
    {
	    return true;
    }
}
