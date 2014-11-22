package com.flipkart.aesop.consoleappenderdatalayer.upsert;

import org.springframework.beans.factory.FactoryBean;

/**
 * Generates objects of {@link ConsoleAppenderUpsertDataLayer } and ensures that it is singleton.
 * @author Jagadeesh Huliyar
 */
public class ConsoleAppenderUpsertDataLayerFactory  implements FactoryBean<ConsoleAppenderUpsertDataLayer>
{

	public ConsoleAppenderUpsertDataLayer getObject() throws Exception
    {
	    return new ConsoleAppenderUpsertDataLayer();
    }

	public Class<?> getObjectType()
    {
	    return ConsoleAppenderUpsertDataLayer.class;
    }

	public boolean isSingleton()
    {
	    return true;
    }
}
