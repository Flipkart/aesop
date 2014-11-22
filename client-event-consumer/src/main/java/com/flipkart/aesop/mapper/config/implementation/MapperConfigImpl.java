/*******************************************************************************
 *
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 *******************************************************************************/

package com.flipkart.aesop.mapper.config.implementation;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import com.flipkart.aesop.mapper.config.MapperConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;

/**
 * Implementation of {@link MapperConfig} specific to HOCON-config.
 * @author Prakhar Jain
 */
public class MapperConfigImpl implements MapperConfig
{
	/** Map to store filePath and the corresponding {@link Config}. */
	ConcurrentHashMap<String, Config> cachedConfigMap;

	/** Private Constructor. */
	private MapperConfigImpl()
	{
		cachedConfigMap = new ConcurrentHashMap<String, Config>();
	}

	/** Helper class to create Single instance of this class thread-safely. */
	private static class SingletonHelper
	{
		/** {@link MapperConfigImpl} instance creation. */
		public static final MapperConfig MAPPER_CONFIG = new MapperConfigImpl();
	}

	/**
	 * Loads {@link SingletonHelper} and Gets Instance of this class.
	 * @return Instance
	 */
	public static MapperConfig getInstance()
	{
		return SingletonHelper.MAPPER_CONFIG;
	}

	public void readConfig(String filePath)
	{
		cachedConfigMap.putIfAbsent(filePath, ConfigFactory.parseFile(new File(filePath)));
	}

	public Config getConfig(String filePath)
	{
		if (!cachedConfigMap.contains(filePath))
		{
			readConfig(filePath);
		}
		return cachedConfigMap.get(filePath);
	}

	public ConfigList getConfigList(String filePath, String keyPath)
	{
		Config config = getConfig(filePath);
		return config.getList(keyPath);
	}

	public ConfigList getConfigList(Config config, String keyPath)
	{
		return config.getList(keyPath);
	}

	public ConfigValue getConfigValue(Config config, String keyPath)
	{
		return config.getValue(keyPath);
	}

	public String getValueForKeyAsString(String key, ConfigValue configValue)
	{
		ConfigObject configObject = ((ConfigObject) configValue);
		ConfigValue internalConfigValue = configObject.get(key);
		Object unwrappedConfig = internalConfigValue.unwrapped();
		return unwrappedConfig.toString();
	}

	public <T extends ConfigValue> T getValueForKeyAsConfigValue(String key, ConfigValue configValue, Class<T> classType)
	{
		ConfigObject configObject = (ConfigObject) configValue;
		ConfigValue internalConfigValue = configObject.get(key);
		return classType.cast(internalConfigValue);
	}

	public Boolean checkIfPathExists(String keyPath, ConfigValue configValue)
	{
		Config config = ((Config) configValue);
		return checkIfPathExists(keyPath, config);
	}

	public Boolean checkIfPathExists(String keyPath, Config config)
	{
		return config.hasPath(keyPath);
	}

	public Boolean checkIfKeyExists(String keyPath, ConfigValue configValue)
	{
		ConfigObject configObject = (ConfigObject) configValue;
		return configObject.containsKey(keyPath);
	}
}
