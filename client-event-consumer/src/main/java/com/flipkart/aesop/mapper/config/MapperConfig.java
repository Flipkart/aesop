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

package com.flipkart.aesop.mapper.config;

import com.flipkart.aesop.mapper.config.implementation.MapperConfigImpl;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

/**
 * Singleton class which implements HOCON-config related functions.
 * @author Prakhar Jain
 * @see MapperConfigImpl
 */
public interface MapperConfig
{
	/**
	 * Reads the HOCON-config from the path and stores as Concurrent Hash Map.
	 * @param filePath
	 */
	void readConfig(String filePath);

	/**
	 * Returns the stored {@link Config} from the Concurrent Hash Map.
	 * @param filePath
	 * @return {@link Config}
	 */
	Config getConfig(String filePath);

	/**
	 * Returns {@link ConfigList} if that keyPath contains an array.
	 * @param filePath
	 * @param keyPath
	 * @return {@link ConfigList}
	 */
	ConfigList getConfigList(String filePath, String keyPath);

	/**
	 * Returns {@link ConfigList} if that keyPath contains an array.
	 * @param config
	 * @param keyPath
	 * @return
	 */
	ConfigList getConfigList(Config config, String keyPath);

	/**
	 * Returns {@link ConfigValue} present at the specified keyPath.
	 * @param config
	 * @param keyPath
	 * @return {@link ConfigValue}
	 */
	ConfigValue getConfigValue(Config config, String keyPath);

	/**
	 * Get value for the key in the {@link ConfigValue} as string.
	 * @param key
	 * @param configValue
	 * @return Value as string
	 */
	String getValueForKeyAsString(String key, ConfigValue configValue);

	/**
	 * Get value for the key in the {@link ConfigValue} as {@link ConfigValue} or a class implementing
	 * {@link ConfigValue}.
	 * @param key
	 * @param configValue
	 * @param classType
	 * @return Value as {@link ConfigValue} or a class implementing {@link ConfigValue}
	 */
	<T extends ConfigValue> T getValueForKeyAsConfigValue(String key, ConfigValue configValue, Class<T> classType);

	/**
	 * Checks if the specified path exists in the {@link ConfigValue}
	 * @param keyPath
	 * @param configValue
	 * @return
	 */
	Boolean checkIfPathExists(String keyPath, ConfigValue configValue);

	/**
	 * Checks if the specified path exists in the {@link Config}
	 * @param keyPath
	 * @param config
	 * @return
	 */
	Boolean checkIfPathExists(String keyPath, Config config);

	/**
	 * Checks if the specified key exists in the {@link ConfigValue}
	 * @param keyPath
	 * @param configValue
	 * @return
	 */
	Boolean checkIfKeyExists(String keyPath, ConfigValue configValue);
}
