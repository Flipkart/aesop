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

package com.flipkart.aesop.destinationoperation;

import java.util.Map;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Interface to be implemented by any Data Layer which uses JDBC to interact with Destination Store.
 * @author Prakhar Jain
 */
public interface JDBCDataLayer
{
	/**
	 * Gets the JDBC Template Map with key as Namespace and the corresponding JDBC Template as value.
	 * @return JDBC Template Map
	 */
	public Map<String, NamedParameterJdbcTemplate> getJdbcTemplateMap();
}
