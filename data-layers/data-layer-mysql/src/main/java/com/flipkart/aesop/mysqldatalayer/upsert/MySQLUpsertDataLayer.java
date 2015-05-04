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
package com.flipkart.aesop.mysqldatalayer.upsert;

import com.flipkart.aesop.destinationoperation.JDBCDataLayer;
import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreProcessor;
import com.flipkart.aesop.destinationoperation.utils.DataLayerConstants;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.mysqldatalayer.delete.MySQLDeleteDataLayer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.core.DbusOpcode;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Map;
import java.util.Set;

/**
 * MySQL Upsert Data Layer. Persists {@link DbusOpcode#UPSERT} events to MySQL.
 * @author Prakhar Jain
 * @see MySQLDeleteDataLayer
 */
public class MySQLUpsertDataLayer extends UpsertDestinationStoreProcessor implements JDBCDataLayer
{
	/** JDBC Template Map, with Namespace name as key and the corresponding JDBC template as value. */
	private Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap;

	/**
	 * Field Constructor.
	 * @param jdbcTemplateMap
	 */
	public MySQLUpsertDataLayer(Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap)
	{
		this.jdbcTemplateMap = jdbcTemplateMap;
	}

	public Map<String, NamedParameterJdbcTemplate> getJdbcTemplateMap()
	{
		return jdbcTemplateMap;
	}

	@Override
	protected ConsumerCallbackResult upsert(AbstractEvent event)
	{
		String upsertQuery = generateUpsertQuery(event);
		NamedParameterJdbcTemplate jdbcTemplate = jdbcTemplateMap.get(event.getNamespaceName());
		jdbcTemplate.update(upsertQuery, event.getFieldMapPair());
        return ConsumerCallbackResult.SUCCESS;
	}

	/**
	 * Generates Upsert Query using {@link #buildQuery(String, String, StringBuilder, StringBuilder, StringBuilder)} and
	 * {@link #populateUpsertQueryParts(Map, Set, StringBuilder, StringBuilder, StringBuilder)} helper functions.
	 * @param event
	 * @return Upsert Query
	 */
	private String generateUpsertQuery(AbstractEvent event)
	{
		StringBuilder columnNameStringBuilder = new StringBuilder();
		StringBuilder columnValueStringBuilder = new StringBuilder();
		StringBuilder updateQueryStringBuilder = new StringBuilder();

		populateUpsertQueryParts(event.getFieldMapPair(), event.getPrimaryKeySet(), columnNameStringBuilder,
		        columnValueStringBuilder, updateQueryStringBuilder);

		return buildQuery(event.getNamespaceName(), event.getEntityName(), columnNameStringBuilder,
		        columnValueStringBuilder, updateQueryStringBuilder);
	}

	/**
	 * Helper Function for {@link #generateUpsertQuery(AbstractEvent)}.
	 * @param fieldMap
	 * @param primaryKeySet
	 * @param columnNameStringBuilder
	 * @param columnValueStringBuilder
	 * @param updateQueryStringBuilder
	 */
	private void populateUpsertQueryParts(Map<String, Object> fieldMap, Set<String> primaryKeySet,
	        StringBuilder columnNameStringBuilder, StringBuilder columnValueStringBuilder,
	        StringBuilder updateQueryStringBuilder)
	{
		for (String field : fieldMap.keySet())
		{
			columnNameStringBuilder.append(field + DataLayerConstants.COMMA);
			columnValueStringBuilder.append(DataLayerConstants.COLON + field + DataLayerConstants.COMMA);
			if (!primaryKeySet.contains(field))
			{
				updateQueryStringBuilder.append(field + DataLayerConstants.EQUALTO_AND_COLON + field + ",");
			}
		}
	}

	/**
	 * Helper Function for {@link #generateUpsertQuery(AbstractEvent)}.
	 * @param namespace
	 * @param entity
	 * @param columnNameStringBuilder
	 * @param columnValueStringBuilder
	 * @param updateQueryStringBuilder
	 * @return Upsert Query
	 */
	private String buildQuery(String namespace, String entity, StringBuilder columnNameStringBuilder,
	        StringBuilder columnValueStringBuilder, StringBuilder updateQueryStringBuilder)
	{
		StringBuilder query = new StringBuilder();

		query.append("INSERT INTO ");
		query.append(namespace + "." + entity + " (");
		query.append(columnNameStringBuilder.substring(0, columnNameStringBuilder.length() - 1));
		query.append(") VALUES(");
		query.append(columnValueStringBuilder.substring(0, columnValueStringBuilder.length() - 1));
		query.append(")  ON DUPLICATE KEY UPDATE ");
		query.append(updateQueryStringBuilder.substring(0, updateQueryStringBuilder.length() - 1));

		return query.toString();
	}
}
