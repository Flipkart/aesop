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
package com.flipkart.aesop.mysqldatalayer.delete;

import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreProcessor;
import com.flipkart.aesop.destinationoperation.JDBCDataLayer;
import com.flipkart.aesop.destinationoperation.utils.DataLayerConstants;
import com.flipkart.aesop.destinationoperation.utils.DataLayerHelper;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.mysqldatalayer.upsert.MySQLUpsertDataLayer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.core.DbusOpcode;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Map;
import java.util.Set;

/**
 * MySQL Delete Data Layer. Persists {@link DbusOpcode#DELETE} events to MySQL.
 * @author Prakhar Jain
 * @see MySQLUpsertDataLayer
 */
public class MySQLDeleteDataLayer extends DeleteDestinationStoreProcessor implements JDBCDataLayer
{
	/** JDBC Template Map, with Namespace name as key and the corresponding JDBC template as value. */
	private Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap;

	/**
	 * Field Constructor.
	 * @param jdbcTemplateMap
	 */
	public MySQLDeleteDataLayer(Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap)
	{
		this.jdbcTemplateMap = jdbcTemplateMap;
	}

	public Map<String, NamedParameterJdbcTemplate> getJdbcTemplateMap()
	{
		return jdbcTemplateMap;
	}

	@Override
	protected ConsumerCallbackResult delete(AbstractEvent event)
	{
		String deleteQuery = getDeleteQuery(event);
		NamedParameterJdbcTemplate jdbcTemplate = jdbcTemplateMap.get(event.getNamespaceName());
		Map<String, Object> nullValueColumnMapping =
		        DataLayerHelper.generateColumnMappingWithNullValues(event.getFieldMapPair(), event.getPrimaryKeySet());
		jdbcTemplate.update(deleteQuery, nullValueColumnMapping);
        return ConsumerCallbackResult.SUCCESS;
	}

	/**
	 * Generates Delete Query using {@link #buildQuery(String, String, StringBuilder, StringBuilder)} and
	 * {@link #populateDeleteQueryParts(Map, Set, StringBuilder, StringBuilder)} helper functions.
	 * @param event
	 * @return Delete Query
	 */
	private String getDeleteQuery(AbstractEvent event)
	{
		StringBuilder updateClauseStringBuilder = new StringBuilder();
		StringBuilder whereClauseStringbuilder = new StringBuilder();

		populateDeleteQueryParts(event.getFieldMapPair(), event.getPrimaryKeySet(), updateClauseStringBuilder,
		        whereClauseStringbuilder);

		return buildQuery(event.getNamespaceName(), event.getEntityName(), updateClauseStringBuilder,
		        whereClauseStringbuilder);
	}

	/**
	 * Helper Function for {@link #getDeleteQuery(AbstractEvent)}.
	 * @param fieldMap
	 * @param primaryKeySet
	 * @param updateClauseStringBuilder
	 * @param whereClauseStringbuilder
	 */
	private void populateDeleteQueryParts(Map<String, Object> fieldMap, Set<String> primaryKeySet,
	        StringBuilder updateClauseStringBuilder, StringBuilder whereClauseStringbuilder)
	{
		for (String columnName : fieldMap.keySet())
		{
			if (!primaryKeySet.contains(columnName))
			{
				updateClauseStringBuilder.append(columnName + DataLayerConstants.EQUALTO_AND_COLON + columnName
				        + DataLayerConstants.COMMA);
			}
			else
			{
				whereClauseStringbuilder.append(columnName + DataLayerConstants.EQUALTO_AND_COLON + columnName
				        + DataLayerConstants.AND);
			}
		}
	}

	/**
	 * Helper Function for {@link #getDeleteQuery(AbstractEvent)}.
	 * @param namespace
	 * @param entity
	 * @param updateClauseStringBuilder
	 * @param whereClauseStringbuilder
	 * @return Delete Query
	 */
	private String buildQuery(String namespace, String entity, StringBuilder updateClauseStringBuilder,
	        StringBuilder whereClauseStringbuilder)
	{
		StringBuilder deleteQuery = new StringBuilder();

		deleteQuery.append("UPDATE ");
		deleteQuery.append(namespace + "." + entity);
		deleteQuery.append(" SET ");
		deleteQuery.append(updateClauseStringBuilder.substring(0, updateClauseStringBuilder.length() - 1));
		deleteQuery.append(" WHERE ");
		deleteQuery.append(whereClauseStringbuilder.substring(0, whereClauseStringbuilder.length() - 5));

		return deleteQuery.toString();
	}
}
