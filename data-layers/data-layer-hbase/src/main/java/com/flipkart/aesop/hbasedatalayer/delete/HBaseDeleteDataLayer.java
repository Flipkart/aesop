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
package com.flipkart.aesop.hbasedatalayer.delete;

import com.flipkart.aesop.destinationoperation.DeleteDestinationStoreProcessor;
import com.flipkart.aesop.destinationoperation.JDBCDataLayer;
import com.flipkart.aesop.destinationoperation.utils.DataLayerConstants;
import com.flipkart.aesop.destinationoperation.utils.DataLayerHelper;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.hbasedatalayer.upsert.HBaseUpsertDataLayer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.core.DbusOpcode;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.util.Map;

/**
 * HBase Delete Data Layer. Persists {@link DbusOpcode#DELETE} events to HBase through Phoenix.
 * @author Prakhar Jain
 * @see HBaseUpsertDataLayer
 */
public class HBaseDeleteDataLayer extends DeleteDestinationStoreProcessor implements JDBCDataLayer
{
	/** Logger for this class. */
	public static final Logger LOGGER = LogFactory.getLogger(HBaseUpsertDataLayer.class);

	/** JDBC Template Map, with Namespace name as key and the corresponding JDBC template as value. */
	private Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap;

	/**
	 * Field Constructor.
	 * @param jdbcTemplateMap jdbcTemplateMap
	 */
	public HBaseDeleteDataLayer(Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap)
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
		String deleteQuery = generateDeleteQuery(event);
		NamedParameterJdbcTemplate jdbcTemplate = jdbcTemplateMap.get(event.getNamespaceName());
		Map<String, Object> nullValueColumnMapping =
		        DataLayerHelper.generateColumnMappingWithNullValues(event.getFieldMapPair(), event.getPrimaryKeySet());
		jdbcTemplate.update(deleteQuery, nullValueColumnMapping);
        return ConsumerCallbackResult.SUCCESS;
	}

	/**
	 * Generates Delete Query using {@link #buildQuery(String, String, StringBuilder, StringBuilder)} and
	 * {@link #populateQueryParts(Map, StringBuilder, StringBuilder)} helper functions.
	 * @param event event
	 * @return Delete Query.
	 */
	private String generateDeleteQuery(AbstractEvent event)
	{
		StringBuilder columnNameStringBuilder = new StringBuilder();
		StringBuilder placeholderStringBuilder = new StringBuilder();

		populateQueryParts(event.getFieldMapPair(), columnNameStringBuilder, placeholderStringBuilder);

		return buildQuery(event.getNamespaceName(), event.getEntityName(), columnNameStringBuilder,
		        placeholderStringBuilder);

	}

	/**
	 * Helper function for {@link #generateDeleteQuery(AbstractEvent)}.
	 * @param namespace namespace
	 * @param entity entity
	 * @param columnNameStringBuilder columnNameStringBuilder
	 * @param placeholderStringBuilder placeholderStringBuilder
	 * @return Delete Query
	 */
	private String buildQuery(String namespace, String entity, StringBuilder columnNameStringBuilder,
	        StringBuilder placeholderStringBuilder)
	{
		StringBuilder upsertQuery = new StringBuilder();

		upsertQuery.append("UPSERT INTO ");
		if (namespace != null && !"".equals(namespace))
		{
			upsertQuery.append(namespace + ".");
		}
		upsertQuery.append(entity + "(");
		upsertQuery.append(columnNameStringBuilder.substring(0, columnNameStringBuilder.length() - 1));
		upsertQuery.append(") VALUES(");
		upsertQuery.append(placeholderStringBuilder.substring(0, placeholderStringBuilder.length() - 1));
		upsertQuery.append(")");

		return upsertQuery.toString();
	}

	/**
	 * Helper function for {@link #generateDeleteQuery(AbstractEvent)}.
	 * @param fieldMap
	 * @param columnNameStringBuilder
	 * @param placeholderStringBuilder
	 */
	private void populateQueryParts(Map<String, Object> fieldMap, StringBuilder columnNameStringBuilder,
	        StringBuilder placeholderStringBuilder)
	{
		for (String columnName : fieldMap.keySet())
		{
			columnNameStringBuilder.append(columnName + DataLayerConstants.COMMA);
			placeholderStringBuilder.append(DataLayerConstants.COLON + columnName + DataLayerConstants.COMMA);
		}
	}
}
