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
package com.flipkart.aesop.hbasedatalayer.upsert;

import com.flipkart.aesop.destinationoperation.JDBCDataLayer;
import com.flipkart.aesop.destinationoperation.UpsertDestinationStoreProcessor;
import com.flipkart.aesop.destinationoperation.utils.DataLayerConstants;
import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.hbasedatalayer.delete.HBaseDeleteDataLayer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.core.DbusOpcode;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import java.util.Map;

/**
 * HBase Update Data Layer. Persists {@link DbusOpcode#UPSERT} events to HBase through Phoenix.
 * @author Prakhar Jain
 * @see HBaseDeleteDataLayer
 */
public class HBaseUpsertDataLayer extends UpsertDestinationStoreProcessor implements JDBCDataLayer
{
	/** Logger for this class. */
	public static final Logger LOGGER = LogFactory.getLogger(HBaseUpsertDataLayer.class);

	/** JDBC Template Map, with Namespace name as key and the corresponding JDBC template as value. */
	Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap;

	/**
	 * Field Constructor.
	 * @param jdbcTemplateMap
	 */
	public HBaseUpsertDataLayer(Map<String, NamedParameterJdbcTemplate> jdbcTemplateMap)
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
	 * Generates Upsert Query using {@link #buildQuery(String, String, StringBuilder, StringBuilder)} and
	 * {@link #populateQueryParts(Map, StringBuilder, StringBuilder)} helper functions.
	 * @param event
	 * @return Upsert Query.
	 */
	private String generateUpsertQuery(AbstractEvent event)
	{
		StringBuilder columnNameStringBuilder = new StringBuilder();
		StringBuilder placeholderStringBuilder = new StringBuilder();

		populateQueryParts(event.getFieldMapPair(), columnNameStringBuilder, placeholderStringBuilder);

		return buildQuery(event.getNamespaceName(), event.getEntityName(), columnNameStringBuilder,
		        placeholderStringBuilder);

	}

	/**
	 * Helper function for {@link #generateUpsertQuery(AbstractEvent)}.
	 * @param namespace
	 * @param entity
	 * @param columnNameStringBuilder
	 * @param placeholderStringBuilder
	 * @return Upsert Query
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
	 * Helper function for {@link #generateUpsertQuery(AbstractEvent)}.
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
