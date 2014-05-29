package com.flipkart.aesop.avro.schemagenerator.data;

import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * <code>TableRecord</code> is record which will be mapped to json to generate schema
 * @author yogesh.dahiya
 */

public class TableRecord
{
	/** table name */
	private String name;
	/** schema type */
	private String type;
	/** doc string */
	private String doc;
	/** schema namespace */
	private String namespace;
	/** meta for the table */
	private String meta;
	/** table fields */
	private List<Field> fields;

	public TableRecord(String name, String type, String doc, String namespace, List<String> primaryKeys,
	        List<Field> fields)
	{
		super();
		this.name = name;
		this.type = type;
		this.doc = doc;
		this.namespace = namespace;
		this.meta = "pk=" + StringUtils.join(primaryKeys, ",");
		this.fields = fields;
	}

	/**
	 * <code>Field</code> encapsulate the field schema for the table
	 * @author yogesh.dahiya
	 */
	public static class Field
	{
		/** schema field name */
		private String name;
		/** array of field types */
		private String[] type;
		/** meta for the field */
		private String meta;

		public Field(String dbFieldName, String dbFieldType, int dbFieldPosition)
		{
			/** this can be different from dbFieldName */
			this.name = dbFieldName;
			this.type = new String[]{MysqlToAvroMapper.valueOf(dbFieldType.toUpperCase()).getAvroType(), "null"};
			this.meta =
			        "dbFieldName=" + dbFieldName + ";dbFieldPosition=" + dbFieldPosition + ";dbFieldType="
			                + dbFieldType;
		}

		public String getName()
		{
			return name;
		}

		public void setName(String name)
		{
			this.name = name;
		}

		public String[] getType()
		{
			return type;
		}

		public void setType(String[] type)
		{
			this.type = type;
		}

		public String getMeta()
		{
			return meta;
		}

		public void setMeta(String meta)
		{
			this.meta = meta;
		}

	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public String getType()
	{
		return type;
	}

	public void setType(String type)
	{
		this.type = type;
	}

	public String getDoc()
	{
		return doc;
	}

	public void setDoc(String doc)
	{
		this.doc = doc;
	}

	public String getNamespace()
	{
		return namespace;
	}

	public void setNamespace(String namespace)
	{
		this.namespace = namespace;
	}

	public String getMeta()
	{
		return meta;
	}

	public void setMeta(String meta)
	{
		this.meta = meta;
	}

	public List<Field> getFields()
	{
		return fields;
	}

	public void setFields(List<Field> fields)
	{
		this.fields = fields;
	}

}
