package com.flipkart.aesop.runtime.producer.avro.utils;

import com.linkedin.databus2.schemas.utils.SchemaHelper;
import org.apache.avro.Schema;
import java.util.List;

/**
 * Created by akshit.agarwal on 14/03/16.
 */
public class AvroSchemaHelper
{
    private static String ROW_CHANGE_META_FIELD = "rowChangeField";

    public static String getRowChangeField(Schema schema)
    {
        return SchemaHelper.getMetaField(schema, ROW_CHANGE_META_FIELD);
    }
}
