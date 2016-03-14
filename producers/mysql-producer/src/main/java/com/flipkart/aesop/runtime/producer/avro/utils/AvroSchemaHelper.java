package com.flipkart.aesop.runtime.producer.avro.utils;

import com.linkedin.databus2.schemas.utils.SchemaHelper;
import org.apache.avro.Schema;
import java.util.List;

/**
 * Created by akshit.agarwal on 14/03/16.
 */
public class AvroSchemaHelper extends SchemaHelper
{
    private static String rowChangeMetaField = "rowChangeField";

    public static String getRowChangeField(Schema schema)
    {
        List<Schema.Field> schemaFields = schema.getFields();
        String rowChangeField = null;
        for (Schema.Field field : schemaFields)
        {
            String fieldValue = SchemaHelper.getMetaField(field, rowChangeMetaField);
            if (fieldValue != null)
            {
                rowChangeField = field.name();
            }
        }
        return rowChangeField;
    }
}
