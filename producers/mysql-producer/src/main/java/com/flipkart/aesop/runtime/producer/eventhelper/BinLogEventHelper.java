package com.flipkart.aesop.runtime.producer.eventhelper;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Row;

import java.util.List;

/**
 * Created by akshit.agarwal on 11/03/16.
 */
public class BinLogEventHelper
{

    public static void appendColumnToRow(Row actualRow, Column columnToAppend)
    {
        List<Column> oldColValues = actualRow.getColumns();
        oldColValues.add(columnToAppend);
        actualRow.setColumns(oldColValues);
    }

}
