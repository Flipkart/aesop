package com.flipkart.aesop.sample.client.transformer;

import com.flipkart.aesop.event.AbstractEvent;
import com.flipkart.aesop.transformer.PreMappingTransformer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author : arya.ketan
 * @version : 1.0
 * @date : 27/02/15
 */
public class SourceEventDateAppenderTransformer implements PreMappingTransformer
{
    public AbstractEvent transform(AbstractEvent event)
    {
        event.getFieldMapPair().put("source_event_date", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").
                    format(new Date()));

        return event;
    }
}
