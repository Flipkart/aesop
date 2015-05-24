package com.flipkart.aesop.bootstrap.mysql;


import com.flipkart.aesop.event.AbstractEvent;

/* This is a wrapper class for Abstract Event and SCN. This is to track SCNs with each
 * generated event .
 */
public class MysqlEvent
{
    /* Associated SCN of the Transformed Source Event */
    private long scn;
    /* Abstract Event */
    private AbstractEvent abstractEvent;

    public MysqlEvent(long scn,AbstractEvent abstractEvent) {
        this.scn = scn;
        this.abstractEvent = abstractEvent;
    }

    public long getScn() {
        return scn;
    }

    public AbstractEvent getAbstractEvent() {
        return abstractEvent;
    }
}
