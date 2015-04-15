package com.flipkart.aesop.bootstrap.mysql;


import com.flipkart.aesop.event.AbstractEvent;

public class MysqlEvent
{
    private long scn;
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
