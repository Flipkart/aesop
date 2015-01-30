package com.flipkart.aesop.bootstrap;

import com.flipkart.aesop.event.AbstractEvent;

/**
 * Created by nikhil.bafna on 1/25/15.
 */
public interface SourceEventConsumer
{
	public void onEvent(AbstractEvent event);

	public void shutdown() throws InterruptedException;
}
