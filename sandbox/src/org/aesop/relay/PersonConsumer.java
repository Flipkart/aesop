/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus. 
 */

package org.aesop.relay;

import org.aesop.events.example.person.Person;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;

import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;

/**
 * This consumer simply prints the attributes of the received {@link Person} object(s).
 * 
 * @author Regunath B
 *
 */
public class PersonConsumer extends AbstractDatabusCombinedConsumer {
	
	public static final Logger LOG = Logger.getLogger(PersonConsumer.class.getName());

	private int identifier;

	public PersonConsumer(int identifier) {
		this.identifier = identifier;
	}

	@Override
	public ConsumerCallbackResult onDataEvent(DbusEvent event,
			DbusEventDecoder eventDecoder) {
		return processEvent(event, eventDecoder);
	}

	@Override
	public ConsumerCallbackResult onBootstrapEvent(DbusEvent event,
			DbusEventDecoder eventDecoder) {
		return processEvent(event, eventDecoder);
	}

	private ConsumerCallbackResult processEvent(DbusEvent event, DbusEventDecoder eventDecoder) {
		GenericRecord decodedEvent = eventDecoder.getGenericRecord(event, null);
		try {
			Long key = (Long) decodedEvent.get("key");			
			Utf8 firstName = (Utf8) decodedEvent.get("firstName");
			Utf8 lastName = (Utf8) decodedEvent.get("lastName");
			Long birthDate = (Long) decodedEvent.get("birthDate");
			Utf8 deleted = (Utf8) decodedEvent.get("deleted");

			System.out.println("ID:" + this.identifier + " key : " + key + " firstName: "
					+ firstName.toString() + ", lastName: "
					+ lastName.toString() + ", birthDate: " + birthDate
					+ ", deleted: " + deleted.toString());
		} catch (Exception e) {
			//LOG.error("error decoding event ", e);
			//return ConsumerCallbackResult.ERROR;
			LOG.error("error processing event : " + decodedEvent);
		}

		return ConsumerCallbackResult.SUCCESS;
	}

}
