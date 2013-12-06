/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus. 
 */

package org.aesop.relay;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.aesop.events.example.person.Person;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

/**
 * A custom Databus {@link EventProducer} that creates a fixed number of change events of type {@link Person} using in-memory data.
 * The events are created in a separate thread and appended to the Databus event buffer instance. Uses code as-is, modified or in parts from the Databus sample 
 * or main codebase.
 * 
 * @author Regunath B
 *
 */
public class PersonEventProducer implements EventProducer {

	private AtomicLong sinceSCN = new AtomicLong(-1);
	private DbusEventBufferAppendable eventBuffer;
	private MaxSCNReaderWriter maxScnReaderWriter;
	private DbusEventsStatisticsCollector dbusEventsStatisticsCollector;
	private String schema;

	public PersonEventProducer(DbusEventBufferAppendable eventBuffer,
			MaxSCNReaderWriter maxScnReaderWriter,
			DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
			SchemaRegistryService schemaRegistryService,
			PhysicalSourceStaticConfig physicalSourceConfig) {
		this.eventBuffer = eventBuffer;
		this.maxScnReaderWriter = maxScnReaderWriter;
		this.dbusEventsStatisticsCollector = dbusEventsStatisticsCollector;
		LogicalSourceStaticConfig sourceConfig = physicalSourceConfig
				.getSources()[0];
		try {
			schema = schemaRegistryService.fetchLatestSchemaByType(sourceConfig
					.getName());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getName() {
		return "PersonEventProducer";
	}

	public long getSCN() {
		return sinceSCN.get();
	}

	@Override
	public void start(long sinceSCN) {
		this.sinceSCN.set(sinceSCN);
		EventProducerThread thread = new EventProducerThread(eventBuffer,
				maxScnReaderWriter, dbusEventsStatisticsCollector);
		thread.start();

	}

	@Override
	public boolean isRunning() {
		return true;
	}

	@Override
	public boolean isPaused() {
		return false;
	}

	@Override
	public void unpause() {
	}

	@Override
	public void pause() {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public void waitForShutdown() throws InterruptedException,
			IllegalStateException {
	}

	@Override
	public void waitForShutdown(long timeout) throws InterruptedException,
			IllegalStateException {
	}

	private class EventProducerThread extends Thread {
		private DbusEventBufferAppendable eventBuffer;
		private MaxSCNReaderWriter maxScnReaderWriter;

		private EncoderFactory factory = EncoderFactory.get();
		private BinaryEncoder cachedAvroEncoder;
		private DbusEventsStatisticsCollector dbusEventsStatisticsCollector;

		EventProducerThread(DbusEventBufferAppendable eventBuffer,
				MaxSCNReaderWriter maxScnReaderWriter,
				DbusEventsStatisticsCollector dbusEventsStatisticsCollector) {
			this.eventBuffer = eventBuffer;
			this.maxScnReaderWriter = maxScnReaderWriter;
			this.dbusEventsStatisticsCollector = dbusEventsStatisticsCollector;
		}

		public void run() {
			eventBuffer.startEvents();
			byte[] schemaId = SchemaHelper.getSchemaId(schema);
			int count = 100;
			for (long i = sinceSCN.longValue(); i < (sinceSCN.longValue() + count); i++) {
				Person person = new Person(i, "Aesop " + i, "Mr. " + i, i, "false");
				byte[] serializedEvent = serializeEvent(person);
				DbusEventKey eventKey = new DbusEventKey(i);
				eventBuffer.appendEvent(eventKey, (short) 1, (short) 1,
						System.currentTimeMillis(), (short) 101, schemaId,
						serializedEvent, false, dbusEventsStatisticsCollector);
				System.out.println("Added an event : " + "Aesop Mr. " + i);
			}
			eventBuffer.endEvents(sinceSCN.longValue() + count,
					dbusEventsStatisticsCollector);
			try {
				maxScnReaderWriter.saveMaxScn(sinceSCN.longValue() + count);
			} catch (DatabusException e) {
				e.printStackTrace();
			}
		}

		private byte[] serializeEvent(GenericRecord record) {
			// Serialize the row
			byte[] serializedValue;
			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				Encoder encoder = factory.directBinaryEncoder(bos,
						cachedAvroEncoder);
				GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
						record.getSchema());
				writer.write(record, encoder);
				serializedValue = bos.toByteArray();
				return serializedValue;
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			return null;
		}

	}

}
