/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus and NGDATA hbase-sep. 
 */
package com.flipkart.aesop.relay.hbase;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.flipkart.aesop.events.example.person.Person;
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
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;

/**
 * An {@link EventProducer} that listens to HBase WAL edits using the hbase-sep module library classes such as {@link SepConsumer} and {@link EventListener} and in 
 * turn creates change events of the sample {@link Person}
 * 
 * @author Regunath B
 *
 */
public class WALEditPersonEventProducer implements EventProducer {

	private AtomicLong sinceSCN = new AtomicLong(-1);
	private static String schema;
	
	private static EncoderFactory factory = EncoderFactory.get();
	private static BinaryEncoder cachedAvroEncoder;
	
	private SepConsumer sepConsumer;
	
	public WALEditPersonEventProducer(DbusEventBufferAppendable eventBuffer,
			MaxSCNReaderWriter maxScnReaderWriter,
			DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
			SchemaRegistryService schemaRegistryService,
			PhysicalSourceStaticConfig physicalSourceConfig) {		
		try {			
			LogicalSourceStaticConfig sourceConfig = physicalSourceConfig.getSources()[0];
			schema = schemaRegistryService.fetchLatestSchemaBySourceName(sourceConfig.getName());
			
	        Configuration conf = HBaseConfiguration.create();
	        conf.setBoolean("hbase.replication", true);
	
	        ZooKeeperItf zk = ZkUtil.connect("localhost", 20000);
	        SepModel sepModel = new SepModelImpl(zk, conf);
	
	        final String subscriptionName = "relayAppender";
	
	        if (!sepModel.hasSubscription(subscriptionName)) {
	            sepModel.addSubscriptionSilent(subscriptionName);
	        }
	
	        sepConsumer = new SepConsumer(subscriptionName, 0, new RelayAppender(sinceSCN, eventBuffer, maxScnReaderWriter, dbusEventsStatisticsCollector), 
	        		1, "localhost", zk, conf);		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
    private static class RelayAppender implements EventListener {
    	
    	private AtomicLong sinceSCN = new AtomicLong(-1);    	
    	private DbusEventBufferAppendable eventBuffer;
    	private MaxSCNReaderWriter maxScnReaderWriter;
    	private DbusEventsStatisticsCollector dbusEventsStatisticsCollector;
    	
    	public RelayAppender (AtomicLong sinceSCN, DbusEventBufferAppendable eventBuffer, 
        		MaxSCNReaderWriter maxScnReaderWriter, DbusEventsStatisticsCollector dbusEventsStatisticsCollector) {
    		this.sinceSCN = sinceSCN;
			this.eventBuffer = eventBuffer;
			this.maxScnReaderWriter = maxScnReaderWriter;
			this.dbusEventsStatisticsCollector = dbusEventsStatisticsCollector;    		
    	}
    	
        @Override
        public void processEvents(List<SepEvent> sepEvents) {
			eventBuffer.startEvents();
			byte[] schemaId = SchemaHelper.getSchemaId(schema);
        	
            for (SepEvent sepEvent : sepEvents) {
            	String firstName = null;
            	String lastName = null;
            	long dob = 0;
            	String deleted = "false";
            	Person person = null;
                for (KeyValue kv : sepEvent.getKeyValues()) {
                	if (kv.isDeleteFamily()) {
                		person = new Person(Bytes.toLong(sepEvent.getRow()), "","",0L,"true",null);
                	} else {
						String columnQualifier = new String(kv.getQualifier());	
						if (columnQualifier.equalsIgnoreCase("firstName")) {
							firstName = Bytes.toString(kv.getValue());
						} else if (columnQualifier.equalsIgnoreCase("lastName")) {
							lastName = Bytes.toString(kv.getValue());						
						} else if (columnQualifier.equalsIgnoreCase("birthDate")) {
							dob = Bytes.toLong(kv.getValue());
						}
                	}
                }
                if (person == null) {
                	person = new Person(Bytes.toLong(sepEvent.getRow()),firstName, lastName, dob, deleted,null);
                }
				byte[] serializedEvent = serializeEvent(person);
				DbusEventKey eventKey = new DbusEventKey(Bytes.toLong(sepEvent.getRow()));
				eventBuffer.appendEvent(eventKey, (short) 1, (short) 1,
						System.currentTimeMillis(), (short) 101, schemaId,
						serializedEvent, false, dbusEventsStatisticsCollector);
				System.out.println("Added an event : " + person.getKey() + " " + person.getFirstName() + " " + person.getLastName() + " " + person.getDeleted());
				
                sinceSCN.getAndIncrement();
            }
			eventBuffer.endEvents(sinceSCN.longValue() , dbusEventsStatisticsCollector);
			try {
				maxScnReaderWriter.saveMaxScn(sinceSCN.longValue());
			} catch (DatabusException e) {
				e.printStackTrace();
			}            
        }
    }
    
	private static byte[] serializeEvent(GenericRecord record) {
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

	@Override
	public String getName() {
		return "WALEditPersonEventProducer";
	}

	@Override
	public long getSCN() {
		return sinceSCN.get();
	}

	@Override
	public void start(long sinceSCN) {
		this.sinceSCN.set(sinceSCN);
		try {
			sepConsumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean isPaused() {
		return false;
	}

	@Override
	public boolean isRunning() {
		return true;
	}

	@Override
	public void pause() {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public void unpause() {
	}

	@Override
	public void waitForShutdown() throws InterruptedException,
			IllegalStateException {
	}

	@Override
	public void waitForShutdown(long arg0) throws InterruptedException,
			IllegalStateException {
	}

}
