/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the NGDATA hbase-sep. 
 */
package org.aesop.relay.hbase;

import org.aesop.events.example.person.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Executes insert/update/delete commands for {@link Person} data objects on the HBase table by name "aesop-person"
 * 
 * @author Regunath B
 *
 */
public class PersonDataInjesterMain {

	public static void main(String[] args) throws Exception {
        new PersonDataInjesterMain().run();
    }
	
	public void run() throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
        final byte[] infoCf = Bytes.toBytes("info");

        // column qualifiers
        final byte[] firstNameCq = Bytes.toBytes("firstName");
        final byte[] lastNameCq = Bytes.toBytes("lastName");
        final byte[] birthDateCq = Bytes.toBytes("birthDate");
		
        HTable htable = new HTable(conf, "aesop-person");
        htable.setAutoFlush(true);
        // insert rows
        for (long i=0; i < 1000; i++) {
        	 byte[] rowkey = Bytes.toBytes(i);
             Put put = new Put(rowkey);
             put.add(infoCf, firstNameCq, Bytes.toBytes("Aesop " + i));
             put.add(infoCf, lastNameCq, Bytes.toBytes("Mr. " + i));
             put.add(infoCf, birthDateCq, Bytes.toBytes(i));
             htable.put(put);
             System.out.println(i + " Added row " + Bytes.toString(rowkey) + " " + "Aesop " + i);
        }
        // update rows
        for (long i=200; i < 500; i++) {
        	 byte[] rowkey = Bytes.toBytes(i);
             Put put = new Put(rowkey);
             put.add(infoCf, firstNameCq, Bytes.toBytes("Updated Aesop " + i));
             put.add(infoCf, lastNameCq, Bytes.toBytes("Updated Mr. " + i));
             put.add(infoCf, birthDateCq, Bytes.toBytes(i + 1));
             htable.put(put);
             System.out.println(i + " Updated row " + Bytes.toString(rowkey) + " " + "Updated Aesop " + i);
        }
        // delete rows
        for (long i=500; i < 700; i++) {
        	 byte[] rowkey = Bytes.toBytes(i);
             Delete delete = new Delete(rowkey);
             htable.delete(delete);
             System.out.println(i + " Deleted row " + Bytes.toString(rowkey) + " " + "Updated Aesop " + i);
        }
        htable.close();
        System.out.println("Done injesting");        
	}
}
