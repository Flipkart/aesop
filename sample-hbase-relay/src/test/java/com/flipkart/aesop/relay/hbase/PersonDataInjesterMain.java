/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.relay.hbase;

import com.flipkart.aesop.events.sample.person.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the NGDATA hbase-sep.
 *  
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
        for (long i=1000; i < 2000; i++) {
        	 byte[] rowkey = Bytes.toBytes(i);
             Put put = new Put(rowkey);
             put.add(infoCf, firstNameCq, Bytes.toBytes("Aesop " + i));
             put.add(infoCf, lastNameCq, Bytes.toBytes("Mr. " + i));
             put.add(infoCf, birthDateCq, Bytes.toBytes(i));
             htable.put(put);
             System.out.println(i + " Added row " + Bytes.toString(rowkey) + " " + "Aesop " + i);
        }
        // update rows
        for (long i=1200; i < 1500; i++) {
        	 byte[] rowkey = Bytes.toBytes(i);
             Put put = new Put(rowkey);
             put.add(infoCf, firstNameCq, Bytes.toBytes("Updated Aesop " + i));
             put.add(infoCf, lastNameCq, Bytes.toBytes("Updated Mr. " + i));
             put.add(infoCf, birthDateCq, Bytes.toBytes(i + 1));
             htable.put(put);
             System.out.println(i + " Updated row " + Bytes.toString(rowkey) + " " + "Updated Aesop " + i);
        }
        // delete rows
        for (long i=1500; i < 1700; i++) {
        	 byte[] rowkey = Bytes.toBytes(i);
             Delete delete = new Delete(rowkey);
             htable.delete(delete);
             System.out.println(i + " Deleted row " + Bytes.toString(rowkey) + " " + "Updated Aesop " + i);
        }
        htable.close();
        System.out.println("Done injesting");        
	}
}
