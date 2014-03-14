/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the NGDATA hbase-sep. 
 */
package com.flipkart.aesop.relay.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.flipkart.aesop.events.example.person.Person;

import java.io.IOException;

/**
 * Creates a HBase table to store records of the sample {@link Person} type
 * @author Regunath B
 * 
 */
public class SchemaSetupMain {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		createSchema(conf);
	}

	public static void createSchema(Configuration hbaseConf) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(hbaseConf);
		if (!admin.tableExists("aesop-person")) {
			HTableDescriptor tableDescriptor = new HTableDescriptor("aesop-person");
			HColumnDescriptor infoCf = new HColumnDescriptor("info");
			infoCf.setScope(1);
			tableDescriptor.addFamily(infoCf);
			admin.createTable(tableDescriptor);
		}
		admin.close();
	}
}
