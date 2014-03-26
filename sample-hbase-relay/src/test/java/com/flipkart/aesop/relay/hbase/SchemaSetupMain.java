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

import java.io.IOException;

import com.flipkart.aesop.events.sample.person.Person;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the NGDATA hbase-sep.
 *  
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
