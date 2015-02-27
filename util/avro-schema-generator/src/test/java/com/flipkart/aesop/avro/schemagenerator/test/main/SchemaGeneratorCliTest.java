package com.flipkart.aesop.avro.schemagenerator.test.main;

import com.flipkart.aesop.avro.schemagenerator.main.SchemaGeneratorCli;

/**
 * @author yogesh.dahiya
 */

public class SchemaGeneratorCliTest
{
	public void testForSchemaGeneratorCli()
	{
		//String[] commandLineArguments = "-d information_schema -f /Users/santosh.p/Desktop/test -v 0.1 -t INNODB_METRICS".split("\\s+");
		String[] commandLineArguments = "-d payment -f /Users/santosh.p/Desktop/avro-schema -v 0.1".split("\\s+");
		SchemaGeneratorCli.main(commandLineArguments);
	}

}
