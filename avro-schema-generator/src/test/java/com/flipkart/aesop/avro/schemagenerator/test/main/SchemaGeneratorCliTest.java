package com.flipkart.aesop.avro.schemagenerator.test.main;

import com.flipkart.aesop.avro.schemagenerator.main.SchemaGeneratorCli;

/**
 * @author yogesh.dahiya
 */

public class SchemaGeneratorCliTest
{
	public void testForSchemaGeneratorCli()
	{
		String[] commandLineArguments = "-d or_test -t test_table".split("\\s+");
		SchemaGeneratorCli.main(commandLineArguments);
	}

}
