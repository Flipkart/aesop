package com.flipkart.aesop.avro.schemagenerator.test.main;

import org.junit.Test;

import com.flipkart.aesop.avro.schemagenerator.main.SchemaGeneratorCli;

/**
 * @author yogesh.dahiya
 */

public class SchemaGeneratorCliTest
{
	@Test
	public void testForSchemaGeneratorCli()
	{
		String[] commandLineArguments = "-d or_test -t test_table".split("\\s+");
		SchemaGeneratorCli.main(commandLineArguments);
	}

}
