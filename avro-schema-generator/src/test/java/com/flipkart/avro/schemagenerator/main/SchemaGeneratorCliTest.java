package com.flipkart.avro.schemagenerator.main;

import org.junit.Test;

/**
 * @author yogesh.dahiya
 */

public class SchemaGeneratorCliTest
{
	@Test
	public void testForSchemaGeneratorCli()
	{
		String[] commandLineArguments = "-d or_test -e test_table -i person".split("\\s+");
		SchemaGeneratorCli.main(commandLineArguments);
	}

}
