package com.flipkart.aesop.avro.schemagenerator.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

import com.flipkart.aesop.avro.schemagenerator.mysql.DataSourceConfig;

/**
 * <code>SchemaGeneratorCli</code> command line interface for the schema generator
 * @author yogesh.dahiya
 */

public class SchemaGeneratorCli
{
	public static void main(String[] commandLineArguments)
	{
		final CommandLineParser cmdLineGnuParser = new GnuParser();
		final Options gnuOptions = constructOptions();
		final String prefix="pz.";
		final String extension=".avsc";//extension of file in which generated schema will be stored
		CommandLine commandLine;
		
		FileWriter fileWriter=null;
		BufferedWriter bufferedWriter=null;
		try
		{
			commandLine = cmdLineGnuParser.parse(gnuOptions, commandLineArguments);
			if (commandLine.hasOption("help"))
			{
				printHelp(StringUtils.join(commandLineArguments, " "), gnuOptions, 80, "", "", 3, 5, true, System.out);
				return;
			}
			if (!commandLine.hasOption("d"))
			{
				throw new Exception("mandatory parameter db missing;");
			}
			
			if (!commandLine.hasOption("f"))
			{
				throw new Exception("mandatory parameter output-folder missing;");
			}
			
			if (!commandLine.hasOption("v"))
			{
				throw new Exception("mandatory parameter version missing;");
			}
			
			

			List<DataSourceConfig> dataSourceConfigs = new ArrayList<DataSourceConfig>();
			Map<String, List<String>> tablesInclusionListMap = new HashMap<String, List<String>>();
			Map<String, List<String>> tablesExclusionListMap = new HashMap<String, List<String>>();

			DataSourceConfig dataSourceConfig = new DataSourceConfig();
			dataSourceConfig.setDbName(commandLine.getOptionValue("d"));
			dataSourceConfig.setHostName(commandLine.hasOption("h") ? commandLine.getOptionValue("h") : "localhost");
			dataSourceConfig.setPort(commandLine.hasOption("o") ? commandLine.getOptionValue("o") : "3306");
			dataSourceConfig.setUserName(commandLine.hasOption("u") ? commandLine.getOptionValue("u") : "root");
			dataSourceConfig.setPassword(commandLine.hasOption("p") ? commandLine.getOptionValue("p") : "");

			dataSourceConfigs.add(dataSourceConfig);
			List<String> dbInclusionTableList =
			        commandLine.hasOption("i") ? Arrays.asList(commandLine.getOptionValues("i")) : null;
			List<String> dbExclusionTableList =
			        commandLine.hasOption("e") ? Arrays.asList(commandLine.getOptionValues("e")) : null;
			tablesInclusionListMap.put(commandLine.getOptionValue("d"), dbInclusionTableList);
			tablesExclusionListMap.put(commandLine.getOptionValue("d"), dbExclusionTableList);

			
			String outputFolder=commandLine.getOptionValue("f");
			//check if outputFolder exists, if not create one.
			File f = new File(outputFolder);
			if (!f.exists() || !f.isDirectory()) {
					f.mkdir();
				}
			
			
			
			SchemaGenerator schemaGenerator =
			        new SchemaGenerator(dataSourceConfigs, tablesInclusionListMap, tablesExclusionListMap);
			System.out.println("Generating Schema ...\n");
			if (commandLine.hasOption("t"))
			{
				String schema =
				        schemaGenerator
				                .generateSchema(commandLine.getOptionValue("d"), commandLine.getOptionValue("t"));
				fileWriter= new FileWriter(outputFolder+"/"+prefix+commandLine.getOptionValue("d")+"."+commandLine.getOptionValue("t") + "." + commandLine.getOptionValue("v") + extension);
				bufferedWriter=new BufferedWriter(fileWriter);
				bufferedWriter.write(schema);
				bufferedWriter.close();
			}
			else
			{
				Map<String, String> tableNameToSchema =
				        schemaGenerator.generateSchemaForAllTables(commandLine.getOptionValue("d"));
				for (String tableName : tableNameToSchema.keySet())
				{
					fileWriter= new FileWriter(outputFolder+"/"+prefix+commandLine.getOptionValue("d")+"."+tableName+ "." + commandLine.getOptionValue("v") + extension);
					bufferedWriter=new BufferedWriter(fileWriter);
					bufferedWriter.write(tableNameToSchema.get(tableName));
					bufferedWriter.close();
				}
			}
			System.out.println("**********************************************************************************\n");
			System.out.println("********* Written Schema to folder "+outputFolder+" ***********\n");
			System.out.println("**********************************************************************************\n");
		}
		catch (Exception e)
		{
			System.out.println(e.getMessage());
		}
		finally{
			if(bufferedWriter!=null){
				try{
				bufferedWriter.close();
				}catch(IOException ioe){
					ioe.printStackTrace();
				}
			}
		}

	}

	/**
	 * construct command line options
	 * @return options
	 */
	@SuppressWarnings("static-access")
	public static Options constructOptions()
	{
		final Options gnuOptions = new Options();
		gnuOptions
				.addOption("f", "output-folder", true, "path to the folder for storing output")
				.addOption("v", "version", true, "version number of schema")
				.addOption("h", "host", true, "host name for connection ; default localhost")
		        .addOption("o", "port", true, "port for connection ; default 3306")
		        .addOption("u", "user", true, "user name for connection ; default root")
		        .addOption("p", "password", true, "password for the connection ; default empty string")
		        .addOption("t", "table", true, "table name for schema generation ; default all ")
		        .addOption("e", "exclusion-list", true, "exclusion list ; default none")
		        .addOption("i", "inclusion-list", true, "inclusion list ; default all")
		        .addOption("?", "help", false, "help")
		        .addOption(
		                OptionBuilder.withArgName("dbName").withLongOpt("db").withDescription("db name for connection")
		                        .hasArg().create('d'))

		        .addOption(
		                OptionBuilder.withArgName("args").withLongOpt("exclusion-list")
		                        .withDescription("exclusion list ; default none").hasArgs().create('e'))
		        .addOption(
		                OptionBuilder.withArgName("args").withLongOpt("inclusion-list")
		                        .withDescription("inclusion list ; default none").hasArgs().create('i'));

		return gnuOptions;
	}

	/**
	 * Write "help" to the provided OutputStream.
	 * @param options
	 * @param printedRowWidth
	 * @param header
	 * @param footer
	 * @param spacesBeforeOption
	 * @param spacesBeforeOptionDescription
	 * @param displayUsage
	 * @param out
	 */
	public static void printHelp(String commandLineSyntax, final Options options, final int printedRowWidth,
	        final String header, final String footer, final int spacesBeforeOption,
	        final int spacesBeforeOptionDescription, final boolean displayUsage, final OutputStream out)
	{
		final PrintWriter writer = new PrintWriter(out);
		final HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.printHelp(writer, printedRowWidth, commandLineSyntax, header, options, spacesBeforeOption,
		        spacesBeforeOptionDescription, footer, displayUsage);
		writer.flush();
	}
}
