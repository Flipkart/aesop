/*
 * WARNING : This is test code. It is a quick hack to try out features using third party libraries like
 * the LinkedIn Databus. 
 */
package org.aesop.bootstrap;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import org.aesop.events.example.person.Person;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.databus.container.netty.HttpRelay.Config;
import com.linkedin.databus.container.netty.HttpRelay.StaticConfig;
import com.linkedin.databus.core.util.Base64;
import com.linkedin.databus.core.util.ConfigLoader;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.FileSystemSchemaRegistryService;

/**
 * A sample client that consumes snapshots served by a {@link GenericBootstrapHttpServerMain} or similar Databus bootstrap server accessible from the endpoint : 
 * PersonBootstrapClientMain#BOOTSTRAP_SERVICE_URL. Note that the parameters passed for fetching snapshots is not externalized but rather hard-coded in this class.
 * Value of Parameter such as "bootstrap_target_scn" would need to be changed depending on values found in column 'windowscn' in the MySQL table 'bootstrap_producer_state'. 
 * 
 * Note: Include all jars in "lib" in the classpath before executing this class. 
 * 
 * <pre>
 * java -cp .:lib/*.jar org.aesop.bootstrap.PersonBootstrapClientMain
 * 
 * <pre>
 * 
 * @author Regunath B
 *
 */
public class PersonBootstrapClientMain extends Thread {

	static final String BOOTSTRAP_SERVICE_URL = "http://localhost:11111/bootstrap";

	private static final TypeReference<Map<String, Object>> JSON_GENERIC_MAP_TYPEREF = new TypeReference<Map<String, Object>>() {
	};
	private ObjectMapper objectMapper = new ObjectMapper();

	public void run() {

		Schema schema = null;

		File sourcesJson = new File("conf/sources-person.json");
		PhysicalSourceStaticConfig pStaticConfig = null;
		try {
			Config config = new Config();
			ConfigLoader<StaticConfig> staticConfigLoader = new ConfigLoader<StaticConfig>(
					"databus.bootstrap.", config);
			StaticConfig sConfig = staticConfigLoader
					.loadConfig(new Properties());
			pStaticConfig = objectMapper.readValue(sourcesJson,
					PhysicalSourceConfig.class).build();
			LogicalSourceStaticConfig sourceConfig = pStaticConfig.getSources()[0];
			FileSystemSchemaRegistryService schemaRegistryService = FileSystemSchemaRegistryService
					.build(sConfig.getSchemaRegistry().getFileSystem());
			schema = schemaRegistryService.fetchLatestVersionedSchemaByType(
					sourceConfig.getName()).getSchema();
		} catch (Exception e) {
			e.printStackTrace();
		}

		DefaultHttpClient client = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(BOOTSTRAP_SERVICE_URL);
		URIBuilder uriBuilder = new URIBuilder(httpget.getURI());
		uriBuilder
				.addParameter(
						"checkPoint",
						"{\"windowOffset\":-1,\"snapshot_offset\":-1,\"prevScn\":-1,\"windowScn\":0,\"bootstrap_target_scn\":900,\"catchup_source\":\"org.aesop.events.example.person.Person\",\"consumption_mode\":\"BOOTSTRAP_CATCHUP\"}");
		uriBuilder.addParameter("batchSize", "100000");
		uriBuilder.addParameter("output", "json");
		try {
			((HttpRequestBase) httpget).setURI(uriBuilder.build());
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		}

		BinaryDecoder binDecoder = null;

		try {
			HttpResponse response = client.execute(httpget);
			HttpEntity responseEntity = response.getEntity();
			byte[] responseData = EntityUtils.toByteArray(responseEntity);
			String[] lines = new String(responseData).split("\n");
			int count = 0;
			for (String line : lines) {
				if (count == lines.length - 1) {
					Map<String, Object> jsonObj = objectMapper.readValue(line,
							JSON_GENERIC_MAP_TYPEREF);
					byte[] valueBytes = Base64.decode((String) jsonObj
							.get("value"));
					System.out.println(new String(valueBytes));
				} else {
					Map<String, Object> jsonObj = objectMapper.readValue(line,
							JSON_GENERIC_MAP_TYPEREF);
					byte[] valueBytes = Base64.decode((String) jsonObj
							.get("value"));
					binDecoder = new DecoderFactory().binaryDecoder(valueBytes,
							binDecoder);
					ReflectDatumReader<Person> reader = new ReflectDatumReader<Person>(
							schema);
					Person person = new Person();
					person = reader.read(person, binDecoder);
					System.out.println(" key : "
							+ person.getKey() + " firstName: "
							+ person.getFirstName() + ", lastName: "
							+ person.getLastName() + ", birthDate: "
							+ person.getBirthDate() + ", deleted: "
							+ person.getDeleted());
				}
				count += 1;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		new PersonBootstrapClientMain().start();
	}

}
