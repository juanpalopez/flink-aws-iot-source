/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package iotCore;

import com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants;
import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer;

import iotCore.awsIotCore.AwsIotCoreSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */


public class StreamingJob {
	private static final String region = "eu-west-1";
	private static final String outputStreamName = "ExampleOutputStream";

	public static final class JsonConverter implements FlatMapFunction<String, JSONObject> {
		@Override
		public void flatMap(String value, Collector<JSONObject> out) {
			String[] jsonLines = value.split("\\r?\\n");

			for (String line: jsonLines){
				out.collect(new JSONObject(line));
			}
		}
	}

	public static final class BasicEventFlatter implements FlatMapFunction<JSONObject, JSONObject> {
		@Override
		public void flatMap(JSONObject value, Collector<JSONObject> out) {

			JSONArray measurements = value.getJSONArray("ListEvents");

			for(int i=0; i < measurements.length(); i++){
				JSONObject inner = measurements.getJSONObject(i);

				JSONObject basicEvent = new JSONObject();

				basicEvent.put("value", inner.get("Data"));
				basicEvent.put("id_tag", inner.get("Tag"));
				basicEvent.put("momentum", inner.get("Momentum"));
				basicEvent.put("status", inner.get("Status"));

				out.collect(basicEvent);
			}

		}
	}

	public static final class JsonToString implements MapFunction<JSONObject, String>{
		@Override
		public String map(JSONObject originEvent) {
			return originEvent.toString();
		}
	}

	private static FlinkKinesisFirehoseProducer<String> createFirehoseSinkFromStaticConfig() {
		/*
		 * com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants
		 * lists of all of the properties that firehose sink can be configured with.
		 */

		Properties outputProperties = new Properties();
		outputProperties.setProperty("aws.region", region);

		FlinkKinesisFirehoseProducer<String> sink = new FlinkKinesisFirehoseProducer<>(outputStreamName, new SimpleStringSchema(), outputProperties);
		ProducerConfigConstants config = new ProducerConfigConstants();
		return sink;
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties configProps = new Properties();
		String topicName = "<TOPIC_NAME>";
		String bucketName = "<BUCKET_NAME>";
		String certificateFile = "<IOT_CERTIFICATE_FILE>";
		String privateKey = "<IOT_PRIVATE_KEY>";
		String endpoint = "<IOT_ENDPOINT>";
		String clientId = "<CLIENT_ID>";

		configProps.setProperty("iot-source.clientEndpoint",endpoint);
		configProps.setProperty("iot-source.clientId",clientId);
		configProps.setProperty("iot-source.bucketName", bucketName);
		configProps.setProperty("iot-source.certificateFile", certificateFile);
		configProps.setProperty("iot-source.privateKey", privateKey);

		System.out.println(privateKey);
		DataStream<String> iot = env.addSource(new AwsIotCoreSource(topicName, configProps));

		DataStream<String> raw = iot
				.flatMap(new JsonConverter())
				.flatMap(new BasicEventFlatter())
				.map(new JsonToString());

		raw.addSink(createFirehoseSinkFromStaticConfig());

		env.execute("Iot Core");
	}
}
