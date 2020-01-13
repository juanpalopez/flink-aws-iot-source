package iotCore.awsIotCore;

import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.AWSIotTopic;
import iotCore.awsIotCore.sampleUtil.SampleUtil;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AwsIotCoreSource extends RichSourceFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(AwsIotCoreSource.class);

    private final Properties configProps;
    private final String topic;
    private AWSIotMqttClient client;

    // ----- Required property keys

    public static final String CLIENT_ENDPOINT = "iot-source.clientEndpoint";

    public static final String CLIENT_ID = "iot-source.clientId";

    public static final String BUCKET_NAME = "iot-source.bucketName";

    public static final String CERTIFICATE_FILE = "iot-source.certificateFile";

    public static final String PRIVATE_KEY_FILE = "iot-source.privateKey";

    // ----- Runtime fields
    private transient boolean running = true;

    /**
     * Creates a new Flink IoT Core Consumer.
     *
     * <p>The AWS credentials to be used, AWS region of the IoT Core, initial position to start streaming
     * from are configured with a {@link Properties} instance.</p>
     *
     * @param topic
     *           The single AWS IoT Core topic to read from.
     * @param configProps
     *           The properties used to configure AWS credentials, AWS region, and initial starting position.
     */
    public AwsIotCoreSource(String topic, Properties configProps) {
        checkProperty(configProps, CLIENT_ENDPOINT);
        checkProperty(configProps, CLIENT_ID);
        checkProperty(configProps, BUCKET_NAME);
        checkProperty(configProps, CERTIFICATE_FILE);
        checkProperty(configProps, PRIVATE_KEY_FILE);

        this.topic = topic;
        this.configProps = configProps;
    }

    private static void checkProperty(Properties p, String key) {
        if (!p.containsKey(key)) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }
    // ------------------------------------------------------------------------
    //  Source life cycle
    // ------------------------------------------------------------------------


    class IotCoreTopic extends AWSIotTopic {
        SourceContext<String> ctx;
        public IotCoreTopic(String topic, AWSIotQos qos, SourceContext<String> ctx){
            super(topic, qos);
            this.ctx = ctx;
        }
        @Override
        public void onMessage(AWSIotMessage message){
                ctx.collect(message.getStringPayload());
        }
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        String clientEndpoint = configProps.getProperty(CLIENT_ENDPOINT);
        String clientId = configProps.getProperty(CLIENT_ID);
        String bucketName = configProps.getProperty(BUCKET_NAME);
        String certificateFile = configProps.getProperty(CERTIFICATE_FILE);
        String privateKeyFile = configProps.getProperty(PRIVATE_KEY_FILE);

        SampleUtil.KeyStorePasswordPair pair = SampleUtil.getKeyStorePasswordPair(bucketName, certificateFile, privateKeyFile);
        AWSIotMqttClient client = new AWSIotMqttClient(clientEndpoint, clientId, pair.keyStore, pair.keyPassword);

        AWSIotQos qos = AWSIotQos.QOS0;

        running = true;
        IotCoreTopic iotTopic = new IotCoreTopic(topic, qos, sourceContext);

        client.connect();
        client.subscribe(iotTopic);
        while (running){
            iotTopic.getStringPayload();
        }

    }

    @Override
    public void close() throws Exception{
        this.running = false;
        LOG.info("Closing AWS Iot Core source");
        //client.disconnect();
    }

    @Override
    public void cancel(){
        LOG.info("Cancelling AWS Iot Core source");
        try {
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }





}
