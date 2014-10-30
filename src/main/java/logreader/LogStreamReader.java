package logreader;


import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

public class LogStreamReader {
    private static final String DEFAULT_APP_NAME = "LogReaderApp2";
    private static final String DEFAULT_STREAM_NAME = "loggingStream2";

    private static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream DEFAULT_INITIAL_POSITION = InitialPositionInStream.TRIM_HORIZON;

    private static String applicationName = DEFAULT_APP_NAME;
    private static String streamName = DEFAULT_STREAM_NAME;
    private static String kinesisEndpoint = DEFAULT_KINESIS_ENDPOINT;
    private static InitialPositionInStream initialPositionInStream = DEFAULT_INITIAL_POSITION;

    private static KinesisClientLibConfiguration kinesisClientLibConfiguration;

    private static final Logger LOG = LoggerFactory.getLogger(LogStreamReader.class);

    public static void main(String[] args) throws IOException {
        configure();

        LOG.info("Running " + applicationName + " to process stream " + streamName);

        IRecordProcessorFactory recordProcessorFactory = new LogReaderRecordProcessorFactory();
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        int exitCode = 0;
        try {
            worker.run();
        } catch(Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }

        System.exit(exitCode);

    }

    private static ClientConfiguration configureClient() {
        final ClientConfiguration commonConfig = new ClientConfiguration();
        final String proxyHost = System.getProperty("http.proxyHost");
        if(proxyHost != null) {
            commonConfig.setProxyHost(proxyHost);
        }

        final String proxyPort = System.getProperty("http.proxyPort");
        if(proxyPort != null) {
            commonConfig.setProxyPort(Integer.parseInt(proxyPort));
        }
        return commonConfig;
    }

    private static void configure() throws IOException {

        // ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl" , "60");

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        LOG.info("Using workerId: " + workerId);

        // Get credentials from IMDS. If unsuccessful, get them from the credential profiles file.
        AWSCredentialsProvider credentialsProvider = new SystemPropertiesCredentialsProvider();
        credentialsProvider.getCredentials();


        LOG.info("Using credentials with access key id: " + credentialsProvider.getCredentials().getAWSAccessKeyId());

        ClientConfiguration commonConfig = configureClient();

        kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(applicationName, streamName,
                    credentialsProvider, workerId)
                        .withCommonClientConfig(commonConfig)
                        .withInitialPositionInStream(initialPositionInStream);
    }
}
