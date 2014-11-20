package loggly;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import loggly.*;
import logreader.LogStreamProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit.RestAdapter;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

public class LogglyRecordProcessor extends LogStreamProcessor {
    private final Logger LOG = LoggerFactory.getLogger(LogglyRecordProcessor.class);
    private String kinesisShardId;

    private String token = "not-a-token";

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    private LogglyService logglyService;

    LogglyCallback callback = new LogglyCallback();


    public LogglyRecordProcessor() {
        token = System.getProperty("logglyToken");

        RestAdapter restAdapter = new RestAdapter.Builder()
                .setEndpoint("http://logs-01.loggly.com")
                .build();

        logglyService = restAdapter.create(LogglyService.class);
    }


    private LogMessage msgFromData(String raw) {
        if(CallTimeMsg.isTimeMessage(raw)) {
            return new CallTimeMsg(raw);
        } else {
            return new RawMessage(raw);
        }
    }


    @Override
    protected void processRecordsWithRetries(List<Record> records) {
        for (Record record : records) {
            boolean processedSuccessfully = false;
            String data = null;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    // For this app, we interpret the payload as UTF-8 chars.
                    data = decoder.decode(record.getData()).toString();
                    //LOG.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + data);
                    LOG.info("seq no: -->" + record.getSequenceNumber() + ", chars: " + data.length());

                    LOG.info("Posting log data: " + data);

                    LogMessage msg = msgFromData(data);
                    LOG.info("message to post is " + msg);
                    logglyService.postLogData(token, msg,callback);
                    //
                    // Logic to process record goes here.
                    //
                    processedSuccessfully = true;
                    break;
                } catch (CharacterCodingException e) {
                    LOG.error("Malformed data: " + data, e);
                    break;
                } catch (Throwable t) {
                    LOG.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                LOG.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }
}
