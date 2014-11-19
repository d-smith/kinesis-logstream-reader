package logstash;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.google.gson.Gson;
import loggly.CallTimeMsg;
import loggly.LogMessage;
import loggly.RawMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;


public class LogstashRecordProcessor implements IRecordProcessor {

    String logstashHost;
    int logstashPort;
    LogstashSender logstashSender;
    Gson gson = new Gson();

    public LogstashRecordProcessor() {
        logstashHost = System.getProperty("logstashHost");
        if(logstashHost == null) {
            throw new Error("No value defined for system property logstashHost");
        }

        String portSetting = System.getProperty("logstashPort");
        if(portSetting == null) {
            throw new Error("No value defined for system property logstashPort");
        }

        logstashPort = Integer.valueOf(portSetting);

        try {
            logstashSender = new LogstashSender(logstashHost, logstashPort);
        } catch(IOException io) {
            throw new Error(io);
        }
    }

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    private final Logger LOG = LoggerFactory.getLogger(LogstashRecordProcessor.class);
    private String kinesisShardId;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;

    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Processing " + records.size() + " records from " + kinesisShardId);

        // Process records and perform all exception handling.
        processRecordsWithRetries(records);

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason shutdownReason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownReason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }


    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
    }

    private LogMessage msgFromData(String raw) {
        if(CallTimeMsg.isTimeMessage(raw)) {
            return new CallTimeMsg(raw);
        } else {
            return new RawMessage(raw);
        }
    }


    private void processRecordsWithRetries(List<Record> records) {
        for (Record record : records) {
            boolean processedSuccessfully = false;
            String data = null;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    // For this app, we interpret the payload as UTF-8 chars.
                    data = decoder.decode(record.getData()).toString();


                    LogMessage msg = msgFromData(data);
                    String logString  = gson.toJson(msg);
                    LOG.info("Send to logstash: " + logString);

                    logstashSender.send(logString);

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
