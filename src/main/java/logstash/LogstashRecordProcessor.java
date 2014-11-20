package logstash;

import com.amazonaws.services.kinesis.model.Record;
import com.google.gson.Gson;
import logreader.LogStreamProcessor;
import messages.CallTimeMsg;
import messages.LogMessage;
import messages.RawMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;


public class LogstashRecordProcessor extends LogStreamProcessor {

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


    private final Logger LOG = LoggerFactory.getLogger(LogstashRecordProcessor.class);

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
