package logstash;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class LogstashSender {
    OutputStream logMessageStream = null;
    private static final Logger LOG = LoggerFactory.getLogger(LogstashSender.class);

    public LogstashSender(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        logMessageStream = socket.getOutputStream();
    }

    public void send(String logmsg) throws IOException {
        logMessageStream.write((logmsg + "\n").getBytes());
    }
}
