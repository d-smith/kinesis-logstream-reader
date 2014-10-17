package loggly;


public class CallTimeMsg extends LogMessage {
    //Time lines look like TIME - 12 millis - f5b37628-1e7e-406b-80ae-1365ca658cf0 - AddNote

    public static boolean isTimeMessage(String raw) {
        return raw.contains("TIME - ");
    }

    String txn;
    String id;
    String svcTime;

    public CallTimeMsg(String raw) {
        String[] parts = raw.split(" - ");
        txn = parts[3];
        id = parts[2];

        String rawTime = parts[1];
        svcTime = rawTime.split(" ")[0];
    }

    public String getTxn() {
        return txn;
    }

    public String getId() {
        return id;
    }

    public String getSvcTime() {
        return svcTime;
    }
}
