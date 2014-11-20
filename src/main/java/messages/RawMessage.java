package messages;


public class RawMessage extends LogMessage {
    String message;
    public RawMessage(String message) {
        this.message = message;
    }

    public String getMessage() { return message; }
}
