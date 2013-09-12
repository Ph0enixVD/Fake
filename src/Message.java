import java.io.Serializable;

public class Message implements Serializable {
  private static final long serialVersionUID = 1L;
  private MessageType type;
  private Object serializedObj;
  private Object arg;

  public Message(MessageType type, Object obj, Object arg) {
    this.type = type;
    this.serializedObj = obj;
    this.arg = arg;
  }

  public MessageType getType() {
    return type;
  }

  public Object getObj() {
    return serializedObj;
  }

  public Object getArg() {
    return arg;
  }
}
