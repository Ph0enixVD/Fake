import java.io.Serializable;

public class Message implements Serializable{

	private static final long serialVersionUID = 1L;

	private MessageType type;
	
	private Object serializedObj;
	
	public Message(MessageType type, Object obj ) {
		this.type = type;
		this.serializedObj = obj;
	}
	
	public MessageType getType() {
		return type;
	}
	
	public Object getObj() {
		return serializedObj;
	}
	
}

