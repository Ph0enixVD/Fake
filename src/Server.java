import java.io.Serializable;


public class Server implements Serializable{

	private static final long serialVersionUID = 1L;

	public Server( String ip, int port) {
		this.IP = ip;
		this.port = port;
	}
	
	private String IP;
	private int port;
	
	public String getIP() {
		return IP;
	}
	
	public int getPort() {
		return port;
	}

}
