import java.io.Serializable;
import java.util.HashSet;

public class Server implements Serializable, Comparable<Server> {

  private static final long serialVersionUID = 1L;

  private String IP;
  private int port;
  private HashSet<Integer> threadSet; // Set of thread IDs of slave server

  public Server(String ip, int port) {
    this.IP = ip;
    this.port = port;
    this.threadSet = new HashSet<Integer>();
  }
  
  public String getIP() {
    return this.IP;
  }

  public int getPort() {
    return this.port;
  }

  public HashSet<Integer> getThreadSet() {
    return this.threadSet;
  }

  @Override
  public int compareTo(Server server) {
    return this.threadSet.size() - server.getThreadSet().size();
  }

}
