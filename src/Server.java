import java.io.Serializable;
import java.util.HashSet;

public class Server implements Serializable, Comparable<Server> {

  private static final long serialVersionUID = 1L;

  private HashSet<Integer> threadList;

  public Server(String ip, int port) {
    this.IP = ip;
    this.port = port;
    threadList = new HashSet<Integer>();
  }

  private String IP;
  private int port;

  public String getIP() {
    return IP;
  }

  public int getPort() {
    return port;
  }

  public HashSet<Integer> getThreadList() {
    return threadList;
  }

  @Override
  public int compareTo(Server ser) {
    return this.threadList.size() - ser.getThreadList().size();
  }

}
