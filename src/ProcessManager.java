import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.google.common.collect.MinMaxPriorityQueue;

/**
 * 
 * @author LI MINGYUAN, LI FANGSHI
 * 
 */
public class ProcessManager {

  private static final int SUCCESS = 1;
  // private static final int ERROR = -1;
  private static final int MASTER_PORT = 15619;
  private boolean isMaster;
  private Server master = null;
  private Server localhost = null;
  // master
  private MinMaxPriorityQueue<Server> list;
  // slave
  private LinkedList<Thread> threadList;
  private HashMap<Thread, Integer> threadMap;
  private HashMap<Thread, MigratableProcess> mpMap;

  private int threadID = 0;

  public ProcessManager(boolean isMaster, Server master, Server localhost) {
    this.isMaster = isMaster;
    if (this.isMaster) {
      list = MinMaxPriorityQueue.<Server> create();
    } else {
      this.master = master;
      this.localhost = localhost;
      threadList = new LinkedList<Thread>();
      threadMap = new HashMap<Thread, Integer>();
      mpMap = new HashMap<Thread, MigratableProcess>();
    }
  }

  @SuppressWarnings("unchecked")
  private int processMessage(Message msg, Socket socket) {

    // to do : close socket?

    MessageType type = msg.getType();
    if (type == MessageType.MsgNewSlaveRequest) {
      //master get from slave, add this server as a slave
      //msg obj = server obj, which contains addr and port of the slave
      Server slave = (Server) msg.getObj();
      list.offer(slave);
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);

      try {
        System.out.println("msg processed");
        ObjectOutputStream out = new ObjectOutputStream(
            socket.getOutputStream());
        out.writeObject(response);
        out.flush();
        System.out.println("response sent");
        socket.getInputStream().close();
        out.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else if (type == MessageType.MsgProcessStart) {
      //slave get the msg from master, start the thread
      //msg obj = process, msg arg = tid
      MigratableProcess process = (MigratableProcess) msg.getObj();
      int tID = (int) msg.getArg();
      System.out.println("process recieved");
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);

      try {
        ObjectOutputStream out = new ObjectOutputStream(
            socket.getOutputStream());
        out.writeObject(response);
        out.flush();
        System.out.println("response sent");
        socket.getInputStream().close();
        out.close();
      } catch (Exception e) {
        e.printStackTrace();
      }

      Thread thread = new Thread(process);
      threadList.add(thread);
      threadMap.put(thread, tID);
      mpMap.put(thread, process);
      System.out.println("thread " + tID + " lets go!");
      thread.start();

    } else if (type == MessageType.MsgProcessFinish) {
      //master get this msg from slave, indicate a process is finished
      //msg obj = tid, remove the tid from master's list
      int tid = (int) msg.getObj();
      System.out.println("process dying msg recieved");
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);

      try {
        ObjectOutputStream out = new ObjectOutputStream(
            socket.getOutputStream());
        out.writeObject(response);
        out.flush();
        System.out.println("response sent");
        socket.getInputStream().close();
        out.close();
      } catch (Exception e) {
        e.printStackTrace();
      }

      removeThreadFromSlave(tid);
    } else if (type == MessageType.MsgBalanceRequestSrc) {
      //slave get this msg from master, indicate this slave has high load which should 
      //suspend some thread and send back to master
      //msg obj contains the number of thread to migrate
      //msg arg contains the number of thread that master thought this slave is running
      //arg is used for sync. if arg number is not the same as how many thread the slave really has
      //then we send back a bad response which aborts this round of migration
      //if arg check passes, the response msg has obj=list of thread, and arg = list of tids 
      int round = (int) msg.getObj();
      int maxShould = (int) msg.getArg();
      Message response;
      if (threadList.size() != maxShould) {
        // syn problem. response "NI SHA BI"
        response = new Message(MessageType.MsgReponseError, null, null);
      } else {
        List<MigratableProcess> processList = new LinkedList<MigratableProcess>();
        List<Integer> idList = new LinkedList<Integer>();
        for (int i = 0; i < round; i++) {
          Thread thread = threadList.pollLast();
          if (thread != null) {
            MigratableProcess p = mpMap.get(thread);

            if (thread.isAlive()) {
              p.suspend();
              processList.add(p);
              idList.add(threadMap.get(thread));
            }
            threadList.remove(thread);
            threadMap.remove(thread);
            mpMap.remove(thread);
          }
        }
        System.out
            .println("READ TO MIGRATE " + processList.size() + " Process");
        if (processList.size() > 0) {
          response = new Message(MessageType.MsgBalanceResponse,
              (Object) processList, (Object) idList);
        } else {
          response = new Message(MessageType.MsgReponseError, null, null);
        }
      }
      try {
        ObjectOutputStream out = new ObjectOutputStream(
            socket.getOutputStream());
        out.writeObject(response);
        out.flush();
        System.out.println("response sent");
        socket.getInputStream().close();
        out.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else if (type == MessageType.MsgBalanceRequestDst) {
      
      //slave get this msg from master
      //msg obj = list of threads, arg = list of tids
      //slave add these threads, and runs them

      LinkedList<MigratableProcess> processList = (LinkedList<MigratableProcess>) msg
          .getObj();
      LinkedList<Integer> idList = (LinkedList<Integer>) msg.getArg();

      System.out.println("get msg to run " + processList.size() + " Process");

      while (processList.size() > 0) {
        MigratableProcess process = processList.pollFirst();
        int tID = idList.pollFirst();
        System.out.println("migrate tid ==" + tID);
        Thread thread = new Thread(process);
        threadList.add(thread);
        threadMap.put(thread, tID);
        mpMap.put(thread, process);
        System.out.println("thread " + tID + " lets start!!!!");
        thread.start();
      }
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);

      try {
        ObjectOutputStream out = new ObjectOutputStream(
            socket.getOutputStream());
        out.writeObject(response);
        out.flush();
        System.out.println("special response sent");
        socket.getInputStream().close();
        out.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return SUCCESS;
  }

  private Message sendMessage(Server server, Message msg) {
    Socket socket;

    // general method to send a message and return the response
    // arg server contains ip and port that this msg should be sent
    try {
      socket = new Socket(server.getIP(), server.getPort());
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      out.writeObject(msg);
      out.flush();
      System.out.println("msg sent to" + server.getIP());
      ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
      Message response = (Message) in.readObject();
      System.out.println("get response");
      in.close();
      out.close();
      socket.close();
      return response;

    } catch (Exception e) {
      // to do
      e.printStackTrace();
    }

    return null;
  }

  // listening thread
  private class SocketListener extends Thread {

    private ServerSocket listener;

    public SocketListener(int port) {
      try {
        listener = new ServerSocket(port);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void run() {
      while (true) {
        try {
          //listen until get a connection, and process the message
          Socket socket = listener.accept();
          System.out.println("get connection");
          ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
          Message msg = (Message) in.readObject();
          System.out.println("get msg");
          // in.close();
          ProcessManager.this.processMessage(msg, socket);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  //timer to trigger load balance
  private class LoadBalanceTimer extends TimerTask {

    public void run() {

      ProcessManager.this.loadBalance();
    }
  }

  private void loadBalance() {
    System.out.println("load balance start!");
    int round = list.size() / 2;

    // each round we balance load between server with the max and min load
    while (round > 0) {
      Server dst = list.peekFirst();
      Server src = list.peekLast();
      int min = dst.getThreadList().size();
      int max = src.getThreadList().size();
      int rd = (max - min) / 2;

      System.out.println("round" + round + "max & min is " + max + " " + min);

      if (rd < 1)
        break;

      Message requestSrc = new Message(MessageType.MsgBalanceRequestSrc,
          (Object) rd, (Object) max);

      Message responseSrc = sendMessage(src, requestSrc);

      if (responseSrc.getType() != MessageType.MsgBalanceResponse)
        continue;

      @SuppressWarnings("unchecked")
      List<Integer> tidList = (List<Integer>) responseSrc.getArg();

      removeThreadListFromSlave(src, tidList);

      // to do: msg first or add first?
      addThreadListToSlave(dst, tidList);

      Message requestDst = new Message(MessageType.MsgBalanceRequestDst,
          responseSrc.getObj(), responseSrc.getArg());

      // error handle
      sendMessage(dst, requestDst);

      round--;
    }
  }

  private void MasterRun() {
    SocketListener listener = new SocketListener(MASTER_PORT);
    listener.start();

    Timer timer = new Timer();
    timer.schedule(new LoadBalanceTimer(), 10000, 10000);

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      String input = null;
      try {
        System.out.print(">>>");
        input = reader.readLine();
      } catch (Exception e) {
        e.printStackTrace();
      }

      if (input.equals("fake")) {
        debug();
      } else {

        if (input.equals("")) {
          continue;
        }

        String[] process_args = input.split(" ");
        MigratableProcess process = null;
        // to do : false thread name is not handled!
        try {
          @SuppressWarnings("unchecked")
          Class<MigratableProcess> processClass = (Class<MigratableProcess>) (Class
              .forName(process_args[0]));
          Constructor<MigratableProcess> processConstructor = processClass
              .getConstructor();
          process = processConstructor.newInstance();
        } catch (Exception e) {
          e.printStackTrace();
        }

        threadID++;
        Server server = addThreadToSlave(threadID);
        Message msg = new Message(MessageType.MsgProcessStart,
            (Object) process, (Object) threadID);
        sendMessage(server, msg);
      }
    }

    // to do: stop()
  }

  private void SlaveRun() {
    SocketListener listener = new SocketListener(localhost.getPort());
    listener.start();

    Message msg = new Message(MessageType.MsgNewSlaveRequest, localhost, null);
    if (sendMessage(master, msg).getType() == MessageType.MsgResponseSuccess) {
      System.out.println("CONNECT TO MASTER!");
    }

    while (true) {
      try {
        Thread.sleep(30);

        // todo concurrency
        LinkedList<Thread> removeList = new LinkedList<Thread>();
        for (Thread thread : threadList) {
          if (!thread.isAlive()) {
            removeList.add(thread);
          }
        }

        for (Thread thread : removeList) {
          threadList.remove(thread);
          int tid = threadMap.get(thread);
          threadMap.remove(thread);
          mpMap.remove(thread);
          Message m = new Message(MessageType.MsgProcessFinish, (Object) tid,
              null);
          sendMessage(master, m);
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private void debug() {
    System.out.println("Running slaves:");
    for (Server ser : list) {
      System.out.println("slave:" + ser.getIP() + " " + ser.getPort() + " "
          + ser.getThreadList().size());
    }
  }

  private void removeThreadFromSlave(int tid) {
    Server targetSlave = null;
    for (Server slave : list) {
      if (slave.getThreadList().contains(tid)) {
        targetSlave = slave;
        break;
      }
    }
    if (targetSlave != null) {
      list.remove(targetSlave);
      targetSlave.getThreadList().remove(tid);
      list.offer(targetSlave);
    }
  }

  private Server addThreadToSlave(int tid) {
    Server server = list.pollLast();
    server.getThreadList().add(tid);
    list.offer(server);
    return server;
  }

  private void removeThreadListFromSlave(Server server, List<Integer> tidList) {
    Server targetSlave = null;
    for (Server slave : list) {
      if (slave.compareTo(server) == 0) {
        targetSlave = slave;
        break;
      }
    }
    if (targetSlave != null) {
      list.remove(targetSlave);
      targetSlave.getThreadList().removeAll(tidList);
      list.offer(targetSlave);
    }
  }

  private void addThreadListToSlave(Server server, List<Integer> tidList) {
    Server targetSlave = null;
    for (Server slave : list) {
      if (slave.compareTo(server) == 0) {
        targetSlave = slave;
        break;
      }
    }
    if (targetSlave != null) {
      list.remove(targetSlave);
      targetSlave.getThreadList().addAll(tidList);
      list.offer(targetSlave);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {

    try {
      System.out.println(InetAddress.getLocalHost().getHostAddress());
    } catch (Exception e) {
    }

    // TODO Auto-generated method stub
    if (args.length == 0) {
      // This is master
      System.out.println("This is master.");
      ProcessManager manager = new ProcessManager(true, null, null);
      manager.MasterRun();
    } else if (args.length == 2) {
      if (!args[0].equals("-c")) {
        System.out.println("invalid arg");
        return;
      }

      System.out.println("This is slave.");
      String[] master_addr = args[1].split(":");
      Server master = new Server(master_addr[0],
          Integer.parseInt(master_addr[1]));
      int slave_port = (int) (1000 * Math.random()) + 10000;
      String slave_host = null;
      try {
        slave_host = InetAddress.getLocalHost().getHostAddress();
      } catch (Exception e) {
        e.printStackTrace();
      }
      Server slave = new Server(slave_host, slave_port);
      ProcessManager manager = new ProcessManager(false, master, slave);
      manager.SlaveRun();
    }

  }

}
