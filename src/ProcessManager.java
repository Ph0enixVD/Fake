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
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.MinMaxPriorityQueue;

/**
 * 
 * @author LI FANGSHI, mingyual
 * 
 */
public class ProcessManager {
  private static final int SUCCESS = 1;
  private static final int ERROR = -1;
  private static final int MASTER_PORT = 15619;
  private boolean isMaster;
  private Server master;
  private Server localhost;
  
  // Master
  private ReentrantLock masterLock;
  private MinMaxPriorityQueue<Server> masterServerList;
  private int threadId = 0;
  
  // Slave
  private ReentrantLock slaveLock;
  private LinkedList<Thread> slaveThreadList;
  private HashMap<Thread, Integer> slaveThreadIdMap;
  private HashMap<Thread, MigratableProcess> slaveThreadMPMap;

  public ProcessManager(boolean isMaster, Server master, Server localhost) {    
    this.isMaster = isMaster;
    if (this.isMaster) {
      this.masterServerList = MinMaxPriorityQueue.<Server>create();
      this.masterLock = new ReentrantLock();
    } else {
      this.master = master;
      this.localhost = localhost;
      this.slaveLock = new ReentrantLock();
      this.slaveThreadList = new LinkedList<Thread>();
      this.slaveThreadIdMap = new HashMap<Thread, Integer>();
      this.slaveThreadMPMap = new HashMap<Thread, MigratableProcess>();
    }    
  }

  @SuppressWarnings("unchecked")
  private int processMessage(Message msg, Socket socket) {    
    MessageType type = msg.getType();
    if (type == MessageType.MsgNewSlaveRequest) {
      /*
       * master get from slave, add this server as a slave
       * msg obj = server obj, which contains addr and port of the slave
       */
      Server slave = (Server)msg.getObj();
      this.masterLock.lock();
      try {
        this.masterServerList.offer(slave);
      } finally {
        this.masterLock.unlock();
      }
      //System.out.println("MsgNewSlaveRequest processed!");
      this.printWithPrompt("MsgNewSlaveRequest processed!");
      
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);
      if (this.sendProcessMessageResponse(response, socket) == false) {
        System.out.println("sendProcessMessageResponse failed!");
      }
    } else if (type == MessageType.MsgProcessStart) {
      /* 
       * slave get the msg from master, start the thread
       * msg obj = process, msg arg = tid
       */
      MigratableProcess process = (MigratableProcess)msg.getObj();
      int tID = (int)msg.getArg();
      Thread thread = new Thread(process);
      this.slaveLock.lock();
      try {
        this.slaveThreadList.add(thread);
        this.slaveThreadIdMap.put(thread, tID);
        this.slaveThreadMPMap.put(thread, process);
      } finally {
        this.slaveLock.unlock();
      }
      thread.start();
      System.out.println("Start thread " + tID + "!");
      System.out.println("MsgProcessStart processed!");
      
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);
      if (this.sendProcessMessageResponse(response, socket) == false) {
        System.out.println("sendProcessMessageResponse failed!");
      }
    } else if (type == MessageType.MsgProcessFinish) {
      /*
       * master get this msg from slave, indicate a process is finished
       * msg obj = tid, remove the tid from master's list
       */
      int tid = (int)msg.getArg();
      removeThreadFromSlave(tid);
      //System.out.println("MsgProcessFinish processed!");
      this.printWithPrompt("MsgProcessFinish processed!");
      
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);
      if (this.sendProcessMessageResponse(response, socket) == false) {
        //System.out.println("sendProcessMessageResponse failed!");
        this.printWithPrompt("sendProcessMessageResponse failed!");
      }
    } else if (type == MessageType.MsgBalanceRequestSrc) {
      /*
       * slave get this msg from master, indicate this slave has high load which should 
       * suspend some thread and send back to master
       * msg obj contains the number of thread to migrate
       * msg arg contains the number of thread that master thought this slave is running
       * arg is used for sync. if arg number is not the same as how many thread the slave really has
       * then we send back a bad response which aborts this round of migration
       * if arg check passes, the response msg has obj=list of thread, and arg = list of tids
       */ 
      int migrateThreadCnt = (int)msg.getObj();
      int expectedThreadCnt = (int)msg.getArg();
      
      System.out.println("migrateThreadCnt:" + migrateThreadCnt);
      System.out.println("expectedThreadCnt:" + expectedThreadCnt);
      
      Message response = null;
      this.slaveLock.lock();
      try {
        if (this.slaveThreadList.size() != expectedThreadCnt) {
          // syn problem. response "NI SHA BI"
          response = new Message(MessageType.MsgReponseError, null, null);
        } else {
          LinkedList<MigratableProcess> processList = new LinkedList<MigratableProcess>();
          LinkedList<Integer> idList = new LinkedList<Integer>();
          for (int i = 0; i < migrateThreadCnt; i++) {
            Thread thread = slaveThreadList.pollLast();
            if (thread != null) {
              if (thread.isAlive()) {
                MigratableProcess mp = this.slaveThreadMPMap.get(thread);
                mp.suspend();
                processList.add(mp);
                idList.add(this.slaveThreadIdMap.get(thread));
              }
              this.slaveThreadList.remove(thread);
              this.slaveThreadIdMap.remove(thread);
              this.slaveThreadMPMap.remove(thread);
            }
          }
          System.out.println("Src ready to migrate " + processList.size() + " Processes!");
          if (processList.size() > 0) {
            response = new Message(MessageType.MsgBalanceResponse, (Object) processList, (Object) idList);
          } else {
            response = new Message(MessageType.MsgReponseError, null, null);
          }
        }
      } finally {
        this.slaveLock.unlock();
      }
      System.out.println("MsgBalanceRequestSrc processed!");
      if (this.sendProcessMessageResponse(response, socket) == false) {
        System.out.println("sendProcessMessageResponse failed!");
      }      
    } else if (type == MessageType.MsgBalanceRequestDst) {      
      /* 
       * slave get this msg from master
       * msg obj = list of threads, arg = list of tids
       * slave add these threads, and runs them
       */
      LinkedList<MigratableProcess> processList = (LinkedList<MigratableProcess>)msg.getObj();
      LinkedList<Integer> idList = (LinkedList<Integer>) msg.getArg();
      System.out.println("Dst ready to run " + processList.size() + " Processes!");
      while (processList.size() > 0) {
        MigratableProcess process = processList.pollFirst();
        int tID = idList.pollFirst();
        Thread thread = new Thread(process);
        this.slaveLock.lock();
        try {
          this.slaveThreadList.add(thread);
          this.slaveThreadIdMap.put(thread, tID);
          this.slaveThreadMPMap.put(thread, process);
        } finally {
          this.slaveLock.unlock();
        }
        thread.start();
        System.out.println("Start to run Thread " + tID + "!");
      }
      System.out.println("MsgBalanceRequestDst processed!");
      
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);
      if (this.sendProcessMessageResponse(response, socket) == false) {
        System.out.println("sendProcessMessageResponse failed!");
      }      
    }
    return SUCCESS;
  }

  private Message sendMessage(Server server, Message msg) {
    /*
     * general method to send a message and return the response
     * arg server contains ip and port that this msg should be sent
     */
    try {
      Socket socket = new Socket(server.getIP(), server.getPort());
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      out.writeObject(msg);
      out.flush();
      //System.out.println("Send message to " + server.getIP());
      ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
      Message response = (Message)in.readObject();
      out.close();
      in.close();      
      socket.close();
      return response;
    } catch (Exception e) {
      // to do
      e.printStackTrace();
      return null;
    }
  }

  // Listening thread
  private class SocketListener extends Thread {    
    private ServerSocket listener;

    public SocketListener(int port) {      
      try {
        this.listener = new ServerSocket(port);
      } catch (Exception e) {
        e.printStackTrace();
      }      
    }

    public void run() {
      while (true) {
        try {
          /* 
           * listen until get a connection, and process the message
           */
        	Socket socket = this.listener.accept();
        	ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
          Message msg = (Message)in.readObject();
        	if(processMessage(msg, socket) != ProcessManager.SUCCESS) {
        		System.out.println("Process message failed!");
        	}
        	in.close();
        	socket.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  //Timer to trigger load balance
  private class LoadBalanceTimer extends TimerTask {
    public void run() {
      ProcessManager.this.loadBalance();
    }    
  }

  private void loadBalance() {    
    //System.out.println("Load balance start!");
    this.printWithPrompt("Load balance start!");
    int round = 0;
    
    // The round of load balance we do is 1/2 * NumberOfSlaves
    this.masterLock.lock();
    try {
      round = masterServerList.size() / 2;
    } finally {
      this.masterLock.unlock();
    }

    // Each round we balance load between server with the max and min load
    while (round > 0) {
      Server dst = null;
      Server src = null;
      int min = 0;
      int max = 0;
      int diff = 0;
      this.masterLock.lock();
      try {
        dst = this.masterServerList.peekFirst();
        src = this.masterServerList.peekLast();
        min = dst.getThreadSet().size();
        max = src.getThreadSet().size();
        diff = (max - min) / 2;
      } finally {
        this.masterLock.unlock();
      }

      //System.out.println("Round" + round + " max & min is " + max + " " + min);
      this.printWithPrompt("Round" + round + " max & min is " + max + " " + min);
      if (diff < 1)
        break;

      Message requestSrc = new Message(MessageType.MsgBalanceRequestSrc, (Object)diff, (Object)max);
      Message responseSrc = sendMessage(src, requestSrc);
      if (responseSrc.getType() != MessageType.MsgBalanceResponse) {
        //System.out.println("Load balance skip this round!");
        this.printWithPrompt("Load balance skip this round!");
        continue;
      }

      @SuppressWarnings("unchecked")
      LinkedList<Integer> tidList = (LinkedList<Integer>)responseSrc.getArg();
      migrateThreadSet(src, dst, tidList);
      Message requestDst = new Message(MessageType.MsgBalanceRequestDst, responseSrc.getObj(), responseSrc.getArg());

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
        //this.printWithPrompt("");
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
        String[] inputStrings = input.split(" ", 2);
        String[] threadArgs = null;
        if(inputStrings.length > 1) {
          // Thread has arguments
          threadArgs = inputStrings[1].split(" ");
        }
        
        MigratableProcess process = null;
        try {
          @SuppressWarnings("unchecked")
          Class<MigratableProcess> processClass = (Class<MigratableProcess>)(Class.forName(inputStrings[0]));
          Constructor<?>[] processConstructor = processClass.getConstructors();
          if(inputStrings.length == 1) {
            // Thread has no argument
            process = (MigratableProcess)processConstructor[0].newInstance();
          } else {
            // Thread has arguments
            process = (MigratableProcess)processConstructor[0].newInstance(new Object[]{threadArgs});
          }
        } catch (Exception e) {
          System.out.println("False thread name!");
          continue;
        }

        this.threadId++;
        Server server = addThreadToSlave(this.threadId);
        Message msg = new Message(MessageType.MsgProcessStart, (Object)process, (Object)this.threadId);
        sendMessage(server, msg);
      }
    }

    // to do: stop()
  }

  private void SlaveRun() {
    SocketListener listener = new SocketListener(this.localhost.getPort());
    listener.start();

    Message msg = new Message(MessageType.MsgNewSlaveRequest, this.localhost, null);
    if (sendMessage(this.master, msg).getType() == MessageType.MsgResponseSuccess) {
      System.out.println("Connected to Master!");
    }

    while (true) {
      try {
        Thread.sleep(30);
      } catch (Exception e) {
        e.printStackTrace();
        continue;
      }
      LinkedList<Thread> removeList = new LinkedList<Thread>();
      this.slaveLock.lock();
      try {
        for (Thread thread : this.slaveThreadList) {
          if (!thread.isAlive()) {
            removeList.add(thread);
          }
        }
        for (Thread thread : removeList) {
          int tid = this.slaveThreadIdMap.get(thread);
          this.slaveThreadList.remove(thread);
          this.slaveThreadIdMap.remove(thread);
          this.slaveThreadMPMap.remove(thread);
          Message finMsg = new Message(MessageType.MsgProcessFinish, null, (Object) tid);
          sendMessage(master, finMsg);
        }
      } finally {
        this.slaveLock.unlock();
      }
    }
  }

  private void debug() {
    System.out.println("Running slaves:");
    this.masterLock.lock();
    try {
      for (Server ser : masterServerList) {
        System.out.println("slave:" + ser.getIP() + " " + ser.getPort() + " " + ser.getThreadSet().size());
      }
    } finally {
      this.masterLock.unlock();
    }
  }

  private void removeThreadFromSlave(int tid) {
    Server targetSlave = null;
    this.masterLock.lock();
    try {
      for (Server slave : this.masterServerList) {
        if (slave.getThreadSet().contains(tid)) {
          targetSlave = slave;
          break;
        }
      }
      if (targetSlave != null) {
        this.masterServerList.remove(targetSlave);
        targetSlave.getThreadSet().remove(tid);
        this.masterServerList.offer(targetSlave);
      }
    } finally {
      this.masterLock.unlock();
    }
  }

  private Server addThreadToSlave(int tid) {
    Server server = null;
    this.masterLock.lock();
    try {
      server = this.masterServerList.pollLast();
      server.getThreadSet().add(tid);
      this.masterServerList.offer(server);
    } finally {
      this.masterLock.unlock();
    }
    return server;
  }
  
  private void migrateThreadSet(Server src, Server dst, List<Integer> tidList) {
    if (src != null && dst != null) {
      this.masterLock.lock();
      try {
        this.masterServerList.remove(src);
        this.masterServerList.remove(dst);
        src.getThreadSet().removeAll(tidList);
        dst.getThreadSet().addAll(tidList);
        this.masterServerList.offer(src);
        this.masterServerList.offer(dst);
      } finally {
        this.masterLock.unlock();
      }
    }
  }
  
  private boolean sendProcessMessageResponse(Message msg, Socket socket) {
    try {
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      out.writeObject(msg);
      out.flush();
      out.close();
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }
  
  private void printWithPrompt(String msg) {
    System.out.println("\n" + msg);
    System.out.print(">>>");
  }
  
  private static void showUsage() {
    System.out.println("Usage:");
    System.out.println("\tMaster: ProcessManager");
    System.out.println("\tSlave: ProcessManager -c <MasterIP>:<MasterPort>");
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      System.out.println(InetAddress.getLocalHost().getHostAddress());
    } catch (Exception e) {
      e.printStackTrace();
    }

    // TODO Auto-generated method stub
    if (args.length == 0) {
      // This is master
      System.out.println("This is Master!");
      ProcessManager manager = new ProcessManager(true, null, null);
      manager.MasterRun();
    } else if (args.length == 2) {
      if (!args[0].equals("-c")) {
        ProcessManager.showUsage();
        return;
      }
      
      System.out.println("This is Slave!");
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
    } else {
      ProcessManager.showUsage();
    }
  }  
}
