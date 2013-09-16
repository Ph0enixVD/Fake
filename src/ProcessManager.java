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
 * @author LI FANGSHI, Mingyuan Li
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
  private MinMaxPriorityQueue<Server> masterServerList; // List of slave servers
  private int threadId = 0; // Thread Id for next new thread
  
  // Slave
  private ReentrantLock slaveLock;
  private LinkedList<Thread> slaveThreadList; // List of running threads
  private HashMap<Thread, Integer> slaveThreadIdMap;  // Mapping from Thread to thread ID
  private HashMap<Thread, MigratableProcess> slaveThreadMPMap;  // Mapping from Thread to MigratableProcess

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
       * Master get this msg from Slave, adding this server as Slave
       * msg.serializedObj is the Server object, which contains addr and port of the slave 
       */
      Server slave = (Server)msg.getObj();
      this.masterLock.lock();
      try {
        this.masterServerList.offer(slave);
      } finally {
        this.masterLock.unlock();
      }
      this.printWithPrompt("MsgNewSlaveRequest processed!");
      
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);
      if (this.sendProcessMessageResponse(response, socket) == false) {
        System.out.println("sendProcessMessageResponse failed!");
        return ProcessManager.ERROR;
      }
    } else if (type == MessageType.MsgProcessStart) {
      /* 
       * Slave get this msg from Master, starting the thread
       * msg.serializedObj is the MigratableProcess
       * msg.arg is the thread ID
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
        return ProcessManager.ERROR;
      }
    } else if (type == MessageType.MsgProcessFinish) {
      /*
       * Master get this msg from Slave, notifying that a thread has finished
       * msg.arg is the thread ID
       * Remove the thread ID from Master's server list
       */
      int tid = (int)msg.getArg();
      removeThreadFromSlave(tid);
      this.printWithPrompt("MsgProcessFinish processed!");
      
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);
      if (this.sendProcessMessageResponse(response, socket) == false) {
        this.printWithPrompt("sendProcessMessageResponse failed!");
        return ProcessManager.ERROR;
      }
    } else if (type == MessageType.MsgBalanceRequestSrc) {
      /*
       * Slave get this msg from Master, indicating that Slave has high load
       * Slave should suspend some threads and send back to Master
       * msg.serializedObj is the count of the threads to migrate from Slave
       * msg.arg is the count of threads that Master expect Slave is running
       * msg.arg is used for sync. If msg.arg number not the same as the count of threads Slave really has,
       * then we send back a response which aborts this round of migration
       * If msg.arg matches, in response:
       * msg.serializedObj contains the list of threads, and msg.arg contains list of thread IDs
       */ 
      int migrateThreadCnt = (int)msg.getObj();
      int expectedThreadCnt = (int)msg.getArg();
      
      System.out.println("migrateThreadCnt:" + migrateThreadCnt);
      System.out.println("expectedThreadCnt:" + expectedThreadCnt);
      
      Message response = null;
      this.slaveLock.lock();
      try {
        if (this.slaveThreadList.size() != expectedThreadCnt) {
          // Sync problem, msg.arg does not match the real count of threads in Slave
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
        return ProcessManager.ERROR;
      }      
    } else if (type == MessageType.MsgBalanceRequestDst) {      
      /* 
       * Slave get this msg from Master
       * msg.serializedobj is the list of threads, msg.arg is the list of thread IDs
       * Slave adds these threads, and starts running them
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
        return ProcessManager.ERROR;
      }      
    } else if (type == MessageType.MsgTerminate) {
      /* 
       * Slave get this msg from Master, terminating Slave
       */
      this.slaveLock.lock();
      try {
        for (Thread thread : this.slaveThreadList) {
          MigratableProcess mp = this.slaveThreadMPMap.get(thread);
          mp.suspend();
        }
      } finally {
        this.slaveLock.unlock();
      }
      
      Message response = new Message(MessageType.MsgResponseSuccess, null, null);
      if (this.sendProcessMessageResponse(response, socket) == false) {
        System.out.println("sendProcessMessageResponse failed!");
        return ProcessManager.ERROR;
      }
      System.exit(0);
    }
    return ProcessManager.SUCCESS;
  }
  
  // Send the ProcessMessage response message
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

  /*
   * General method used to send a message and return the response
   * The server argument refers to the destination, which contains IP and Port
   */
  private Message sendMessage(Server server, Message msg) {
    try {
      Socket socket = new Socket(server.getIP(), server.getPort());
      ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
      out.writeObject(msg);
      out.flush();
      ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
      Message response = (Message)in.readObject();
      out.close();
      in.close();      
      socket.close();
      return response;
    } catch (Exception e) {
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
           * Listen until a connection is established, and process the message
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

  // Timer to trigger load balance
  private class LoadBalanceTimer extends TimerTask {
    public void run() {
      ProcessManager.this.loadBalance();
    }    
  }

  private void loadBalance() {    
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

      if (diff < 1)
        break;

      Message requestSrc = new Message(MessageType.MsgBalanceRequestSrc, (Object)diff, (Object)max);
      Message responseSrc = sendMessage(src, requestSrc);
      if (responseSrc.getType() != MessageType.MsgBalanceResponse) {
        this.printWithPrompt("Load balance skip this round!");
        continue;
      }

      @SuppressWarnings("unchecked")
      LinkedList<Integer> tidList = (LinkedList<Integer>)responseSrc.getArg();
      migrateThreadSet(src, dst, tidList);
      Message requestDst = new Message(MessageType.MsgBalanceRequestDst, responseSrc.getObj(), responseSrc.getArg());

      if(sendMessage(dst, requestDst) == null) {
        this.printWithPrompt("Load balance error!");
      }
      
      round--;
    }
  }

  private void MasterRun() {
    SocketListener listener = new SocketListener(MASTER_PORT);
    listener.start();

    System.out.println("This is Master!");
    Timer timer = new Timer();
    timer.schedule(new LoadBalanceTimer(), 10000, 10000);

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      String input = null;
      MigratableProcess process = null;
      
      try {
        System.out.print(">>>");
        input = reader.readLine();

        if (input.equals("")) {
          continue;
        }
        else if (input.equals("ps")) {
          this.debug();
        }
        else if (input.equals("quit")) {
          this.quit();
        }
        
        String[] inputStrings = input.split(" ", 2);
        String[] threadArgs = null;
        if(inputStrings.length > 1) {
          // Thread has arguments
          threadArgs = inputStrings[1].split(" ");
        }
        
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
        System.out.println("Invalid input!");
        continue;
      }
      
      this.threadId++;
      Server server = addThreadToSlave(this.threadId);
      if (server == null) {
        System.out.println("No Slave found!");
        this.threadId--;
        continue;
      }
      Message msg = new Message(MessageType.MsgProcessStart, (Object)process, (Object)this.threadId);
      if(sendMessage(server, msg) == null) {
        System.out.println("Failed to Send MsgProcessStart Message!");
      }
    }
  }

  private void SlaveRun() {
    SocketListener listener = new SocketListener(this.localhost.getPort());
    listener.start();

    Message msg = new Message(MessageType.MsgNewSlaveRequest, this.localhost, null);
    if (sendMessage(this.master, msg).getType() == MessageType.MsgResponseSuccess) {
      System.out.println("Connected to Master!");
    } else {
      System.out.println("Failed to connect to Master!");
      return;
    }
    System.out.println("This is Slave!");
    
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
          sendMessage(this.master, finMsg);
        }
      } finally {
        this.slaveLock.unlock();
      }
    }
  }

  // Show debug information
  private void debug() {
    this.masterLock.lock();
    try {
      System.out.println("SlaveIP\t\tSlavePort\tThreadCount");
      System.out.println("-------------------------------------------");
      for (Server server : masterServerList) {
        System.out.println(server.getIP() + "\t" + server.getPort() + "\t\t" + server.getThreadSet().size());
        System.out.print("ThreadID: ");
        for (int i : server.getThreadSet()) {
          System.out.print(i + " ");
        }
        System.out.println("\n-------------------------------------------");
      }
    } finally {
      this.masterLock.unlock();
    }
  }
  
  // Quit ProcessManager
  private void quit() {
    this.masterLock.lock();
    try {
      for (Server server : this.masterServerList) {
        Message msg = new Message(MessageType.MsgTerminate, null, null);
        if (sendMessage(server, msg).getType() != MessageType.MsgResponseSuccess) {
          System.out.println("Terminate error!");
        }
      }
    } finally {
      this.masterLock.unlock();
    }
    System.exit(0);
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
        // Remove slave Server and add it back to keep MinMaxPriorityQueue working
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
      // Remove slave Server and add it back to keep MinMaxPriorityQueue working
      server = this.masterServerList.pollLast();
      if(server == null) {
        return null;
      }
      server.getThreadSet().add(tid);
      this.masterServerList.offer(server);
    } finally {
      this.masterLock.unlock();
    }
    return server;
  }
  
  // Remove a list of thread IDs from src Server, and add them to dst Server
  private void migrateThreadSet(Server src, Server dst, List<Integer> tidList) {
    if (src != null && dst != null) {
      this.masterLock.lock();
      try {
        // Remove slave Server and add it back to keep MinMaxPriorityQueue working
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
  
  // Print messages followed by prompt
  private void printWithPrompt(String msg) {
    System.out.println("\n" + msg);
    System.out.print(">>>");
  }
  
  private static void showUsage() {
    System.out.println("Invalid input!");
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

    if (args.length == 0) {
      // This is Master
      ProcessManager manager = new ProcessManager(true, null, null);
      manager.MasterRun();
    } else if (args.length == 2) {
      if (!args[0].equals("-c")) {
        // Invalid input
        ProcessManager.showUsage();
        System.exit(0);
      }
      
      try {
        //This is Slave
        String[] master_addr = args[1].split(":");
        Server master = new Server(master_addr[0],
            Integer.parseInt(master_addr[1]));
        int slave_port = (int) (1000 * Math.random()) + 10000;
        String slave_host = null;
        slave_host = InetAddress.getLocalHost().getHostAddress();
        Server slave = new Server(slave_host, slave_port);
        ProcessManager manager = new ProcessManager(false, master, slave);
        manager.SlaveRun();
      } catch (Exception e) {
        // Invalid input
        ProcessManager.showUsage();
        System.exit(0);
      }
    } else {
      // Invalid input
      ProcessManager.showUsage();
      System.exit(0);
    }
  }  
}
