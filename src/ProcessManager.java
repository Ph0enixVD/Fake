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


/**
 * 
 * @author LI MINGYUAN, LI FANGSHI
 *
 */
public class ProcessManager {
	
	private static final int SUCCESS = 1;
	private static final int ERROR = -1;
	private static final int MASTER_PORT = 15619;
	private boolean isMaster;
	private Server master;
	private Server host;
	private LinkedList<Server> slaveList;
	private LinkedList<Thread> threadList;
	private HashMap<Server, LinkedList<Thread>> threadMap; //Todo max/min heap!

	public ProcessManager(boolean isMaster, Server master, Server host) {
		this.isMaster = isMaster;
		if(isMaster) {
			slaveList = new LinkedList<Server> ();
			threadMap = new HashMap<Server, LinkedList<Thread>>();
		}
		else {
			threadList = new LinkedList<Thread>();
			this.master = master;
			this.host = host;
		}
	}
	
	private int processMessage(Message msg, Socket socket) {
		
		MessageType type = msg.getType();
		if(type == MessageType.MsgNewSlaveRequest) {
			Server slave = (Server)msg.getObj();
			slaveList.add(slave);
			Message response = new Message( MessageType.MsgNewSlaveResponse ,new String("OK"));
			
			try {
				System.out.println("msg processed");
			    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			    out.writeObject(response);
			    out.flush();
			    System.out.println("response sent");
			    socket.getInputStream().close();
			    out.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else if(type == MessageType.MsgProcess) {
			MigratableProcess process = (MigratableProcess)msg.getObj();
			System.out.println("process recieved");
			Message response = new Message( MessageType.MsgNewSlaveResponse ,new String("OK"));
			
			try {
			    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
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
			System.out.println("thread go");
			thread.start();
		}
		return SUCCESS;
	}
	

	private int sendMessage(Server server, Message msg) {
		Socket socket;
		
		try {
            socket = new Socket( server.getIP(), server.getPort());
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(msg);
            out.flush();
            System.out.println("msg sent to" + server.getIP());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Message response = (Message)in.readObject();
            System.out.println("get response");
            in.close();
            out.close();
            if(response.getObj() instanceof String) {
            	if (((String)response.getObj()).equals("OK") ) {
            		return SUCCESS;
            	}
            }
            socket.close();
            
        } catch (Exception e) {
        	// to do
        	e.printStackTrace();
        }
		
		return ERROR;
	}	
	
	private class SocketListener extends Thread {
		
		private ServerSocket listener;
		
		public SocketListener(int port) {
			try {
			    listener = new ServerSocket(port);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		public void run(){
			while(true) {
				try {
					Socket socket = listener.accept();
					System.out.println("get connection");
					ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
					Message msg = (Message)in.readObject();
					System.out.println("get msg");
					//in.close();
					ProcessManager.this.processMessage(msg, socket); //to do : exception handle
				} catch(Exception e) {
					//to do
					e.printStackTrace();				}
			}
		}
	}
	
	private void MasterRun() {
		SocketListener listener = new SocketListener(MASTER_PORT);
	    listener.start();
	    
	    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
	    while(true) {
	    	String input = null;
	    	try {
                System.out.print(">>>");
				input = reader.readLine();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			if (input.equals("fake")) {
				debug();
			}
			else {
			
				if(input.equals("")) {
					continue;
				}
				
			    String[] process_args = input.split(" "); 
				MigratableProcess process = null;
				try {
					@SuppressWarnings("unchecked")
					Class<MigratableProcess> processClass = (Class<MigratableProcess>)(Class.forName(process_args[0]));
					Constructor<MigratableProcess> processConstructor = processClass.getConstructor();
	                //Object[] obj = new Object[1];
	                //obj[0] = (Object[])process_args;
					process = processConstructor.newInstance();
				} catch(Exception e) {
					e.printStackTrace();
				}
				
				//simple implementation
				Server server = slaveList.peek();
				Message msg = new Message(MessageType.MsgProcess, (Object)process);
				sendMessage(server, msg);
				
				//to do update master data structure
			} 
	    }
	    
	    //to do: stop() 
	}
	
	private void SlaveRun() {
		SocketListener listener = new SocketListener(host.getPort());
	    listener.start();
	    
	    Message msg = new Message(MessageType.MsgNewSlaveRequest, host);
	    if(sendMessage(master,msg) == SUCCESS) {
	    	System.out.println("CONNECT TO MASTER!");
	    }
	    
	    while(true) {
			//to do
	    	//sleep for a while -> check threadlist -> join the thread
	    	//send msg to master
	    }
	    
	}
	
	private void debug() {
		System.out.println("Running slaves:");
		for(Server ser: slaveList) {
			System.out.println("slave:" + ser.getIP()+ " " + ser.getPort());
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try {
		    System.out.println(InetAddress.getLocalHost().getHostAddress());
		} catch (Exception e) {}
		
		// TODO Auto-generated method stub
		if (args.length == 0) {
			//This is master
			System.out.println("This is master.");
			ProcessManager manager = new ProcessManager(true, null,null);
			manager.MasterRun();
		} else if(args.length == 2) {
			if (!args[0].equals("-c")) {
				System.out.println("invalid arg");
				return;
			}
			
			System.out.println("This is slave.");
			String[] master_addr = args[1].split(":");
			Server master = new Server(master_addr[0], Integer.parseInt(master_addr[1]));
			int slave_port = (int) (1000*Math.random()) + 10000;
			String slave_host = null;
			try {
			    slave_host = InetAddress.getLocalHost().getHostAddress();
			} catch(Exception e) {
			    e.printStackTrace();
			}
			Server slave = new Server(slave_host, slave_port);
			ProcessManager manager = new ProcessManager(false, master, slave);
			manager.SlaveRun();
		}
		
	}

}
