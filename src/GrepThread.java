import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class GrepThread implements MigratableProcess {
  private TransactionalFileInputStream inFile;
  private TransactionalFileOutputStream outFile;
  private String query;
  
  private volatile boolean suspending;
  
  public GrepThread() throws Exception {
    this.query = "first";
    this.inFile = new TransactionalFileInputStream("inputfile");
    this.outFile = new TransactionalFileOutputStream("outputfile");
  }
  
  public void run()
  {
    System.out.println("GrepThread started running!");
    PrintStream out = new PrintStream(outFile);
    DataInputStream in = new DataInputStream(inFile);
    
    try {
      while (!this.suspending) {
        //System.out.println("running, inFile offset:" + inFile.offset);
        String line = in.readLine();
        if (line == null) {
          //System.out.println("Line is null!");
          break;
        }
        System.out.println("Line is:" + line);
        if (line.contains(query)) {
          //System.out.println("running, outFile offset:" + outFile.offset);
          out.println(line);
        }
        
        // Make grep take longer so that we don't require extremely large files for interesting results
        try {
          System.out.println("ready to sleep 5000!");
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          System.out.println("sleep interrupted!");
          // Ignore it
        }
      }
    } catch (EOFException e) {
      //End of File
    } catch (IOException e) {
      System.out.println ("GrepProcess: Error: " + e);
    }
    this.suspending = false;
    //System.out.println("Thread run over!");
  }
  
  public void suspend() {
    this.suspending = true;
    //System.out.println("Suspend inFile offset:" + inFile.offset);
    //System.out.println("Suspend outFile offset:" + outFile.offset);
    while (suspending) {
      ;
    }
  }
}