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
  
  public GrepThread(String args[]) throws Exception {
    if (args.length != 3) {
      System.out.println("usage: GrepProcess <queryString> <inputFile> <outputFile>");
      throw new Exception("Invalid Arguments");
    }
    this.query = args[0];
    this.inFile = new TransactionalFileInputStream(args[1]);
    this.outFile = new TransactionalFileOutputStream(args[2]);
  }
  
  public void run()
  {
    System.out.println("GrepThread started running!");
    PrintStream out = new PrintStream(outFile);
    DataInputStream in = new DataInputStream(inFile);
    
    try {
      while (!this.suspending) {
        String line = in.readLine();
        if (line == null) {
          break;
        }
        if (line.contains(query)) {
          out.println(line);
        }
        
        // Make grep take longer so that we don't require extremely large files for interesting results
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          // Ignore it
        }
      }
    } catch (EOFException e) {
      //End of File
    } catch (IOException e) {
      System.out.println ("GrepProcess: Error: " + e);
    }
    this.suspending = false;
  }
  
  public void suspend() {
    this.suspending = true;
    while (suspending) {
      ;
    }
  }
}