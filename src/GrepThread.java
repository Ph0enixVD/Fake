import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class GrepThread implements MigratableProcess {
  private TransactionalFileInputStream inFile;
  private TransactionalFileOutputStream outFile;
  private String query;
  
  private boolean suspending;
  
  public GrepThread() throws Exception {
    this.query = "first";
    this.inFile = new TransactionalFileInputStream("inputfile");
    this.outFile = new TransactionalFileOutputStream("outputfile");
  }
  
  public void run()
  {
    PrintStream out = new PrintStream(outFile);
    BufferedReader in = new BufferedReader(new InputStreamReader(inFile));
    
    try {
      while (!this.suspending) {        
        String line = in.readLine();
        if (line == null) break;        
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
  }
}