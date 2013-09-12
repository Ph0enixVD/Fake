import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;

public class TransactionalFileOutputStream extends OutputStream implements Serializable {
  private static final long serialVersionUID = 1L;
  String fileName;
  long offset;
  
  public TransactionalFileOutputStream(String fileName) {
    this.fileName = fileName;
    this.offset = 0;
  }
  
  public void write(int data) {
    try {
      RandomAccessFile file = new RandomAccessFile(this.fileName, "rw");
      file.seek(this.offset);
      file.write(data);
      this.offset++;
      file.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}