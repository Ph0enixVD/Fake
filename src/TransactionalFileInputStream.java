import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;

public class TransactionalFileInputStream extends InputStream implements Serializable {
  private static final long serialVersionUID = 1L;
  String fileName;
  long offset;
  
  public TransactionalFileInputStream(String fileName) {
    this.fileName = fileName;
    this.offset = 0;
  }
  
  public int read() {
    try {
      RandomAccessFile file = new RandomAccessFile(this.fileName, "r");
      file.seek(this.offset);
      int data = file.read();
      this.offset++;
      file.close();
      return data;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return -1;
  }
}