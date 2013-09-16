public class TestThread implements MigratableProcess {
  private static final long serialVersionUID = 1L;
  private volatile boolean suspending;
  private int count = 0;

  public TestThread() {
  }

  @Override
  public void run() {
    System.out.println("TestThread started running!");
    while (!this.suspending) {
      try {
        Thread.sleep(2000);
        count++;
      } catch (Exception e) {
      }

      if (count > 10)
        break;
    }
    if (count > 10)
      System.out.println("TestThread finish success!");
    else
      System.out.println("TestThread suspended!");
    this.suspending = false;
  }

  @Override
  public void suspend() {
    suspending = true;
    while (suspending) {
      ;
    }
  }
}