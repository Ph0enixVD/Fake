public class TestThread implements MigratableProcess {

  private static final long serialVersionUID = 34122351L;

  private boolean suspended = false;
  private int count = 0;

  public TestThread() {
  }

  @Override
  public void run() {

    suspended = false;

    while (suspended == false) {
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
  }

  @Override
  public void suspend() {
    suspended = true;
  }
}