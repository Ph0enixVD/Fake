
public class TestThread implements MigratableProcess {
	
	private static final long serialVersionUID = 34122351L;

	public TestThread() {
	}

	@Override
	public void run() {
	   
		System.out.println("TestThread start to run.");
		try{
			for(int i = 0; i < 10; i++) {
				Thread.sleep(1000);
			}
		} catch(Exception e) {
			//to do
		}
		System.out.println("TestThread finish.");
	}
	
	
	@Override
	public void suspend() {	
	}
}