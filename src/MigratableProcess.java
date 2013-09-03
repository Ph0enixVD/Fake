import java.io.Serializable;

/**
 * 
 * @author LI MINGYUAN, LI FANGSHI
 *
 */
public interface MigratableProcess extends Runnable, Serializable {
	
	void suspend();
	
}
