import java.util.Hashtable;
import java.util.logging.Level;


public class Logger {

    private java.util.logging.Logger _logger;
    private Hashtable<String, Long> counters = new Hashtable<>();


    public void debug(String msg) {
        _logger.log(Level.FINE, msg);
    }

    public void info(String msg) {
        _logger.log(Level.INFO, msg);
    }

    public void warning(String msg) {
        _logger.log(Level.WARNING, msg);
    }

    public void error(String msg) {
        _logger.log(Level.SEVERE, msg);
    }

    public void exception(String msg) {
        _logger.log(Level.SEVERE, "exception: " + msg);
    }

    public void increment(String counter) {
        if (counters.containsKey(counter)) {
            Long prevValue = counters.get(counter);
            counters.put(counter, new Long(prevValue.longValue() + 1));
        } else {
            counters.put(counter, new Long(1));
        }
    }

    public long counter_value(String counter) {
        if (counters.containsKey(counter)) {
            return counters.get(counter).longValue();
        } else {
            return 0;
        }
    }
}

