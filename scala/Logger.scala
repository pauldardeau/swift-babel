import java.util.Hashtable;
import java.util.logging.Level;


class Logger {

    private java.util.logging.Logger _logger;
    private Hashtable<String, Long> counters = new Hashtable<>();


    def debug(msg: String) {
        _logger.log(Level.FINE, msg);
    }

    def info(msg: String) {
        _logger.log(Level.INFO, msg);
    }

    def warning(msg: String) {
        _logger.log(Level.WARNING, msg);
    }

    def error(msg: String) {
        _logger.log(Level.SEVERE, msg);
    }

    def exception(msg: String) {
        _logger.log(Level.SEVERE, "exception: " + msg);
    }

    def increment(counter: String) {
        if (counters.containsKey(counter)) {
            Long prevValue = counters.get(counter);
            counters.put(counter, new Long(prevValue.longValue() + 1));
        } else {
            counters.put(counter, new Long(1));
        }
    }

    def counter_value(counter: String) : Long = {
        if (counters.containsKey(counter)) {
            return counters.get(counter).longValue();
        } else {
            return 0;
        }
    }
}

