import java.util.HashMap;


public class StatBuckets {

    private HashMap<Integer, Long> stats_buckets;
    private long over_counter;


    public StatBuckets() {
        stats_buckets = new HashMap<>();
        over_counter = 0;
    }

    public void increment(int counter_size) {
        Integer counter_size_obj = new Integer(counter_size);
        if (stats_buckets.containsKey(counter_size_obj)) {
            Long counter_value = stats_buckets.get(counter_size_obj);
            stats_buckets.put(counter_size, new Long(counter_value.longValue() + 1));
        } else {
            stats_buckets.put(counter_size_obj, new Long(1));
        }
    }

    public void increment_over() {
        ++over_counter;
    }

    public String toString() {
        //TODO: implement StatBuckets::toString
        return "";
    }
}


