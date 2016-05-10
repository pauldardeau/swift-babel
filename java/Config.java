import java.util.HashMap;


public class Config {

    private HashMap<String,String> dict;

    public Config() {
        dict = new HashMap<>();
    }

    public String get(String key) {
        return dict.get(key);
    }

    public String get(String key, String default_value) {
        if (dict.containsKey(key)) {
            return dict.get(key);
        } else {
            return default_value;
        }
    }
}

