import java.util.HashMap;


class Config {

    private HashMap<String,String> dict;

    def Config() {
        dict = new HashMap<>();
    }

    def get(key: String) : String = {
        return dict.get(key);
    }

    def get(key: String, default_value: String) : String = {
        if (dict.containsKey(key)) {
            return dict.get(key);
        } else {
            return default_value;
        }
    }
}

