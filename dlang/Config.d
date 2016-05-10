module Config;


class Config {

private:
    string[string] dict;

public:
    this() {
    }

    string get(string key) {
        return dict.get(key);
    }

    string get(string key, string default_value) {
        if (dict.containsKey(key)) {
            return dict[key];
        } else {
            return default_value;
        }
    }
}

