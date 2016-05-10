import BaseStoragePolicy;


class StoragePolicySingleton {

private:

public:
    void iter() {
        return iter(_POLICIES);
    }

    int length() {
        return len(_POLICIES);
    }

    BaseStoragePolicy getitem(key) {
        return _POLICIES[key];
    }

    void getattribute(string name) {
        return getattr(_POLICIES, name);
    }

    override string toString() {
        return _POLICIES.toString();
    }

}

