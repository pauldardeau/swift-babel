module StoragePolicy;

import BaseStoragePolicy;
import Config;
import ConfigParser;
import PolicyError;
import StoragePolicyCollection;


class StoragePolicy {

private:

public:

    /**
      Reload POLICIES from ``swift.conf``
    */
    void reload_storage_policies() {
        ConfigParser policy_conf = new ConfigParser();
        policy_conf.read(SWIFT_CONF_FILE);
        try {
            _POLICIES = this.parse_storage_policies(policy_conf);
        } catch (PolicyError e) {
            throw new SystemExit("ERROR: Invalid Storage Policy Configuration "
                         "in %s (%s)" % (SWIFT_CONF_FILE, e));
        }
    }

    StoragePolicyCollection parse_storage_policies(ConfigParser conf) {
        BaseStoragePolicy[] policies;
        for (section, conf.sections()) {
            if (!section.startswith("storage-policy:")) {
                continue;
            }
            policy_index = section.split(':', 1)[1];
            config_options = dict(conf.items(section));
            policy_type = config_options.pop("policy_type", DEFAULT_POLICY_TYPE);
            policy_cls = BaseStoragePolicy.policy_type_to_policy_cls[policy_type];
            policy = policy_cls.from_config(policy_index, config_options);
            policies.append(policy);
        }

        return new StoragePolicyCollection(policies);
    }
}

