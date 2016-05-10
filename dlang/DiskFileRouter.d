module DiskFileRouter;


import BaseDiskFileManager;
import BaseStoragePolicy;
import PolicyError;


class DiskFileRouter
{

private:
    /* policy_type_to_manager_cls = {} */
    BaseDiskFileManager[string] policy_type_to_manager_cls;


public:
    static void register(cls, policy_type) {
        /*
        Decorator for Storage Policy implementations to register
        their DiskFile implementation.
        */
        void register_wrapper(diskfile_cls) {
            if (policy_type in cls.policy_type_to_manager_cls) {
                throw new PolicyError(
                    "%r is already registered for the policy_type %r" % (
                        cls.policy_type_to_manager_cls[policy_type],
                        policy_type));
            }
            cls.policy_type_to_manager_cls[policy_type] = diskfile_cls;
            return diskfile_cls;
        }
        return register_wrapper;
    }

    this(kwargs) {
        this.policy_to_manager = {};
        for (policy, POLICIES) {
            manager_cls = this.policy_type_to_manager_cls[policy.policy_type];
            this.policy_to_manager[policy] = manager_cls(kwargs);
        }
    }

    BaseDiskFileManager getitem(BaseStoragePolicy policy) {
        return this.policy_to_manager[policy];
    }
}

