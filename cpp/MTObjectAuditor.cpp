#include <string>
#include <vector>
#include <algorithm>

#include "MTObjectAuditor.h"
#include "OSUtils.h"
#include "SwiftUtils.h"

using namespace std;


int MTObjectAuditor::run_thread(AuditorOptions& options) {
}

void MTObjectAuditor::audit_loop(bool parent,
                                 int zbo_fps,
                                 AuditorOptions& options) {

    //Parallel audit loop
    this->clear_recon_cache("ALL");
    this->clear_recon_cache("ZBF");
    options.device_dirs = options.override_devices;
    if (parent) {
        options.zero_byte_fps = zbo_fps;
        this->run_audit(options);
    } else {
        vector<int> tids;
        int zbf_tid;
        if (this->conf_zero_byte_fps) {
            options.zero_byte_fps = true;
            zbf_tid = this->run_thread(options);
            tids.push_back(zbf_tid);
        }

        if (this->concurrency == 1) {
            // Audit all devices in 1 process
            tids.push_back(this->run_thread(options));
        } else {
            // Divide devices amongst parallel processes set by
            // self.concurrency.  Total number of parallel processes
            // is self.concurrency + 1 if zero_byte_fps.
            int parallel_proc;
            vector<string> device_list;

            if (this->conf_zero_byte_fps) {
                parallel_proc = this->concurrency + 1;
            } else {
                parallel_proc = this->concurrency;
            }

            if (options.override_devices.length() > 0) {
                device_list = list(options.override_devices);
            } else {
                device_list = SwiftUtils::listdir(this->devices);
            }

            std::random_shuffle(device_list.begin(),
                                device_list.end());

            while (!device_list.empty()) {
                int tid = -1;
                if (tids.size() == parallel_proc) {
                    tid = OSUtils::wait();
                    ObjectAuditor::erase_id(tids, tid);
                }
                // ZBF scanner must be restarted as soon as it finishes
                if (this->conf_zero_byte_fps && tid == zbf_tid) {
                    options.device_dirs = options.override_devices;
                    // sleep between ZBF scanner thread executions
                    this->_sleep();
                    options.zero_byte_fps = true;
                    zbf_tid = this->run_thread(options);
                    tids.push_back(zbf_tid);
                } else {
                    vector<string> threaded_device_dirs;
                    threaded_device_dirs.push_back(device_list.back());
                    device_list.pop_back();
                    options.device_dirs = threaded_device_dirs;
                    tids.push_back(this->run_thread(options));
                }
            }
        }

        while (!tids.empty()) {
            int tid = OSUtils::wait();
            // ZBF scanner must be restarted as soon as it finishes
            if (this->conf_zero_byte_fps && tid == zbf_tid &&
                tids.size() > 1) {
                options.device_dirs = options.override_devices;
                // sleep between ZBF scanner thread executions
                this->_sleep();
                options.zero_byte_fps = true;
                tids.push_back(this->run_thread(options));
            }
            ObjectAuditor::erase_id(tids, tid);
        }
    }
}

