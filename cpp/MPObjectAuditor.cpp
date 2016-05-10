#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <algorithm>

#include "MPObjectAuditor.h"
#include "OSUtils.h"
#include "SwiftUtils.h"

using namespace std;


int MPObjectAuditor::fork_child(AuditorOptions& options, bool zero_byte_fps) {
    //Child execution
    int pid = OSUtils::fork();
    if (pid) {
        return pid;
    } else {
        signal(SIGTERM, SIG_DFL);

        if (zero_byte_fps) {
            options.zero_byte_fps = this->conf_zero_byte_fps;
        }

        try {
            this->run_audit(options);
        } catch (const std::exception& e) {
            this->logger->exception(
                string("ERROR: Unable to run auditing: ") + e.what());
        }

        exit(0);
    }
}

void MPObjectAuditor::audit_loop(bool parent,
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
        vector<int> pids;
        int zbf_pid;
        if (this->conf_zero_byte_fps) {
            options.zero_byte_fps = true;
            zbf_pid = this->fork_child(options);
            pids.push_back(zbf_pid);
        }

        if (this->concurrency == 1) {
            // Audit all devices in 1 process
            pids.push_back(this->fork_child(options));
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
                int pid = -1;
                if (pids.size() == parallel_proc) {
                    pid = OSUtils::wait();
                    ObjectAuditor::erase_id(pids, pid);
                }
                // ZBF scanner must be restarted as soon as it finishes
                if (this->conf_zero_byte_fps && pid == zbf_pid) {
                    options.device_dirs = options.override_devices;
                    // sleep between ZBF scanner forks
                    this->_sleep();
                    options.zero_byte_fps = true;
                    zbf_pid = this->fork_child(options);
                    pids.push_back(zbf_pid);
                } else {
                    vector<string> forked_device_dirs;
                    forked_device_dirs.push_back(device_list.back());
                    device_list.pop_back();
                    options.device_dirs = forked_device_dirs;
                    pids.push_back(this->fork_child(options));
                }
            }
        }

        while (!pids.empty()) {
            int pid = OSUtils::wait();
            // ZBF scanner must be restarted as soon as it finishes
            if (this->conf_zero_byte_fps && pid == zbf_pid &&
                pids.size() > 1) {
                options.device_dirs = options.override_devices;
                // sleep between ZBF scanner forks
                this->_sleep();
                options.zero_byte_fps = true;
                pids.push_back(this->fork_child(options));
            }
            ObjectAuditor::erase_id(pids, pid);
        }
    }
}

