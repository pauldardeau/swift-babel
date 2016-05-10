
#include "ObjectAuditor.h"
#include "ConfigParser.h"


int main(int argc, char* argv[]) {
    bool once = true;
    ConfigParser config;
    ObjectAuditor auditor(config);
    if (once) {
        auditor.run_once();
    } else {
        auditor.run_forever();
    }
}

