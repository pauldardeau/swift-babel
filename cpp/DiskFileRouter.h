#ifndef DISKFILEROUTER_H
#define DISKFILEROUTER_H


#include "Config.h"
#include "Logger.h"


class DiskFileRouter {

private:
    Config config;
    Logger* logger;


public:
    DiskFileRouter(Config config, Logger* logger);

};

#endif

