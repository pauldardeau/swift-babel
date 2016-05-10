#!/bin/sh
NIM_CC="nim"
CC_OPTS="c"

#${NIM_CC} ${CC_OPTS} AuditLocation.nim
#${NIM_CC} ${CC_OPTS} AuditorWorker.nim
#${NIM_CC} ${CC_OPTS} BaseDiskFile.nim
#${NIM_CC} ${CC_OPTS} BaseDiskFileManager.nim
#${NIM_CC} ${CC_OPTS} BaseDiskFileReader.nim
#${NIM_CC} ${CC_OPTS} BaseStoragePolicy.nim
#${NIM_CC} ${CC_OPTS} Config.nim
#${NIM_CC} ${CC_OPTS} Daemon.nim
#${NIM_CC} ${CC_OPTS} Logger.nim
#${NIM_CC} ${CC_OPTS} LogOptions.nim
#${NIM_CC} ${CC_OPTS} ObjectAuditor.nim
#${NIM_CC} ${CC_OPTS} PolicyError.nim
#${NIM_CC} ${CC_OPTS} utils.nim
${NIM_CC} ${CC_OPTS} StatBuckets.nim
${NIM_CC} ${CC_OPTS} StorageDevice.nim
#${NIM_CC} ${CC_OPTS} StoragePolicy.nim
${NIM_CC} ${CC_OPTS} StoragePolicyCollection.nim
${NIM_CC} ${CC_OPTS} SwiftUtils.nim
