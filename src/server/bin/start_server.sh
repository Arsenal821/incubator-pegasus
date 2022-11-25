#!/bin/bash

set -x

CWD=$(cd "$(dirname "$0")" && pwd)

print_help () {
    echo "USAGE: $0 TARGET_CONFIG_INI_PATH meta|replica"
}


if [ $# -lt 2 ]; then
    print_help
    exit 1
fi

TARGET_CONFIG_INI_PATH=$1
ROLE_NAME=$2

BIN_DIR=${CWD}
MODULE_DIR=$(cd "$(dirname "${BIN_DIR}")" && pwd)

LIB_DIR=${MODULE_DIR}/lib
case ${ROLE_NAME} in
    meta)
        SERVER_BIN=meta_server
        ;;
    replica)
        SERVER_BIN=replica_server
        ;;
    *)
        echo "Invalid role name: ${ROLE_NAME}"
        exit 1
        ;;
esac

ARCH_TYPE=''
output=`arch`
if [ "$output"x == "x86_64"x ]; then
    ARCH_TYPE="amd64"
elif [ "$output"x == "aarch64"x ]; then
    ARCH_TYPE="aarch64"
else
    echo not support arch "$output"
fi

export LD_LIBRARY_PATH=${JAVA_HOME}/jre/lib/$ARCH_TYPE/server:${JAVA_HOME}/jre/lib/$ARCH_TYPE:${LIB_DIR}:${LD_LIBRARY_PATH}

if [[ -n "${USE_TCMALLOC_HEAP_PROFILE}" ]]; then
    export TCMALLOC_SAMPLE_PARAMETER=524288
    if [[ -z "${HEAPPROFILE}" ]]; then
        echo "Env HEAPPROFILE has not been defined"
        exit 1
    fi
fi

export LD_PRELOAD="${LIB_DIR}/libtcmalloc_and_profiler.so.4"

if [[ ! -f "${BIN_DIR}/${SERVER_BIN}" ]]; then
    echo "${BIN_DIR}/${SERVER_BIN} not found"
    exit 1
fi

${BIN_DIR}/${SERVER_BIN} ${TARGET_CONFIG_INI_PATH} -app_list ${ROLE_NAME}
