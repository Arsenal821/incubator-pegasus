#!/bin/bash

set -x

CWD=$(cd "$(dirname "$0")" && pwd)

print_help () {
    echo "USAGE: $0 TARGET_CONFIG_INI_PATH meta|replica TARGET_PID_FILE_PATH"
}


if [ $# -lt 3 ]; then
    print_help
    exit 1
fi

TARGET_CONFIG_INI_PATH=$1
ROLE_NAME=$2
TARGET_PID_FILE_PATH=$3

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

# el9 程序包内带的 tcmalloc 可能会引起问题，故包内没有该 lib ,采用环境的系统 lib 
if [[ -f "${LIB_DIR}/libtcmalloc_and_profiler.so.4" ]]; then
    export LD_PRELOAD="${LIB_DIR}/libtcmalloc_and_profiler.so.4"
else
    echo "${LIB_DIR}/libtcmalloc_and_profiler.so.4 not found! use system lib."
fi

if [[ ! -f "${BIN_DIR}/${SERVER_BIN}" ]]; then
    echo "${BIN_DIR}/${SERVER_BIN} not found"
    exit 1
fi

echo $BASHPID > $TARGET_PID_FILE_PATH && exec ${BIN_DIR}/${SERVER_BIN} ${TARGET_CONFIG_INI_PATH} -app_list ${ROLE_NAME}
