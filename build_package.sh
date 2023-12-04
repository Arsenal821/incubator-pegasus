#!/bin/bash

set -e
set -x

tag=$(date +'%Y-%m-%d %H:%M:%S')

function print_help() {
    echo "USAGE: $0 build release|debug"
}

function build() {
    local package_type=$1

    ./run.sh pack_server
    ./run.sh pack_tools

    local source_server_dir=$(ls | grep "pegasus-server-.*-${package_type}$")
    local source_tools_dir=$(ls | grep "pegasus-tools-.*-${package_type}$")

    local target_server_dir=PACK_OUT/skv_server
    local target_tools_dir=${target_server_dir}/tools
    mkdir -p ${target_server_dir}/{bin,conf,lib,tools,hadoop}
    
    cp -a ${source_server_dir}/bin/meta_server ${target_server_dir}/bin/
    cp -a ${source_server_dir}/bin/replica_server ${target_server_dir}/bin/
    cp -a ${source_server_dir}/bin/start_server.sh ${target_server_dir}/bin/

    cp -a ${source_server_dir}/lib ${target_server_dir}/

    cp -a ${source_server_dir}/META_SERVER_VERSION ${target_server_dir}/
    cp -a ${source_server_dir}/REPLICA_SERVER_VERSION ${target_server_dir}/

    cp -r ${source_server_dir}/hadoop ${target_server_dir}/

    mkdir -p ${target_tools_dir}/DSN_ROOT/bin/pegasus_shell ${target_tools_dir}/src/shell
    cp -r ${source_tools_dir}/DSN_ROOT/bin/pegasus_shell/pegasus_shell ${target_tools_dir}/DSN_ROOT/bin/pegasus_shell/
    cp -r ${source_tools_dir}/src/shell/config.ini ${target_tools_dir}/src/shell
    cp -r ${source_tools_dir}/{scripts,run.sh} ${target_tools_dir}

    if test $package_type == "release"; then
        strip ${target_server_dir}/bin/meta_server
        strip ${target_server_dir}/bin/replica_server
        strip ${target_server_dir}/lib/*so*
        strip ${target_tools_dir}/DSN_ROOT/bin/*/*
    fi

    echo "skv server package built successfully"
}

if [[ $# -lt 1 ]]; then
    print_help 
    exit 1
fi

action=$1

case ${action} in
    build)
        if [[ $# -lt 2 ]]; then
            print_help 
            exit 1
        fi

        package_type=$2
        build ${package_type}
        ;;
    *)
        ;;
esac

