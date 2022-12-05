#!/bin/bash
# 本脚本用于获取当前打包级别下最新的 prebuild 的 skv 二进制tar包
# 运行环境为编译镜像
# 本地运行时可能会因为环境变量 IS_DEVELOP 缺失导致下载行为不符合预期，可通过手动设置解决
# 下载的二进制 tar 包会被解压到 ./binary 目录下( docker 场景下，目录识别存在限制)
# 由于历史原因，多个打包job可能会并发执行且无法控制先后，所以在脚本前做了存在性判断，旨在加快打包速度

set -ex

INSTALL_DIR=./binary/installed

script_path=$(cd "$(dirname "$0")"; pwd)
level=
if [ $IS_DEVELOP == 1 ]
then
    level="develop"
else
    level="test"
fi

# 清空当前路径上可能存在的package_index.json文件
rm -rf *package_index.json

# for test on local, 应该在job中配置,本地调试时替换为自己的key
#JFROG_PASSWORD=xxx
#jfrog c add --url=https://jfrog-internal.sensorsdata.cn/ --password=${JFROG_PASSWORD}  --interactive=false

version='2.4'

# 下载用于统计的package_index.json文件
jfrog rt dl dragon-internal/inf/skv/skv_prebuild_binary/${version}/${level}/package_index.json --flat
latest_package=`python3 prebuild-tools/get_latest_package.py .`
version_tag=${latest_package: 4 :10}

package_url=https://jfrog-internal.sensorsdata.cn:443/artifactory/dragon-internal/inf/skv/skv_prebuild_binary/${version}/${level}/${version_tag}/${latest_package}

wget ${package_url}

mkdir -p skv_binary
tar -xvf ${latest_package} -C ./skv_binary
