#!/bin/bash
# 本脚本用于收集 skv 镜像的制品文件
# 执行前先清空可能存在的 shim skv_offline 等目录，再执行拷贝

set -e
set -x

if [ $# -gt 0 ]; then
  module_name=$1
else
  echo "args err!"
fi

INSTALL_DIR=./skv_binary
target_dir=./build-tools/dockerfiles/$module_name

# 先清空可能存在的shim和kudu目录，避免对后续拷贝造成干扰
rm -rf ${target_dir}/skv_shim
rm -rf ${target_dir}/admintools
rm -rf ${target_dir}/$module_name

cp -rf skv_shim ${target_dir}
cp -rf admintools ${target_dir}
cp -rf ${INSTALL_DIR} ${target_dir}
