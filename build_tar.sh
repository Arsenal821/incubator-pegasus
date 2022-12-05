#!/bin/bash
# 本脚本用于 skv 二进制文件的 prebuild 操作，预先生成二进制的 tar 包
# 该脚本对应 Jenkins 中 skv 的 prebuild 任务，修改时请注意配套修改
# 由于是非标准打包，无法获取、生成版本号，故需要在 Jenkins job 调用执行脚本时传入一个时间字符串作为参数
# 执行格式如下所示：
#
# sh build_tar.sh ${build_type} ${timestamp}
#
# 执行示例如下所示：
#
# sh build_tar.sh release 2022-11-01-15-12-20
#
# 该脚本执行完后tar的路径为/opt/skv/skv-${timestamp}.tar
# 生成的tar包会被Jenkins job上传到jforg仓库，仓库路径如下：
# dragon-internal/inf/skv/skv_prebuild_binary/${level}/
# 其中${level}是打包的级别，取值范围为[develop, test]

set -e
set -x
curPath=$(readlink -f "$(dirname "$0")")
MAKE_JOB_NUM=${MAKE_JOB_NUM:-32}

if [ $# -gt 1 ]; then
  build_type=$1
  timestamp=$2
else
  echo "args err!"
fi

build_type=$1
if [ "$build_type" != "debug" -a "$build_type" != "release" ]; then
	echo "Not exist build_type $build_type !"
	exit 1
fi

if [ -d $curPath/DSN_ROOT ]; then
    echo "$curPath/DSN_ROOT exist, not need build again."
    exit 0
fi

build_start_time=`date +%s`
$curPath/run.sh build -v -t $build_type -c --clear_thirdparty -j ${MAKE_JOB_NUM}

release_start_time=`date +%s`
$curPath/build_package.sh build $build_type

finish_time=`date +%s`
build_used_time=$((release_start_time-build_start_time))
release_used_time=$((finish_time-release_start_time))
total_used_time=$((finish_time-build_start_time))
echo "skv elapsed time: total $((total_used_time/60))m $((total_used_time%60))s, build $((build_used_time/60))m $((build_used_time%60))s, release $((release_used_time/60))m $((release_used_time%60))s"

mkdir /opt/skv
cd $curPath/PACK_OUT/skv_server
tar -cvf /opt/skv/skv-${timestamp}.tar ./*
