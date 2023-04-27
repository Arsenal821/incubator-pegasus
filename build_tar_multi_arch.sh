# sh build_tar_multi_arch.sh --arch_version ${arch_version} --timestamp ${timestamp} --build_type ${build_type}

set -e
set -x

arch_version=
timestamp=
build_type=
while [ $# -ge 2 ]; do
  case "$1" in
    --timestamp) timestamp=$2; shift 2;;
    --arch_version) arch_version=$2; shift 2;;
    --build_type) build_type=$2; shift 2;;
    *) echo "unknown parameter $2."; exit 1; break;;
  esac
done

if [[ ${arch_version} != 'x86_64' ]] && [[ ${arch_version} != 'aarch64' ]];then
  echo "Unsupported arch version ${arch_version}"
  exit 1
fi

if [[ ${build_type} != 'release' ]] && [[ ${build_type} != 'debug' ]];then
  echo "Unsupported build type ${build_type}"
  exit 1
fi

curPath=$(readlink -f "$(dirname "$0")")
MAKE_JOB_NUM=${MAKE_JOB_NUM:-32}

if [ -d $curPath/DSN_ROOT ]; then
    echo "$curPath/DSN_ROOT exist, not need build again."
    exit 0
fi

build_start_time=`date +%s`
$curPath/run.sh build -v -t ${build_type} -c --clear_thirdparty -j ${MAKE_JOB_NUM}

release_start_time=`date +%s`
$curPath/build_package.sh build ${build_type}

# 编译 pegic & admin-cli
go env -w GOPROXY=https://jfrog-internal.sensorsdata.cn/artifactory/api/go/go
mkdir ${curPath}/PACK_OUT/skv_server/skv_tools
cd ${curPath}/admin-cli && make
cp ${curPath}/admin-cli/bin/admin-cli ${curPath}/PACK_OUT/skv_server/skv_tools
cd ${curPath}/pegic && make
cp ${curPath}/pegic/bin/pegic ${curPath}/PACK_OUT/skv_server/skv_tools

finish_time=`date +%s`
build_used_time=$((release_start_time-build_start_time))
release_used_time=$((finish_time-release_start_time))
total_used_time=$((finish_time-build_start_time))
echo "skv elapsed time: total $((total_used_time/60))m $((total_used_time%60))s, build $((build_used_time/60))m $((build_used_time%60))s, release $((release_used_time/60))m $((release_used_time%60))s"

mkdir -p /opt/skv
cd $curPath/PACK_OUT/skv_server
tar -cvf /opt/skv/skv-${arch_version}-${timestamp}.tar ./*
