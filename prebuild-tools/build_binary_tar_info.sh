#!/bin/bash
# 本脚本用于 skv 二进制文件的 prebuild 操作的 meta 文件更新
# 该脚本对应 Jenkins 中 skv 的 prebuild 任务之后的脚本回调，修改时请注意配套修改
# 执行格式如下所示：
#
# sh build_binary_tar.sh ${timestamp} ${build_type}
#
# 执行示例如下所示：
#
# sh build_binary_tar_info.sh 2022-11-01-16-11-20 develop
#
# 生成的 ${timestamp}.manifest.json 会被 Jenkins job 上传到 jforg 仓库，仓库路径如下：
# dragon-internal/inf/skv/skv_prebuild_binary/${version}/${level}/
# ${level} 是打包的级别，取值范围为[develop, test]
# ${version} 是分支版本，目前支持 2.0、2.4
# 同时更新仓库中最新的20次打包信息，并筛选出最近一次的打包制品，保存在同一目录下的 package_index.json 文件中

set -e
set -x

os_version=$(cat /etc/redhat-release | sed 's/.*release //g' | awk '{print $1}' | awk -F. '{print $1}')
os_tag=el6
if [ $os_version -eq 6 ]; then
    os_tag=el6
elif [ $os_version -eq 7 ]; then
    os_tag=el7
elif [ $os_version -eq 9 ]; then
    os_tag=el9
else
    echo "os: $os_version not support!"
    exit 1
fi

build_type=''
timestamp=''

if [ $# -gt 1 ]; then
  timestamp=$1
  build_type=$2
else
  echo "args err!"
fi

package_name='skv-'${timestamp}
full_package_name='skv-'${timestamp}'.tar'

script_path=$(cd "$(dirname "$0")"; pwd)

# 生成 manifest 文件
python3 ${script_path}/gen_manifest.py ${package_name} ${build_type}
mv manifest.json ${package_name}.manifest.json

# for test on local, 应该在job中配置,本地调试时替换为自己的key
#JFROG_PASSWORD=xxx
#jfrog c add --url=https://jfrog-internal.sensorsdata.cn/ --password=${JFROG_PASSWORD}  --interactive=false

# 上传 manifest.json
version='2.4'
jfrog rt u ${package_name}.manifest.json dragon-internal/inf/skv/skv_prebuild_binary/${version}/${build_type}/${package_name}.manifest.json
rm ${package_name}.manifest.json

# 更新用于统计的package_index.json文件
jfrog rt dl dragon-internal/inf/skv/skv_prebuild_binary/${version}/${build_type}/*.manifest.json --flat
python3 ${script_path}/update_manifest.py .
jfrog rt u package_index.json dragon-internal/inf/skv/skv_prebuild_binary/${version}/${build_type}/package_index.json --flat
rm *.manifest.json
rm package_index.json
