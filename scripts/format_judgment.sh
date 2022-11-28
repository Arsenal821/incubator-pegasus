#/bin/bash
pwd="$( cd "$( dirname "$0"  )" && pwd )"
cd $pwd/..
is_abnormal=0

./scripts/format_files.sh

for file in `git diff --name-only`
do
    if [[ $file == *.h || $file == *.cpp ]]; then
        echo "checking $file format failed!"
        is_abnormal=1
    fi
done
if [ $is_abnormal -eq 1 ]; then
    git diff
    exit 1
fi
