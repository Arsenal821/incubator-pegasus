#!/bin/env python3
# -*- coding: UTF-8 -*-

import hashlib
import json
import os
import sys
import time


def make_manifest(package_name, level, version, timestamp=time.time()):
    """
    Make a manifest.json document from the contents of a kudu package file.

    @param package_name: The name of the package to make mnifest.
    @param level: The package level.
    @param timestamp: Unix timestamp to place in manifest.json
    @return: the manifest.json as a string
    """
    manifest = {}
    manifest['lastUpdated'] = int(timestamp * 1000)
    manifest['packages'] = []
    manifest['packageName'] = package_name + '.tar'
    package_full_name = os.path.join('/opt/skv/', package_name + '.tar')
    if os.path.exists(package_full_name):
        print("Found skv package %s" % package_full_name)
        entry = {}
        entry['packageName'] = package_name + '.tar'
        entry['level'] = level
        entry['version'] = version
        # releaseNotes 暂时写死
        entry['releaseNotes'] = 'For Sensors Skv Packing'
        with open(package_full_name, 'rb') as fp:
            entry['hash'] = hashlib.sha1(fp.read()).hexdigest()
        entry['replaces'] = "Skv"
        entry['jforg_path'] = os.path.join('dragon-internal/inf/skv/skv_prebuild_binary', version, level, package_name + '.manifest.json')
        manifest['packages'].append(entry)
    else:
        print("Package : %s is not exist" % package_full_name)
        raise Exception("Could not find package: " + package_full_name)
    return json.dumps(manifest, indent=4, separators=(',', ': '))


if __name__ == "__main__":
    package_name = ''
    level = ''
    version = '2.4'
    if len(sys.argv) > 2:
        package_name = sys.argv[1]
        level = sys.argv[2]
    print("Working package : %s, package level : %s." % (package_name, level))

    manifest = make_manifest(package_name, level, version)
    path = os.path.curdir
    with open(os.path.join(path, 'manifest.json'), 'w') as fp:
        fp.write(manifest)
