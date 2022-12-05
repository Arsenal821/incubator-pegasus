# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

from functools import reduce
import string

from skv_admin_api import SkvAdminApi
from skv_upgrader.skv_upgrader_package import SkvUpgraderPackage

HOT_UPGRADE_TYPE = 'hot'
COLD_UPGRADE_TYPE = 'cold'
UPGRADE_TYPE_SET = {HOT_UPGRADE_TYPE, COLD_UPGRADE_TYPE}

BEFORE_UPGRADE_PY_FILE_NAME = 'before_upgrade.py'
BEFORE_UPGRADE_ENTRY_NAME = 'main'


def get_first_host(host_list):
    return min(host_list) if host_list else None


def str_is_camel_case(s):
    if len(s) == 0:
        return False

    for c in s:
        if c not in string.ascii_letters:
            return False

    return True


def camel_case_to_snake_case(s):
    return reduce(lambda x, y: x + ('_' if y.isupper() else '') + y, s).lower()


def get_version_from_line(line):
    if isinstance(line, bytes):
        # while line is extracted from a tar file,
        # it will be a bytes rather than a str
        line = line.decode('utf-8')
    elif not isinstance(line, str):
        raise Exception("unsupported line type from skv version file: "
                        "{type_name}".format(type_name=type(line).__name__))

    line = line.lower()

    begin = line.find('server')
    if begin < 0:
        return None
    else:
        begin += len('server')

    for end_keyword in ['release', 'debug']:
        end = line.find(end_keyword, begin)
        if end >= 0:
            break
    else:
        return None

    version = line[begin:end].strip()
    if version:
        return version
    else:
        raise ValueError("invalid skv version line")


def get_file_from_opened_file(f, module_path, module_name, version_file_name):
    for line in f:
        try:
            version = get_version_from_line(line)
            if version:
                return version
        except ValueError:
            raise Exception(
                "skv version line is invalid in {version_file_name} of "
                "{module_path} for {module_name}: {line}".format(
                    version_file_name=version_file_name,
                    module_path=module_path,
                    module_name=module_name,
                    line=line,
                )
            )

    raise Exception(
        "there is no skv version info in {version_file_name} of "
        "{module_path} for {module_name}".format(
            version_file_name=version_file_name,
            module_path=module_path,
            module_name=module_name,
        )
    )


def get_version_from_module_package(pack_path, module_name, version_file_name, logger):
    skv_upgrader_package = SkvUpgraderPackage(pack_path, module_name, logger)
    return skv_upgrader_package.get_version_from_file(
        version_file_name, get_file_from_opened_file,
    )


def get_version_from_module_dir(module_dir, module_name, version_file_name, logger):
    version_file_path = "{module_dir}/{version_file_name}".format(
        module_dir=module_dir, version_file_name=version_file_name)
    with open(version_file_path) as f:
        return get_file_from_opened_file(f, module_dir, module_name, version_file_name)


def get_version_from_target_files(module_path, module_name, version_file_name_list,
                                  version_file_extractor, logger):
    version = None
    parsed_file_count = 0
    for version_file_name in version_file_name_list:
        try:
            current_version = version_file_extractor(
                module_path, module_name, version_file_name, logger,
            )
        except Exception as exc:
            logger.warn(
                "parse {version_file_name} in {module_path} for {module_name} "
                "error: {exc}".format(
                    version_file_name=version_file_name,
                    module_path=module_path,
                    module_name=module_name,
                    exc=exc,
                )
            )
            continue

        parsed_file_count += 1

        if not current_version:
            continue

        if not version:
            version = current_version
            continue

        if current_version != version:
            raise Exception(
                "there are two different skv versions in {module_path} for "
                "{module_name}: {current_version} and {version}".format(
                    module_path=module_path,
                    module_name=module_name,
                    current_version=current_version,
                    version=version,
                )
            )

    return version, parsed_file_count


def get_version_from_files(module_path, module_name, version_file_extractor, logger):
    for version_file_name_list in [['META_SERVER_VERSION', 'REPLICA_SERVER_VERSION'], ['VERSION']]:
        version, parsed_file_count = get_version_from_target_files(
            module_path, module_name, version_file_name_list, version_file_extractor, logger
        )
        if parsed_file_count == 0:
            logger.warn(
                "none of {version_file_name_list} in {module_path} for {module_name} "
                "has been parsed successfully".format(
                    version_file_name_list=version_file_name_list,
                    module_path=module_path, module_name=module_name,
                )
            )
            continue
        elif parsed_file_count < len(version_file_name_list):
            raise Exception(
                "some of {version_file_name_list} in {module_path} for {module_name} "
                "has failed to be parsed".format(
                    version_file_name_list=version_file_name_list,
                    module_path=module_path, module_name=module_name,
                )
            )

        if not version:
            raise Exception(
                "all of {version_file_name_list} have no skv version info "
                "in {module_path} for {module_name}".format(
                    version_file_name_list=version_file_name_list,
                    module_path=module_path, module_name=module_name,
                )
            )

        return version

    raise Exception(
        "there is no skv version info in {module_path} for {module_name}".format(
            module_path=module_path, module_name=module_name
        )
    )


def get_current_version(module_dir, module_name, logger, skv_admin_api=None):
    if not skv_admin_api:
        skv_admin_api = SkvAdminApi(logger, module_name)

    try:
        from_version = skv_admin_api.get_version()
        return from_version
    except Exception as exc:
        raise Exception(
            "failed to get skv version by server_info from {path}: "
            "{exc}, maybe skv instances have been stopped, or config "
            "is missing for shell, or skv versions is too old".format(
                path=skv_admin_api.skv_tool_run_script, exc=exc,
            )
        )
