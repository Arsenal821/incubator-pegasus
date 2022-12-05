#!/bin/env python3
# -*- coding: UTF-8 -*-
import fcntl
import socket
import sys
import json
import logging
import os
import configparser
from datetime import datetime

from skv_utils import shell_wrapper, template


SKV_OFFLINE_MODULE_NAME = "skv_offline"
SKV_ONLINE_MODULE_NAME = "skv_online"
SKV_META_SERVER_ROLE_NAME = "meta_server"
SKV_REPLICA_SERVER_ROLE_NAME = "replica_server"

ROLE_NAME_LIST = ["meta_server", "replica_server"]

MODULE_NAME_ROLE_NAME_TO_DATA_DIR_NAME = {
    SKV_OFFLINE_MODULE_NAME: {
        SKV_META_SERVER_ROLE_NAME: "meta_dir",
        SKV_REPLICA_SERVER_ROLE_NAME: "random_dir"
    },
    SKV_ONLINE_MODULE_NAME: {
        SKV_META_SERVER_ROLE_NAME: "online_random_dir",
        SKV_REPLICA_SERVER_ROLE_NAME: "online_random_dir"
    },
}

SKV_PROMETHEUS_PORT = {
    SKV_OFFLINE_MODULE_NAME: {
        SKV_META_SERVER_ROLE_NAME: 8370,
        SKV_REPLICA_SERVER_ROLE_NAME: 8371
    },
    SKV_ONLINE_MODULE_NAME: {
        SKV_META_SERVER_ROLE_NAME: 8360,
        SKV_REPLICA_SERVER_ROLE_NAME: 8361,
    },
}

DYNAMIC_MUTABLE_CONFIG_SIGN = '${'

APP_NAME_MAP = {
    SKV_META_SERVER_ROLE_NAME: 'meta',
    SKV_REPLICA_SERVER_ROLE_NAME: 'replica',
}


class SkvRoleBase:
    class _ShellArgs:
        def __init__(self):
            self.role_script_path = ""
            self.concrete_command = ""
            self.params_file_path = ""
            self.shim_path = ""
            self.exec_type = ""
            self.temp_dir_path = ""

            self.__parse_shell_args()

        def __parse_shell_args(self):
            if len(sys.argv) < 6:
                raise Exception(
                    "Invalid Input Shell Args. [len(sys.argv)=%d, args=%s]" % (len(sys.argv), str(sys.argv)))

            self.role_script_path = sys.argv[0]
            self.concrete_command = sys.argv[1].upper()
            self.params_file_path = sys.argv[2]
            self.shim_path = sys.argv[3]
            self.exec_type = sys.argv[4]
            self.temp_dir_path = sys.argv[5]

        def __str__(self):
            return "{role_script_path=%s, concrete_command=%s, params_file_path=%s," \
                   "shim_path=%s, exec_type=%s, temp_dir_path=%s}" % \
                   (self.role_script_path, self.concrete_command, self.params_file_path, self.shim_path, self.exec_type, self.temp_dir_path)

    def __init__(self, role_name):
        self._shellArgs = self._ShellArgs()

        self.role_name = role_name

        self.logger = None

        self.params_json_dict = {}
        self.__mutable_configuration_from_params = {}

        # machine related info
        self.module_log_dir = None
        self.parent_log_dir = None
        self.log_dir = None
        self.runtime_dir = None
        self.fqdn = None
        self.ip = None

        # module related info
        self.module_name = None
        self.meta_server_port = None
        self.meta_server_list = None
        self.replica_server_port = None
        self.replica_server_list = None
        self.zk_server_list_fqdn = None
        self.cluster_root = None

        # skv home dir
        self.skv_home_dir = None

        # skvAdminApi
        self.api = None

    def do(self):
        do_dict = {
            'INSTALL': self.install,
            'START': self.start,
            'STOP': self.stop,
            'STATUS': self.status,
            'PREPARE_NON_ROLLING_UPGRADE': self.prepare_non_rolling_upgrade,
            'FINALIZE_NON_ROLLING_UPGRADE': self.finalize_non_rolling_upgrade,
        }

        if self._shellArgs.concrete_command not in do_dict:
            raise Exception("Invalid Command! [command=%s]" % self._shellArgs.concrete_command)

        do_dict[self._shellArgs.concrete_command]()

    def install(self):
        # 渲染服务配置文件
        self._generate_config()

        # 增量渲染shell命令行工具配置文件
        self._generate_shell_config()

        # 软链 skv_offline --> skv
        # self._normalize_home_exec_dir()

    def start(self):
        exec_files_dir = os.path.join(self.skv_home_dir, self.module_name, self.role_name)
        if not os.path.exists(exec_files_dir):
            raise Exception("file dir {exec_files_dir} not exists!".format(exec_files_dir=exec_files_dir))
        config_file = self._generate_config()

        start_script_path = os.path.join(exec_files_dir, 'bin', 'start_server.sh')
        # 后面会加入 collector 模块？
        app_name = APP_NAME_MAP[self.role_name] if self.role_name in APP_NAME_MAP else self.role_name

        self.logger.info("try to start skv server. [app_name={app_name}]".format(app_name=app_name))

        output_path = self._generate_output_log_file()
        # 修改当前目录为runtime 避免core打满程序目录
        runtime_dir = os.path.join(self.runtime_dir, 'skv_%s_cores' % app_name)
        os.makedirs(runtime_dir, exist_ok=True)
        os.chdir(runtime_dir)
        start_command = "{start_script_path} {conf_file_path} {app_name} &>> {output_path}".format(
            start_script_path=start_script_path,
            conf_file_path=config_file,
            app_name=app_name,
            output_path=output_path,
        )

        shell_wrapper.check_call(start_command)

    # stop & status 操作在mothership_modules.yml里配置了相应字段后，走mothership框架逻辑，这里自己return
    def stop(self):
        return

    def status(self):
        return

    def prepare_non_rolling_upgrade(self):
        return

    def finalize_non_rolling_upgrade(self):
        return

    def initialize(self):
        self._parse_params_json_file()

        # 云平台要求安装的时候得创建好params.json传过来的日志目录
        shell_wrapper.check_call('mkdir -p {log_dir}'.format(log_dir=self.log_dir))

    def _parse_params_json_file(self):
        def __update(section, sub_key, sub_value, updater):
            if section not in updater:
                updater[section] = {}
            updater[section][sub_key] = sub_value

        with open(self._shellArgs.params_file_path) as params_json_file:
            self.params_json_dict = json.load(params_json_file)

        # fetch configuration
        original_configuration = self.params_json_dict["configurations"][self.role_name + ".ini"]
        for key, value in original_configuration.items():
            key_list = key.split('|')
            if len(key_list) != 2:
                raise Exception("Invalid configuration item in params.json. [key=%s, value=%s]" % (key, value))

            __update(key_list[0], key_list[1], value, self.__mutable_configuration_from_params)

        # fetch machine related info & module related info
        self.module_name = self.params_json_dict["module_params"]["module_name"]

        self.module_log_dir = self.params_json_dict["runtime_params"]["log_dir"]

        self.parent_log_dir = os.path.dirname(self.module_log_dir)

        self.log_dir = os.path.join(self.params_json_dict["runtime_params"]["log_dir"], self.role_name)

        self.runtime_dir = self.params_json_dict["runtime_params"]["runtime_dir"]

        self.fqdn = self.params_json_dict["node_params"]["hostname"]

        self.ip = socket.gethostbyname(self.fqdn)

        self.meta_server_port = str(self.params_json_dict["cluster_port_info"]["meta_server_ports"]["server_port"]["port"])
        self.meta_server_list = [socket.gethostbyname(one_host) + ":" + self.meta_server_port for one_host in self.params_json_dict["cluster_node_info"]["meta_server"]["nodes"]]

        self.replica_server_port = str(self.params_json_dict["cluster_port_info"]["replica_server_ports"]["server_port"]["port"])
        self.replica_server_list = [socket.gethostbyname(one_host) + ":" + self.replica_server_port for one_host in self.params_json_dict["cluster_node_info"]["replica_server"]["nodes"]]

        zookeeper_server_port = str(self.params_json_dict["dependency_resource_connection_info"]["zookeeper"]["zk_client_port"])
        self.zk_server_list_fqdn = [one_host + ":" + zookeeper_server_port for one_host in self.params_json_dict["dependency_resource_connection_info"]["zookeeper"]["zk_server_nodes"]]

        # TODO
        # meta server 元数据信息应当从mothership平台里获取
        self.cluster_root = os.path.join(self.params_json_dict["runtime_params"]["backpack_path"], self.module_name)

        # skv home dir
        # self.skv_home_dir = os.environ["SKV_HOME"]
        self.skv_home_dir = self.params_json_dict["source_params"]["home_path"]

        # logger
        self.logger = self.__init_logger()

        self.logger.info("parsed info from params.json. [module_name={module_name}, log_dir={log_dir},"
                         "runtime_dir={runtime_dir},fqdn={fqdn}, ip={ip},meta_server_list={meta_server_list},"
                         "replica_server_list={replica_server_list}, zk_server_list={zk_server_list},"
                         "skv_home_dir={skv_home_dir}, shellArgs={_shellArgs}]"
                         .format(module_name=self.module_name,
                                 log_dir=self.log_dir,
                                 runtime_dir=self.runtime_dir,
                                 fqdn=self.fqdn,
                                 ip=self.ip,
                                 meta_server_list=self.meta_server_list,
                                 replica_server_list=self.replica_server_list,
                                 zk_server_list=self.zk_server_list_fqdn,
                                 skv_home_dir=self.skv_home_dir,
                                 _shellArgs=self._shellArgs))

        if 'SKV_HOME' not in os.environ:
            os.environ['SKV_HOME'] = self.skv_home_dir
        sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
        from skv_admin_api import SkvAdminApi
        self.api = SkvAdminApi(self.logger, self.module_name, ','.join(self.meta_server_list))

    '''
    见 skv_shim/config_change/config_change.py 注释里关于配置类型的描述
    这里不同的role('meta_server'、'replica_server')返回各自的b类型配置
    '''
    def _dynamic_and_immutable_config(self):
        raise Exception("_dynamic_and_immutable_config not implemented!")

    def _generate_config(self):
        config_template_file_name = "%s.ini.template" % self.role_name
        config_template_file = os.path.join(self._shellArgs.shim_path, "template", config_template_file_name)

        # 这里加上 delimiters='=' 的原因是 : pegasus里有类似profiler::size.request.server=true 的配置，被此parser解析后，渲染出的结果会变成 : "profiler = :size.request.server = true"
        config_parser = configparser.ConfigParser(delimiters='=')

        if not os.path.exists(config_template_file):
            raise Exception("can not find %s!" % config_template_file)
        config_parser.read(config_template_file)

        configs_list = [self._dynamic_and_immutable_config(), self.__mutable_configuration_from_params]

        for one_configs in configs_list:
            for (section, kv) in one_configs.items():
                if not kv:
                    continue
                if not config_parser.has_section(section):
                    config_parser.add_section(section)
                for (name, value) in kv.items():
                    config_parser.set(section, name, str(value))

        config_file = os.path.join(self.runtime_dir, "conf", "%s.ini" % self.role_name)
        config_file_dir = os.path.dirname(config_file)
        if not os.path.isdir(config_file_dir):
            os.mkdir(config_file_dir)
        with open(config_file, "w+") as f:
            config_parser.write(f)

        return config_file

    def _generate_shell_config(self):
        shell_config_dir = os.path.join(self.skv_home_dir, self.module_name, self.role_name, "tools/src/shell")
        self.logger.info("Try to render skv shell config: {shell_config_dir}".format(
            shell_config_dir=shell_config_dir,
        ))

        if not os.path.isdir(shell_config_dir):
            raise Exception('cannot find %s!' % shell_config_dir)
        else:
            log_dir = os.path.join(self.parent_log_dir, 'shell')
            module_template_dir = os.path.join(
                self._shellArgs.shim_path,
                'template')
            config_ini = 'config.ini'

            # 额外写一个空文件 'config.flock' 专门用来对 config.ini 进行排他性读写
            flock_file = os.path.join(shell_config_dir, "config.flock")
            with open(flock_file, "a") as f:
                # 文件加排他锁
                fcntl.flock(f, fcntl.LOCK_EX)

                # 读取 ${shell_config_dir}/config.ini 中之前可能已经渲染到section-'pegasus.clusters' 里的配置项
                module_to_server_list = {}
                possible_exist_shell_config_file = os.path.join(shell_config_dir, config_ini)
                if os.path.exists(possible_exist_shell_config_file):
                    config_parser = configparser.ConfigParser(delimiters='=')
                    config_parser.read(possible_exist_shell_config_file)
                    module_to_server_list = {
                        option_name: option_value
                        for option_name, option_value in config_parser.items("pegasus.clusters") if '@' not in option_name
                    }

                module_to_server_list[self.module_name] = ",".join(self.meta_server_list)
                meta_server_conf = "\n".join(["%s=%s" % (k, v) for (k, v) in module_to_server_list.items()])
                param = {
                    'meta_server_conf': meta_server_conf,
                    'log_dir': log_dir,
                }

                template.render_template(config_ini, param, module_template_dir, 'shell_config', shell_config_dir)

    def _normalize_home_exec_dir(self):
        dst = os.path.join(self.skv_home_dir, "skv")

        if os.path.exists(dst) and os.path.isdir(dst):
            return

        src = os.path.join(self.skv_home_dir, "skv_offline")
        if not os.path.exists(src):
            src = os.path.join(self.skv_home_dir, "skv_online")
        if not os.path.exists(src):
            raise Exception("skv_offline & skv_online exec bin dir are all not exists!")

        cmd = 'ln -s {src} {dst}'.format(src=src, dst=dst)
        shell_wrapper.check_call(cmd)

    def __init_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        log_dir = os.path.join(self.log_dir, __name__)
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        timestamp_postfix = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]
        log_file_path = os.path.join(log_dir, self.role_name + "_" + str(timestamp_postfix) + ".log")
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
        logger.addHandler(file_handler)

        # 配置console 打印INFO级别
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logger.addHandler(console)

        return logger

    @staticmethod
    def _output_header(output_file_path):
        shell_wrapper.check_call(
            "echo \"START_TIME: $(date +\"%Y-%m-%d %H:%M:%S\")\" >> {output_path}".format(
                output_path=output_file_path))

    def _generate_output_log_file(self):
        shell_wrapper.check_call('mkdir -p {log_dir}'.format(log_dir=self.log_dir))

        symlink_name = "{app_name}.output.ERROR".format(
            app_name=APP_NAME_MAP[self.role_name] if self.role_name in APP_NAME_MAP else self.role_name,
        )

        current_datetime = datetime.now()
        timestamp_postfix = current_datetime.strftime('%Y%m%d_%H%M%S_%f')[:-3]
        file_name = "{symlink_name}.{timestamp_postfix}".format(
            symlink_name=symlink_name,
            timestamp_postfix=timestamp_postfix,
        )

        output_path = os.path.join(self.log_dir, file_name)
        self._output_header(output_path)

        symlink_path = os.path.join(self.log_dir, symlink_name)
        try:
            os.unlink(symlink_path)
        except FileNotFoundError:
            pass
        os.symlink(file_name, symlink_path)

        return output_path


if __name__ == "__main__":

    current_dir = os.path.dirname(__file__)

    meta_server_instance = SkvRoleBase("meta_server")
    meta_server_instance.__getattribute__("_shellArgs").temp_dir_path = current_dir
    meta_server_instance.__getattribute__("_shellArgs").shim_path = os.path.dirname(current_dir)
    meta_server_instance.logger = meta_server_instance._SkvRoleBase__init_logger()
    meta_server_instance.module_name = "skv_offline"
    meta_server_instance.skv_home_dir = os.path.join(current_dir, "test_home_dir")
    meta_server_instance.parent_log_dir = os.path.join(current_dir, "logs")
    shell_wrapper.check_call('mkdir -p {dir}'.format(dir=os.path.join(meta_server_instance.skv_home_dir, meta_server_instance.module_name, "tools/src/shell")))
    meta_server_instance.meta_server_list = ["127.0.0.1:8180", "127.0.0.1:8181", "127.0.0.1:8182"]

    meta_server_instance._generate_shell_config()

    meta_server_instance.module_name = "skv_online"
    meta_server_instance.meta_server_list = ["127.0.0.1:8281", "127.0.0.1:8281", "127.0.0.1:8282"]
    meta_server_instance._generate_shell_config()
