import os
import sys

from hyperion_utils.shell_utils import check_call

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from skv_common import is_skv_in_mothership


def get_service_controller(logger, module):
    """
    对外接口 请调用这个接口
    """
    if is_skv_in_mothership(module):
        from mothership_service_controller import MothershipServiceController
        return MothershipServiceController(logger, module)
    else:
        from captain_service_controller import CaptainServiceController
        return CaptainServiceController(logger, module)


def get_skv_config_manager(module, role, logger, verbose=True):
    """
    对外接口 请调用这个接口
    """
    if is_skv_in_mothership(module):
        from mothership_skv_config_manager import MothershipSkvConfigManager
        return MothershipSkvConfigManager(module, role, logger, verbose)
    else:
        from zk_skv_config_manager import ZkSkvConfigManager
        return ZkSkvConfigManager(module, role, logger, verbose)


def _get_client_conf(module, conf_key):
    """
    获取客户端配置
    """
    if is_skv_in_mothership(module):
        # 一般已经在sys.path里面了
        from mothership_client import MothershipClient
        client = MothershipClient()
        return client.get_module_guidance_config(module, conf_key)
    else:
        # 如果是老版本 则用zk
        from hyperion_client.config_manager import ConfigManager
        from skv_common import SKV_PRODUCT_NAME
        return ConfigManager().get_client_conf_by_key(SKV_PRODUCT_NAME, module, conf_key)


def _update_client_conf(module, conf_key, conf_value):
    """
    更新客户端配置
    """
    if is_skv_in_mothership(module):
        cmd = 'mothershipadmin module connection_info update --module "%s" --key "%s" --value "%s"' % (
            module, conf_key, conf_value)
        check_call(cmd)  # 这里接口就没有传入日志 比较坑 先这样吧 反正如果能抛异常 可以去看mothership的admin日志
    else:
        # 如果是sp 2.0 则用zk
        from hyperion_client.config_manager import ConfigManager
        from skv_common import SKV_PRODUCT_NAME
        ConfigManager().set_client_conf_by_key(SKV_PRODUCT_NAME, module, conf_key, conf_value)
