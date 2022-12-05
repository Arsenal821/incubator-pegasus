from configparser import ConfigParser
import xlwt

# |配置文件|section|option|value|是否动态(是否需要在安装时根据获得的集群信息进行初始化)|动态获取方式(需要获取集群哪些信息, 如何计算)|
if __name__ == "__main__":
    # meta_server.ini
    meta_config = ConfigParser(delimiters='=')
    meta_config.read('./meta_server.ini')

    meta_config_work_book = xlwt.Workbook(encoding='utf8')
    meta_config_work_sheet = meta_config_work_book.add_sheet("meta")

    meta_config_work_sheet.write(0, 0, "配置文件")

    meta_config_work_sheet.write(0, 1, "配置节(section)")
    meta_config_work_sheet.write(0, 2, "配置键(option)")
    meta_config_work_sheet.write(0, 3, "配置值(value)")
    meta_config_work_sheet.write(0, 4, "是否动态(是否需要在安装时根据获得的集群信息进行初始化)")
    meta_config_work_sheet.write(0, 5, "动态获取方式(需要获取集群哪些信息, 如何计算)")

    meta_config_work_sheet.write(1, 0, "meta_server.ini")

    func_list = []
    func_list.append((lambda x: 'task.' not in x and 'threadpool.' not in x))
    func_list.append((lambda x: 'threadpool.' in x))
    func_list.append((lambda x: 'task.' in x))

    line_num = 1
    pre_section = ""
    for one_func in func_list:
        for one_section in meta_config.sections():
            if one_func(one_section):
                for one_option in meta_config.options(one_section):
                    value = meta_config[one_section][one_option]
                    if one_section != pre_section:
                        meta_config_work_sheet.write(line_num, 1, one_section)
                        pre_section = one_section
                    meta_config_work_sheet.write(line_num, 2, one_option)
                    meta_config_work_sheet.write(line_num, 3, value)
                    line_num += 1

    meta_config_work_book.save("meta_server_config.xls")

    # replica_server.ini
    replica_config = ConfigParser(delimiters='=')
    replica_config.read('./replica_server.ini')

    replica_config_work_book = xlwt.Workbook(encoding='utf8')
    replica_config_work_sheet = replica_config_work_book.add_sheet("replica")

    replica_config_work_sheet.write(0, 0, "配置文件")

    replica_config_work_sheet.write(0, 1, "配置节(section)")
    replica_config_work_sheet.write(0, 2, "配置键(option)")
    replica_config_work_sheet.write(0, 3, "配置值(value)")
    replica_config_work_sheet.write(0, 4, "是否动态(是否需要在安装时根据获得的集群信息进行初始化)")
    replica_config_work_sheet.write(0, 5, "动态获取方式(需要获取集群哪些信息, 如何计算)")

    replica_config_work_sheet.write(1, 0, "meta_server.ini")

    line_num = 1
    pre_section = ""
    for one_func in func_list:
        for one_section in replica_config.sections():
            if one_func(one_section):
                for one_option in replica_config.options(one_section):
                    value = replica_config[one_section][one_option]
                    if pre_section != one_section:
                        replica_config_work_sheet.write(line_num, 1, one_section)
                        pre_section = one_section
                    replica_config_work_sheet.write(line_num, 2, one_option)
                    replica_config_work_sheet.write(line_num, 3, value)
                    line_num += 1

    replica_config_work_book.save("replica_server_config.xls")
