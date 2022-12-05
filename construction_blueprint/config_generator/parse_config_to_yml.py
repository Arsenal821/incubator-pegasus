from configparser import ConfigParser

if __name__ == "__main__":
    usage_list = ["meta", "replica", "both"]
    meta_usage_list = ["meta", "both"]
    replica_usage_list = ["replica", "both"]
    value_type_list = ["int", "str", "bool"]
    level_list = ["Y", "R"]

    # parse normal config
    print("start parsing normal config......")

    normal_file = open('./normal', "r")
    normal_file_meta_yml = open("./normal_meta.yml", "w")
    normal_file_replica_yml = open("./normal_replica.yml", "w")

    # 收集default_value未定的配置
    need_init_calculate_config_meta_yml = open("./need_init_meta_config.yml", "w")
    need_init_calculate_config_replica_yml = open("./need_init_replica_config.yml", "w")

    for line in normal_file:
        stripped_line = line.strip()
        if not len(stripped_line) or stripped_line.startswith("|") or stripped_line.startswith("#"):
            print("skipped line:" + stripped_line)
            continue

        print("parsing one normal config line:" + stripped_line)

        fields = stripped_line.split(",")
        stripped_fields = []
        for one_field in fields:
            one_stripped_field = one_field.strip()
            stripped_fields.append(one_stripped_field)

        # check each field
        assert(len(stripped_fields[0]))
        assert(len(stripped_fields[1]))
        assert(stripped_fields[2] in usage_list)
        assert(stripped_fields[4] in level_list)
        assert(stripped_fields[5] in value_type_list)

        one_yml_field_name = stripped_fields[0] + "|" + stripped_fields[1]

        def __write_one_yml_record(file, field, characteristics):
            file.write(field + ":\n")
            if len(characteristics[3]):
                if characteristics[5] == "int":
                    real_default_value = characteristics[3]
                else:
                    real_default_value = "'" + characteristics[3] + "'"
                file.write("  default_value" + ": " + real_default_value + "\n")

            file.write("  tag" + ": ''" + "\n")
            file.write("  desc" + ": ''" + "\n")
            file.write("  type" + ": " + characteristics[5] + "\n")

        if stripped_fields[2] in meta_usage_list:
            __write_one_yml_record(normal_file_meta_yml, one_yml_field_name, stripped_fields)
            if not len(stripped_fields[3]):
                __write_one_yml_record(need_init_calculate_config_meta_yml, one_yml_field_name, stripped_fields)

        if stripped_fields[2] in replica_usage_list:
            __write_one_yml_record(normal_file_replica_yml, one_yml_field_name, stripped_fields)
            if not len(stripped_fields[3]):
                __write_one_yml_record(need_init_calculate_config_replica_yml, one_yml_field_name, stripped_fields)

    normal_file.close()
    normal_file_meta_yml.close()
    normal_file_replica_yml.close()
    need_init_calculate_config_meta_yml.close()
    need_init_calculate_config_replica_yml.close()

    # parse thread pool config
    print("start parsing thread pool config......")

    thread_file = open('./thread', "r")
    thread_meta_yml = open("./thread_pool_meta.yml", "w")
    thread_replica_yml = open("./thread_pool_replica.yml", "w")
    for line in thread_file:
        stripped_line = line.strip()
        if not len(stripped_line) or stripped_line.startswith("|") or stripped_line.startswith("#"):
            print("skipped line:" + stripped_line)
            continue

        print("parsing one thread config line:" + stripped_line)

        fields = stripped_line.split(",")
        stripped_fields = []
        for one_field in fields:
            one_stripped_field = one_field.strip()
            stripped_fields.append(one_stripped_field)

        assert(len(stripped_fields[0]))
        assert(stripped_fields[1] in usage_list)
        if len(stripped_fields) > 2:
            assert(len(stripped_fields) == 4)
            assert(stripped_fields[2] in ["true", "false"])
            assert(stripped_fields[3].isdigit())

        section_key = "threadpool." + stripped_fields[0]
        partitioned_field_key = section_key + "|partitioned"
        worker_count_field_key = section_key + "|worker_count"

        def __write_thread_yml_record(file, partitioned_field, worker_count_field, characteristics):
            file.write(partitioned_field + ":\n")
            if len(characteristics) > 2:
                file.write("  default_value" + ": '" + characteristics[2] + "'\n")
            file.write("  tag" + ": ''" + "\n")
            file.write("  desc" + ": ''" + "\n")
            file.write("  type" + ": bool\n")

            file.write(worker_count_field + ":\n")
            if len(characteristics) > 2:
                file.write("  default_value" + ": " + characteristics[3] + "\n")
            file.write("  tag" + ": ''" + "\n")
            file.write("  desc" + ": ''" + "\n")
            file.write("  type" + ": int\n")

        if stripped_fields[1] in meta_usage_list:
            __write_thread_yml_record(thread_meta_yml, partitioned_field_key, worker_count_field_key, stripped_fields)

        if stripped_fields[1] in replica_usage_list:
            __write_thread_yml_record(thread_replica_yml, partitioned_field_key, worker_count_field_key, stripped_fields)

    thread_file.close()
    thread_meta_yml.close()
    thread_replica_yml.close()

    # parse task pool config
    print("start parsing task config......")

    task_defined_config = ConfigParser(delimiters='=')
    task_defined_config.read('./task_define_by_config.ini')

    task_file = open('./task', "r")
    task_meta_yml = open("./task_meta.yml", "w")
    task_replica_yml = open("./task_replica.yml", "w")

    has_define_by_config_count = 0
    define_by_config_in_run = {}

    normal_field_list = ["rpc_timeout_milliseconds"]
    rrdb_fields_list = ["rpc_request_throttling_mode", "rpc_request_delays_milliseconds", "is_profile", "profiler::size.request.server", "rpc_timeout_milliseconds"]

    field_to_type = {
        "rpc_request_throttling_mode": "str",
        "rpc_request_delays_milliseconds": "str",
        "is_profile": "bool",
        "profiler::size.request.server": "bool",
        "rpc_timeout_milliseconds": "int"
    }

    for line in task_file:
        stripped_line = line.strip()
        if not len(stripped_line) or stripped_line.startswith("|") or stripped_line.startswith("#"):
            print("skipped line:" + stripped_line)
            continue

        print("parsing one task config line:" + stripped_line)

        fields = stripped_line.split(",")
        stripped_fields = []
        for one_field in fields:
            one_stripped_field = one_field.strip()
            stripped_fields.append(one_stripped_field)

        assert(len(stripped_fields) == 2)
        assert(stripped_fields[1] in usage_list)

        section_key = "task." + stripped_fields[0]

        def __write_one_field(file, input_section_key, input_field_tuple):
            field_key = input_section_key + "|" + input_field_tuple[0]
            file.write(field_key + ":\n")
            # 判断对应的section和key是否在define_by_config中
            if task_defined_config.has_section(input_section_key) and task_defined_config.has_option(input_section_key, input_field_tuple[0]):
                if input_field_tuple[1] == 'int':
                    real_default_value = task_defined_config[input_section_key][input_field_tuple[0]]
                else:
                    real_default_value = "'" + task_defined_config[input_section_key][input_field_tuple[0]] + "'"
                file.write("  default_value" + ": " + real_default_value + "\n")

            file.write("  tag" + ": ''" + "\n")
            file.write("  desc" + ": ''" + "\n")
            file.write("  type: " + input_field_tuple[1] + "\n")

        real_field_list = normal_field_list
        if "RRDB" in section_key:
            real_field_list = rrdb_fields_list

        define_by_config_in_run[section_key] = {}
        for one_field in real_field_list:
            if task_defined_config.has_section(section_key) and task_defined_config.has_option(section_key, one_field):
                has_define_by_config_count += 1
                define_by_config_in_run[section_key][one_field] = 1

            if stripped_fields[1] in meta_usage_list:
                __write_one_field(task_meta_yml, section_key, [one_field, field_to_type[one_field]])

            if stripped_fields[1] in replica_usage_list:
                __write_one_field(task_replica_yml, section_key, [one_field, field_to_type[one_field]])

        # 如果此section的某些key不在'real_field_list'中，但是在define_by_config中 也需要生成相应的yml配置
        if task_defined_config.has_section(section_key):
            for one_option in task_defined_config.options(section_key):
                if one_option not in real_field_list:
                    print("section:" + section_key + ", one option:" + one_option)
                    has_define_by_config_count += 1
                    define_by_config_in_run[section_key][one_option] = 1

                    if stripped_fields[1] in meta_usage_list:
                        __write_one_field(task_meta_yml, section_key, [one_option, field_to_type[one_option]])

                    if stripped_fields[1] in replica_usage_list:
                        __write_one_field(task_replica_yml, section_key, [one_option, field_to_type[one_option]])

    print("task has_define_by_config_count:" + str(has_define_by_config_count))
    for one_section in task_defined_config.sections():
        for one_option in task_defined_config.options(one_section):
            if one_section not in define_by_config_in_run or one_option not in define_by_config_in_run[one_section]:
                print(one_section + ":" + one_option + " not in default config run.")

    task_file.close()
    task_meta_yml.close()
    task_replica_yml.close()
