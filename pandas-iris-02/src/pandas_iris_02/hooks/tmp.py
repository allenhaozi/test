
class Metastore(Enum):
    Name = 'metaName'
    Id = 'metaId'


def check_args(expected: bool, msg: str):
    if not expected:
        raise Exception(msg)


def remove_nulls(d):
    return {k: v for k, v in d.items() if v is not None}


'''
metastore service
'''


class StorageService:

    def __init__(self, meta_obj):
        self.id = meta_obj['id']
        self.connection_config = meta_obj['metaExtra']['hdfs-connection']
        self.meta_obj = meta_obj
        check_args('hadoop_conf' in self.connection_config, 'hadoop_conf not set')
        check_args('core-site.xml' in self.connection_config['hadoop_conf'], 'core-site.xml not found')

        core_site = ET.fromstring(self.connection_config['hadoop_conf']['core-site.xml'])
        # xml_root = core_site.getroot()
        self.default_fs = None
        for property in core_site:
            p_name = None
            p_value = None
            for ele in property:
                if ele.tag == 'name':
                    p_name = ele.text
                elif ele.tag == 'value':
                    p_value = ele.text
            if p_name == 'fs.defaultFS':
                if p_value.endswith('/'):
                    self.default_fs = p_value[:-1]
                else:
                    self.default_fs = p_value

                if self.default_fs.endswith(':8020'):
                    self.default_fs = self.default_fs[:-5]
                break
        check_args(self.default_fs is not None, 'fs.defaultFS not found in core-site.xml')
        logging.info(f"Using fs.defaultFS = {self.default_fs}")



class MetaResolver:
    def __init__(self, meta_api):
        self.meta_api = meta_api

        # 后期可能支持别的类型
        self.items = {
            'locations': dict(),
            'tables': dict()
        }

    def resolve_meta_obj_from_meta(self, param_type, meta):
        ret = {}
        if param_type.lower() == 'table':
            if Metastore.Id.value in meta:
                meta_obj = self.meta_api.get_by_id('tables', meta[Metastore.Id.value])
            elif Metastore.Name.value in meta:
                meta_obj = self.meta_api.get_by_name('tables', meta[Metastore.Name.value])
            else:
                raise Exception("value中需有metaName or metaId")

            location_id = meta_obj['location']['id']
            location = self.meta_api.get_location(location_id)
            ret['uri'] = location['path']
            ret['meta_obj'] = meta_obj
            return ret
        else:
            if Metastore.Id.value in meta:
                location = self.meta_api.get_location(meta[Metastore.Id.value])
            elif Metastore.Name.value in meta:
                location = self.meta_api.get_location_by_name(meta[Metastore.Name.value])
            else:
                raise Exception("value中需有metaName or metaId")
            ret['meta_obj'] = location
            ret['uri'] = location['path']
            return ret

    def create_output_meta_obj(self, param_type, meta_name):
        ret = {}
        # Clear previous output
        if param_type.lower() == 'table':
            self.meta_api.delete_by_name_ignore_errors('tables', meta_name)
        self.meta_api.delete_by_name_ignore_errors('locations', meta_name)

        location_type = param_type.capitalize()
        if location_type.lower() == 'workdir':
            location_type = 'Prefix'
        created_location = self.meta_api.post("locations", json={
            'name': meta_name,
            'locationType': location_type
        })
        ss = self.meta_api.get_storage_service(created_location['service']['id'])
        created_location['path'] = ss.default_fs + created_location['path']

        ret['meta_obj'] = created_location
        ret['uri'] = created_location['path']

        logging.info(f"Created output location for {meta_name}: {ret['uri']}")
        return ret

    def get_service_config(self, ss_id):
        # all locations have been resolved, now check whether the storage services
        # the same. All inputs/outputs should be on the same HDFS cluster.
        ss = self.meta_api.get_by_id('services/storageServices', ss_id)
        check_args(ss['serviceType'] == 'HDFS', "Only HDFS is supported for now")
        conn = ss['metaExtra']['hdfs-connection']
        check_args(conn['type'] == 'HDFS', f"目前只支持HDFS, type={conn['type']}")
        check_args('hadoop_conf' in conn and conn['hadoop_conf'] is not None, "hadoop_conf not found in metaExtra")
        hadoop_config = {
            'hadoop_user_name': conn['hadoopUserName'],
            'config_files': conn['hadoop_conf']
        }
        krb5_config = None
        if 'kerberos_conf' in conn and conn['kerberos_conf'] is not None:
            krb5_config = {
                'principal': conn['kerberos_conf']['principal'],
                'config_files': {
                    'keytab_base64': conn['kerberos_conf']['keytab'],
                    'krb5.conf': conn['kerberos_conf']['krb5.conf']
                }
            }
        return hadoop_config, krb5_config


'''
Operator处理逻辑
'''


def _convert_list_to_map(op_meta):
    inputs_dict = {}
    if isinstance(op_meta, list):
        for meta in op_meta:
            inputs_dict[meta['name']] = meta
    else:
        inputs_dict[op_meta['name']] = op_meta
    return inputs_dict


def _merge_op_op_instance(operator_is_json, operator_def_json):
    rst = operator_is_json
    for item in rst:
        # 通过name找到Operator的定义,找到当前name对应的format, operator_inputs_json
        op_name = item['name']
        op_value = operator_def_json[op_name]
        item.update({'def': op_value})
        # operator定义为array
        if 'arraySubFormat' in op_value and op_value['arraySubFormat'] is not None:
            sub_format = op_value['arraySubFormat']  # operator中的定义
            sub_values = item['subValues']
            if sub_values is not None:
                for value in sub_values:
                    value['format'] = sub_format
            else:
                logging.warning(f"operator name:{op_name}为array,instance输入为null")
        else:
            format_type = op_value['format']
            item['format'] = format_type
    return rst


class OperatorAdapter:
    def __init__(self):
        # 算子定义json
        self.ophub_data_dir = "/workspace/ophub-data"
        self.runtimed_dir = "/workspace/ophub-runtime"

        self.resolved_dir = f"{self.runtimed_dir}/resolved-data"
        os.makedirs(self.resolved_dir, mode=0o777, exist_ok=True)
        self.meta_url = "http://{}".format(os.getenv("OPHUB_METADATA_HOST"))
        self.meta_api = MetaServiceApi(self.meta_url)
        self.ss_ids = set()

    def resolve_inputs_file(self):
        # def json
        inputs_file = f"{self.ophub_data_dir}/inputs-def.json"
        with open(inputs_file, 'r') as f:
            operator_def_json = json.load(f, object_hook=remove_nulls)
        operator_def_json = _convert_list_to_map(operator_def_json)

        # json
        operator_is_file = f"{self.ophub_data_dir}/inputs.json"
        with open(operator_is_file, 'r') as f:
            operator_is_json = json.load(f, object_hook=remove_nulls)

        if operator_def_json is not None:
            inputs_params = _merge_op_op_instance(operator_is_json, operator_def_json)
            meta = MetaResolver(self.meta_api)
            for inputs in inputs_params:
                if 'subValues' in inputs and inputs['subValues'] is not None:
                    # get metastore
                    for item in inputs['subValues']:
                        item_type = item['format']
                        # TODO error and check null
                        ret = meta.resolve_meta_obj_from_meta(item_type, item['value'])
                        item['value'] = ret
                        ss_id = ret['meta_obj']['service']['id']
                        service_type = ret['meta_obj']["serviceType"]
                        if service_type == 'HDFS':
                            self.ss_ids.add(ss_id)
                else:
                    item_type = inputs['format']
                    ret = meta.resolve_meta_obj_from_meta(item_type, inputs['value'])
                    inputs['value'] = ret
                    ss_id = ret['meta_obj']['service']['id']
                    service_type = ret['meta_obj']["serviceType"]
                    if service_type == 'HDFS':
                        self.ss_ids.add(ss_id)

            with open(f'{self.resolved_dir}/resolved_inputs.json', 'w') as f:
                json.dump(inputs_params, f, indent=4)
                logging.info("Resolved inputs params:\n%s", json.dumps(inputs_params, indent=4))
        else:
            logging.info("Inputs.json is Null")

    def resolve_outputs_file(self):
        '''
        1.create location
        2.gen file
        :return:
        '''
        # outputs def  file
        outputs_file = f"{self.ophub_data_dir}/outputs-def.json"
        with open(outputs_file, 'r') as f:
            operator_def_file = json.load(f, object_hook=remove_nulls)
        operator_def_json = _convert_list_to_map(operator_def_file)

        # outputs json
        operator_instance_outputs_file = f"{self.ophub_data_dir}//outputs.json"
        with open(operator_instance_outputs_file, 'r') as f:
            operator_is_outputs_json = json.load(f, object_hook=remove_nulls)

        outputs_params = _merge_op_op_instance(operator_is_outputs_json, operator_def_json)

        meta = MetaResolver(self.meta_api)
        for outputs in outputs_params:
            if 'subValues' in outputs and outputs['subValues'] is not None:
                # get metastore
                for item in outputs['subValues']:
                    item_type = item['format']
                    if Metastore.Name.value in item['value']:
                        meta_name = item['value'][Metastore.Name.value]
                        logging.info(f"Creating location for {item['name']}, metaName={meta_name}")
                        item['value'] = meta.create_output_meta_obj(item_type, meta_name)
                    else:
                        raise Exception("value 中需要有metaName")
            else:
                item_type = outputs['format']
                if Metastore.Name.value in outputs['value']:
                    meta_name = outputs['value'][Metastore.Name.value]
                    logging.info(f"Creating location for {outputs['name']}, metaName={meta_name}")
                    outputs['value'] = meta.create_output_meta_obj(item_type, meta_name)
                else:
                    raise Exception("value 中需要有metaName")

        with open(f'{self.resolved_dir}/resolved_outputs.json', 'w') as f:
            json.dump(outputs_params, f, indent=4)
            logging.info("Resolved inputs params:\n%s", json.dumps(outputs_params, indent=4))

    def resolve_storage_service_config_files(self):
        meta = MetaResolver(self.meta_api)
        for ss_id in self.ss_ids:
            logging.info(f'ss_id: {ss_id}')
            dir_path = f'{self.runtimed_dir}/hadoop/{ss_id}'
            os.makedirs(dir_path, mode=0o777, exist_ok=True)
            hadoop_config, krb5_config = meta.get_service_config(ss_id)
            if hadoop_config:
                # 写入hadoop文件
                for fname, txt in hadoop_config['config_files'].items():
                    logging.info(f"Writing {fname} to {dir_path}")
                    with open(f"{dir_path}/{fname}", 'w') as f:
                        f.write(txt)
            if krb5_config:
                os.makedirs(f'{dir_path}/krb5_conf', mode=0o777, exist_ok=True)
                with open(f'{dir_path}/krb5_conf/krb5.conf', 'w') as f:
                    f.write(krb5_config["config_files"]["krb5.conf"])
                with open(f'{dir_path}/krb5_conf/krb5.keytab', 'wb') as f:
                    f.write(base64.b64decode(krb5_config["config_files"]["keytab_base64"]))

    def resolve_env_file(self):
        output_lines = []
        meta = MetaResolver(self.meta_api)
        for ss_id in self.ss_ids:
            dir_path = f'{self.runtimed_dir}/hadoop/{ss_id}'
            hadoop_config, krb5_config = meta.get_service_config(ss_id)
            if hadoop_config:
                output_lines.append(f'export HADOOP_CONF_DIR={dir_path}')
                output_lines.append(f'export HADOOP_USER_NAME={hadoop_config["hadoop_user_name"]}')
                output_lines.append(f'export SPARK_USER={hadoop_config["hadoop_user_name"]}')
            if krb5_config:
                output_lines.append('export IS_KERBERIZED=true')
            break

        with open(f'{self.runtimed_dir}/ophub-env', 'w') as f:
            f.write('\n'.join(output_lines))
            f.write('\n')

    def resolve_params(self):
        # outputs file
        param_file = f"{self.ophub_data_dir}/params-def.json"
        with open(param_file, 'r') as f:
            operator_def_param = json.load(f, object_hook=remove_nulls)
        operator_param_def_json = _convert_list_to_map(operator_def_param)

        # params instance json
        operator_instance_params_file = f"{self.ophub_data_dir}/params.json"
        with open(operator_instance_params_file, 'r') as f:
            operator_is_params_json = json.load(f, object_hook=remove_nulls)

        if operator_is_params_json is not None:
            parma_params = _merge_op_op_instance(operator_is_params_json, operator_param_def_json)
            meta = MetaResolver(self.meta_api)
            for item in parma_params:
                item_type = item['format']
                if item_type.lower() == 'workdir':
                    if Metastore.Name.value in item['value']:
                        meta_name = item['value'][Metastore.Name.value]
                        logging.info(f"Creating location for {item['name']}, metaName={meta_name}")
                        item['value'] = meta.create_output_meta_obj(item_type, meta_name)
                    else:
                        raise Exception("value 中需要有metaName")
        else:
            logging.warning("params.json is null")

    def resolve_all(self):
        logging.info("Start to resolve inputs..")
        self.resolve_inputs_file()
        self.resolve_storage_service_config_files()
        self.resolve_env_file()
        logging.info("Start to resolve outputs..")
        self.resolve_outputs_file()
        logging.info("Start to resolve params..")
        self.resolve_params()


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')
    op = OperatorAdapter()
    op.resolve_all()
