import base64
import logging
import os
from logging import getLogger
from typing import Any, Dict

LOGGER = getLogger(__name__)


from pandas_iris_02.hooks.metaxis_client import MetaxisClient


class HadoopConfTool():
    """ HadoopConfTool
        convert openmetaxis storageServices OSS configuration to local file
    """
    def generate_hadoop_conf(self,metaxis_endpoint:str, ss_fqn:str, hadoop_conf_dir:str) -> None:
        
        metaxis_client = MetaxisClient(metaxis_endpoint) 
        response = metaxis_client.get_storage_service_by_fqn(ss_fqn)
        self._logger.info(f"get storage service response:{response}")
        self._write_data_to_local(response._meta_extra,hadoop_conf_dir)


    @property
    def _logger(self):
        return logging.getLogger(__name__)

    def _write_data_to_local(self,meta_extra, hadoop_conf_dir:str):
        # metaExtra is a slice
        # if store hdfs-connection it will only one item 
        if len(meta_extra) == 0:
            LOGGER.warn("openmetaxis storage service extra configuration is empty")
            return
        
        item = meta_extra[0]
        config_data = item.v
        hadoop_user_name = config_data['hadoopUserName']
        hadoop_conf = config_data['hadoop_conf']
        kerberos_conf = config_data['kerberos_conf']

        for k,v in hadoop_conf.items():
            with open(f"{hadoop_conf_dir}/{k}", 'w') as f:
                f.write(v)
        
        if kerberos_conf != None:   
            # kerberos 
            os.makedirs(f'{hadoop_conf_dir}/krb5_conf', mode=0o777, exist_ok=True)
            for k,v in kerberos_conf.items():
                if k == 'principal':
                    os.environ['PRINCIPAL'] = v
                if k == 'keytab':
                    v = base64.b64decode(v)
                    with open(f"{hadoop_conf_dir}/krb5_conf/{k}", 'wb') as f:
                        f.write(v)
                if k == 'krb5.conf':
                    with open(f"{hadoop_conf_dir}/krb5_conf/{k}", 'w') as f:
                        f.write(v)

