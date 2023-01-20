from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook # gives some properties and methods that all hooks in ariflow shares

from elasticsearch import Elasticsearch

class ElasticHook(BaseHook):

    def __init__(self, conn_id='elastic_default', *args, **kwargs):
        super().__init__(*args, **kwargs) # initialize the base hook class
        conn = self.get_connection(conn_id) # get the connection from the connection id

        conn_config = {}
        hosts = []

        if conn.host:
            hosts = conn.host.split(',')
        if conn.port:
            conn_config['port'] = int(conn.port)
        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)

        self.es = Elasticsearch(hosts, **conn_config)
        self.index = conn.schema

    def info(self):
        return self.es.info()
    
    def set_index(self, index):
        self.index = index
    
    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es(index=index, doc_type=doc_type, body=doc)
        return res

class AirflowElasticPlugin(AirflowPlugin):
    name = "elastic_hook"
    hooks = [ElasticHook]