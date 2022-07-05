# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script for extracting BigQuery usage results
"""

import logging
import os
import sqlite3
import uuid

from pyhocon import ConfigFactory
from elasticsearch import Elasticsearch

from databuilder.extractor.bigquery_metadata_extractor import BigQueryMetadataExtractor
from databuilder.extractor.es_last_updated_extractor import EsLastUpdatedExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.publisher.elasticsearch_constants import DASHBOARD_ELASTICSEARCH_INDEX_MAPPING, \
    USER_ELASTICSEARCH_INDEX_MAPPING
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

logging.basicConfig(level=logging.INFO)

credential_path = r"<path-to-json-key>" 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# set env NEO4J_HOST to override localhost
NEO4J_ENDPOINT = f'bolt://{os.getenv("NEO4J_HOST", "<your-Amundsen-Host-IP-here>")}:7687'
neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'


es_host = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_HOST', '<your-Amundsen-Host-IP-here>')
es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)

es = Elasticsearch([
    {'host': es_host, 'port': es_port}
])

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Exception:
        logging.exception('exception')
    return None

def create_last_updated_job():
    # loader saves data to these folders and publisher reads it from here
    tmp_folder = '/var/tmp/amundsen/last_updated_data'
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)

    task = DefaultTask(extractor=EsLastUpdatedExtractor(),
                       loader=FsNeo4jCSVLoader())

    job_config = ConfigFactory.from_dict({
        'extractor.neo4j_es_last_updated.model_class':
            'databuilder.models.neo4j_es_last_updated.Neo4jESLastUpdated',

        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': 'unique_lastupdated_tag',  # should use unique tag here like {ds}
    })

    return DefaultJob(conf=job_config,
                      task=task,
                      publisher=Neo4jCsvPublisher())



# todo: Add a second model
def create_bq_job(metadata_type, gcloud_project):
    tmp_folder = f'/var/tmp/amundsen/{metadata_type}'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    bq_meta_extractor = BigQueryMetadataExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=bq_meta_extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())

    job_config = ConfigFactory.from_dict({
        f'extractor.bigquery_table_metadata.{BigQueryMetadataExtractor.PROJECT_ID_KEY}': gcloud_project,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}': True,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.FORCE_CREATE_DIR}': True,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4j_endpoint,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4j_user,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4j_password,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    return job


def create_es_publisher_sample_job(elasticsearch_index_alias='table_search_index',
                                   elasticsearch_doc_type_key='table',
                                   model_name='databuilder.models.table_elasticsearch_document.TableESDocument',
                                   entity_type='table',
                                   elasticsearch_mapping=None):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_{uuid}`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param entity_type:                Entity type handed to the `Neo4jSearchDataExtractor` class, used to determine
                                       Cypher query to extract data from Neo4j. Defaults to `table`.
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    # elastic search client instance
    elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = '{}_'.format(elasticsearch_doc_type_key) + str(uuid.uuid4())


    job_config = ConfigFactory.from_dict({
        'extractor.search_data.entity_type': entity_type,
        'extractor.search_data.extractor.neo4j.graph_url': neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.model_class': model_name,
        'extractor.search_data.extractor.neo4j.neo4j_auth_user': neo4j_user,
        'extractor.search_data.extractor.neo4j.neo4j_auth_pw': neo4j_password,
        'extractor.search_data.extractor.neo4j.neo4j_encrypted': False,
        'loader.filesystem.elasticsearch.file_path': extracted_search_data_path,
        'loader.filesystem.elasticsearch.mode': 'w',
        'publisher.elasticsearch.file_path': extracted_search_data_path,
        'publisher.elasticsearch.mode': 'r',
        'publisher.elasticsearch.client': elasticsearch_client,
        'publisher.elasticsearch.new_index': elasticsearch_new_index_key,
        'publisher.elasticsearch.doc_type': elasticsearch_doc_type_key,
        'publisher.elasticsearch.alias': elasticsearch_index_alias, })

    # only optionally add these keys, so need to dynamically `put` them
    if elasticsearch_mapping:
        job_config.put('publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY),
                       elasticsearch_mapping)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    return job


if __name__ == "__main__":
    # start table job
    job1 = create_bq_job('bigquery_metadata', '<your-project-name-here>')
    job1.launch()

    # create_last_updated_job().launch()
    

    job_es_table = create_es_publisher_sample_job(
        elasticsearch_index_alias='table_search_index',
        elasticsearch_doc_type_key='table',
        entity_type='table',
        model_name='databuilder.models.table_elasticsearch_document.TableESDocument')
    job_es_table.launch()

    job_es_user = create_es_publisher_sample_job(
        elasticsearch_index_alias='user_search_index',
        elasticsearch_doc_type_key='user',
        model_name='databuilder.models.user_elasticsearch_document.UserESDocument',
        entity_type='user',
        elasticsearch_mapping=USER_ELASTICSEARCH_INDEX_MAPPING)
    job_es_user.launch()

    job_es_dashboard = create_es_publisher_sample_job(
        elasticsearch_index_alias='dashboard_search_index',
        elasticsearch_doc_type_key='dashboard',
        model_name='databuilder.models.dashboard_elasticsearch_document.DashboardESDocument',
        entity_type='dashboard',
        elasticsearch_mapping=DASHBOARD_ELASTICSEARCH_INDEX_MAPPING)
    job_es_dashboard.launch()
