# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
### DryDock Child Dag
"""
import airflow
from airflow import DAG
from airflow.operators.sensors import HttpSensor
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
import configparser
import json

def sub_dag(parent_dag_name, child_dag_name, args, schedule_interval):
  dag = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name),
    default_args=args,
    start_date=args['start_date'],
    max_active_runs=1,
  )

  # Location of shiyard.conf
  path = '/etc/shipyard/shipyard.conf'

  # Read and parse shiyard.conf
  config = configparser.ConfigParser()
  config.read(path)
  
  # Location of site_yaml and prom_yaml
  site_yaml = config.get('drydock', 'site_yaml')
  prom_yaml =  config.get('drydock', 'prom_yaml')
  
  # Drydock Endpoint
  drydock_api_endpoint = config.get('drydock', 'api_endpoint')
  drydock_design_api = config.get('drydock', 'design_api')
  drydock_task_api = config.get('drydock', 'task_api')
  
  # Drydock Token
  drydock_token = config.get('drydock', 'token')
  
  
  # Add drydock connection to airflow
  add_connection = BashOperator(
      task_id='add_drydock_connection',
      bash_command='airflow connections --add --conn_id drydock_api --conn_uri {{ params.drydock_endpoint }}',
      params={'drydock_endpoint': drydock_api_endpoint},
      dag=dag)
  
  # Check to make sure that the drydock endpoint is reachable
  sensor = HttpSensor(
      task_id='drydock_sensor_check',
      http_conn_id='drydock_api',
      endpoint='',
      poke_interval=5,
      dag=dag)
  
  # Get Design ID
  design_id = BashOperator(
      task_id='design_id',
      bash_command="curl -s -XPOST -H 'X-Auth-Token: {{ params.token }}' {{ params.drydock_design_endpoint }} | jq -cr '.id'",
      params={'drydock_design_endpoint': drydock_design_api, 'token': drydock_token},
      xcom_push=True,
      dag=dag)
  
  # Load site topology and promenade configuration files
  load_site_data = BashOperator(
      task_id='load_site_data',
      bash_command="curl -s -XPOST --data-binary @{{ params.data }} -H 'X-Auth-Token: {{ params.token }}' {{ params.drydock_design_endpoint }}/{{ ti.xcom_pull(task_ids='design_id', key='return_value') }}/parts?ingester=yaml | jq",
      params={'drydock_design_endpoint': drydock_design_api, 'token': drydock_token, 'data': site_yaml},
      dag=dag)
  
  # Create task for prepare site which configures networking in maas and other things
  prepare_site_json = BashOperator(
      task_id='prepare_site_json',
      bash_command='echo `{ "action": "prepare_site", "design_id": "DESIGN_ID", "sitename": "demo" }` | sed -e "s/DESIGN_ID/{{ ti.xcom_pull(task_ids=\'design_id\', key=\'return_value\') }}/" > /tmp/site_task.json',
      dag=dag)
  
  site_task_id = BashOperator(
      task_id='site_task_id',
      bash_command="curl -s -XPOST -H 'X-Auth-Token: {{ params.token }}' -H 'Content-Type: application/json' --data @{{ params.site_json }} {{ params.drydock_task_endpoint }} | jq -cr '.task_id'",
      params={'drydock_task_endpoint': drydock_task_api, 'token': drydock_token, 'site_json': '/tmp/site_task.json'},
      xcom_push=True,
      dag=dag)
  
  # Prepare site
  prepare_site = BashOperator(
      task_id='prepare_site',
      bash_command="curl -H 'X-Auth-Token: {{ params.token }}' {{ params.drydock_task_endpoint }}/{{ ti.xcom_pull(task_ids='site_task_id', key='return_value') }} | jq",
      params={'drydock_task_endpoint': drydock_task_api, 'token': drydock_token},
      dag=dag)
  
  # Create task to prepare nodes
  task_to_prepare_nodes = BashOperator(
      task_id='task_to_prepare_nodes',
      bash_command='echo `{ "action": "prepare_node", "design_id": "DESIGN_ID", "sitename": "demo", "node_filter": { "node_names": "node1,node2" } }` | sed -e "s/DESIGN_ID/{{ ti.xcom_pull(task_ids=\'design_id\', key=\'return_value\') }}/" > /tmp/prepare_task.json',
      dag=dag)
  
  prepare_node_task_id = BashOperator(
      task_id='prepare_node_task_id',
      bash_command="curl -s -XPOST -H 'X-Auth-Token: {{ params.token }}' -H 'Content-Type: application/json' --data @{{ params.task_json }} {{ params.drydock_task_endpoint }} | jq -cr '.task_id'",
      params={'drydock_task_endpoint': drydock_task_api, 'token': drydock_token, 'task_json': '/tmp/prepare_task.json'},
      xcom_push=True,
      dag=dag)
  
  prepare_nodes = BashOperator(
      task_id='prepare_nodes',
      bash_command="curl -H 'X-Auth-Token: {{ params.token }}' {{ params.drydock_task_endpoint }}/{{ ti.xcom_pull(task_ids='prepare_node_task_id', key='return_value') }} | jq",
      params={'drydock_task_endpoint': drydock_task_api, 'token': drydock_token},
      dag=dag)
  
  
  ## Status of machines need to be ready before we can proceed to deploy nodes
  
  # Deploy nodes
  task_to_deploy_nodes = BashOperator(
      task_id='task_to_deploy_nodes',
      bash_command='echo `{ "action": "deploy_node", "design_id": "DESIGN_ID", "sitename": "demo", "node_filter": { "node_names": "node1,node2" } }` | sed -e "s/DESIGN_ID/{{ ti.xcom_pull(task_ids=\'design_id\', key=\'return_value\') }}/" > /tmp/deploy_task.json',
      dag=dag)
  
  deploy_node_task_id = BashOperator(
      task_id='deploy_node_task_id',
      bash_command="curl -s -XPOST -H 'X-Auth-Token: {{ params.token }}' -H 'Content-Type: application/json' --data @{{ params.deploy_json }} {{ params.drydock_task_endpoint }} | jq -cr '.task_id'",
      params={'drydock_task_endpoint': drydock_task_api, 'token': drydock_token, 'deploy_json': '/tmp/deploy_task.json'},
      xcom_push=True,
      dag=dag)
  
  
  # Define dependencies
  sensor.set_upstream(add_connection)
  design_id.set_upstream(sensor)
  load_site_data.set_upstream(design_id)
  prepare_site_json.set_upstream(load_site_data)
  site_task_id.set_upstream(prepare_site_json)
  prepare_site.set_upstream(site_task_id)
  task_to_prepare_nodes.set_upstream(prepare_site)
  prepare_node_task_id.set_upstream(task_to_prepare_nodes)
  prepare_nodes.set_upstream(prepare_node_task_id)
  task_to_deploy_nodes.set_upstream(prepare_nodes)
  deploy_node_task_id.set_upstream(task_to_deploy_nodes)
  
  return dag
