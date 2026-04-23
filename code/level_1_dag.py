# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum # Usar pendulum es el estándar moderno en Airflow

args = {
    'owner': 'mad-developer',
}

with DAG(
    dag_id='hello_world_airflow',
    default_args=args,
    schedule='0 5 * * *',
    # pendulum.today('UTC').subtract(days=1) es el equivalente a days_ago(1)
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"), 
    catchup=False,
) as dag:

    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo Hello',
    )

    print_world = BashOperator(
        task_id='print_world',
        bash_command='echo World',
    )

    print_hello >> print_world