import logging
from datetime import timedelta

from airflow.decorators import dag
from airflow.models import Variable, Connection
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from custom_notifier import send_slack_notification
from custom_operators import LatestImageOperator, RenderYamlOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'Rez',
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
}


@dag(
    "sample",
    default_args=default_args,
    schedule_interval="0/5 * * * *",
    max_active_runs=1,
    catchup=True,
    on_failure_callback=send_slack_notification
)
def sample():
    latest_image_task_id = 'spark_latest_image'
    database_connection = Connection.get_connection_from_secrets(conn_id='database')

    spark_values = {
        'name': 'example-job',
        'image': 'gitlab.com/data/project/spark:{latest_tag}',
        'mainApplicationFile': 'local:///opt/spark/spark_applications/example_pyspark_code.py',
        'sparkVersion': '3.5.0',
        'spark_jars': 'com.github.housepower:clickhouse-native-jdbc-shaded:2.7.1',  # and other jars
        'driver_cores': 2,
        'driver_memory': '4g',
        'executor_cores': 2,
        'executor_memory': '4g',
        'executor_instance': 2,
        'env_vars': {
            "DATABASE_USER": database_connection.login,
            "DATABASE_PASSWORD": database_connection.password,
            "DATABASE_DATABASE": database_connection.schema,
            "DATABASE_HOST": database_connection.host,
            "DATABASE_PORT": database_connection.port
        }
    }

    latest_image = LatestImageOperator(
        task_id=latest_image_task_id
    )

    render_yaml = RenderYamlOperator(
        task_id="render-yaml-operator",
        values=spark_values,
        latest_image_task_id='get-latest-spark-image',
        template_path="spark-application.j2",
        xcom_key='example-pyspark-yaml',
    )

    submit = SparkKubernetesOperator(
        task_id='spark-order-product-wt',
        namespace='data-spark',
        application_file="{{ task_instance.xcom_pull(task_ids='render-yaml-operator', key='order-product-wt-yaml') }}",
        kubernetes_conn_id='kubernetes-spark',
        do_xcom_push=False,
        get_logs=True
    )

    latest_image >> render_yaml >> submit


dag = sample()
