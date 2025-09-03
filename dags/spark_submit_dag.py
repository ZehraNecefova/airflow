from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

with DAG(
    dag_id="spark_submit_people",
    start_date=datetime(2025, 9, 3),
    schedule="*/10 * * * *",  # every 10 minutes
    catchup=False,
    tags=["spark", "bash"],
) as dag:

    # Start marker
    task_started = EmptyOperator(
        task_id="task_started"
    )

    # Spark submit task
    spark_submit_task = BashOperator(
        task_id="spark_submit_job",
        bash_command="""
        spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.hadoop.fs.s3a.access.key=telcoaz \
        --conf spark.hadoop.fs.s3a.secret.key=Telco12345 \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
	--conf spark.eventlog.enabled=false \
        /opt/airflow/dags/hello_spark.py
        """
    )

    # End marker
    finished = EmptyOperator(
        task_id="finished"
    )

    # Define task order
    task_started >> spark_submit_task >> finished
