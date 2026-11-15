"""Ingest committed eligible Raw at :20/:50, independently from source access."""
from datetime import timedelta
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

BASE = 'cd /opt/airflow/project && python -m scripts.run_market_pipeline '
REPORT_ENV = {
    'PIPELINE_REPORT_DIR': '/opt/airflow/logs/pipeline-reports/{{ dag.dag_id }}/{{ run_id }}',
    'PIPELINE_DAG_ID': '{{ dag.dag_id }}',
    'PIPELINE_RUN_ID': '{{ run_id }}',
}
with DAG(
    dag_id='topcv_warehouse_recovery',
    start_date=pendulum.datetime(2026, 11, 8, tz='Asia/Ho_Chi_Minh'),
    schedule='20,50 * * * *', catchup=False, max_active_runs=1, max_active_tasks=1,
    is_paused_upon_creation=True,
    default_args={'owner': 'topcv-market', 'retries': 0, 'execution_timeout': timedelta(minutes=60)},
    tags=['topcv', 'warehouse', 'committed-data', 'no-source-network'],
) as dag:
    readiness = BashOperator(task_id='evaluate_data_readiness',
        bash_command=BASE + 'readiness --report "$PIPELINE_REPORT_DIR/readiness.json"',
        env=REPORT_ENV, append_env=True)
    warehouse = BashOperator(task_id='refresh_warehouse',
        bash_command=BASE + 'refresh --readiness-report "$PIPELINE_REPORT_DIR/readiness.json" --report "$PIPELINE_REPORT_DIR/warehouse.json"',
        env=REPORT_ENV, append_env=True, retries=2, retry_delay=timedelta(minutes=3))
    quality = BashOperator(task_id='evaluate_quality',
        bash_command=BASE + 'quality --report "$PIPELINE_REPORT_DIR/quality.json"',
        env=REPORT_ENV, append_env=True)
    report = BashOperator(task_id='evaluate_pipeline_status',
        bash_command=BASE + 'evaluate --readiness-report "$PIPELINE_REPORT_DIR/readiness.json" --warehouse-report "$PIPELINE_REPORT_DIR/warehouse.json" --quality-report "$PIPELINE_REPORT_DIR/quality.json" --report "$PIPELINE_REPORT_DIR/result.json"',
        env=REPORT_ENV, append_env=True, trigger_rule='all_done')
    readiness >> warehouse >> quality >> report
