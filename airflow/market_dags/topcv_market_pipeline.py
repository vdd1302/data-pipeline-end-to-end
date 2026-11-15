"""Create one daily batch, run bounded slices and evaluate usable committed data."""
from datetime import timedelta
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

BASE = 'cd /opt/airflow/project && python -m scripts.run_market_pipeline '
REPORT_ENV = {
    'PIPELINE_REPORT_DIR': '/opt/airflow/logs/pipeline-reports/{{ dag.dag_id }}/{{ run_id }}',
    'PIPELINE_DAG_ID': '{{ dag.dag_id }}',
    'PIPELINE_RUN_ID': '{{ run_id }}',
    'PIPELINE_BATCH_DATE': '{{ data_interval_end.in_timezone("Asia/Ho_Chi_Minh").strftime("%Y-%m-%d") if (data_interval_end is defined and data_interval_end) else "" }}',
}
with DAG(
    dag_id='topcv_market_pipeline',
    start_date=pendulum.datetime(2026, 11, 7, tz='Asia/Ho_Chi_Minh'),
    schedule='0 19 * * *', catchup=False, max_active_runs=1, max_active_tasks=1,
    is_paused_upon_creation=True,
    default_args={'owner': 'topcv-market', 'retries': 0, 'execution_timeout': timedelta(minutes=40)},
    tags=['topcv', 'daily-coordinator', 'partial-collection', 'source-access-gate'],
) as dag:
    seed = BashOperator(task_id='create_daily_batch',
        bash_command=BASE + 'create-batch --report "$PIPELINE_REPORT_DIR/batch.json"',
        env=REPORT_ENV, append_env=True)
    crawl = BashOperator(task_id='crawl_scopes',
        bash_command=BASE + 'crawl --batch-report "$PIPELINE_REPORT_DIR/batch.json" --report "$PIPELINE_REPORT_DIR/source.json" ${TOPCV_AIRFLOW_CRAWL_ARGS:-}',
        env=REPORT_ENV, append_env=True, skip_on_exit_code=75, pool='topcv_source', pool_slots=1)
    readiness = BashOperator(task_id='evaluate_data_readiness',
        bash_command=BASE + 'readiness --batch-report "$PIPELINE_REPORT_DIR/batch.json" --source-report "$PIPELINE_REPORT_DIR/source.json" --report "$PIPELINE_REPORT_DIR/readiness.json"',
        env=REPORT_ENV, append_env=True, trigger_rule='all_done')
    warehouse = BashOperator(task_id='refresh_warehouse',
        bash_command=BASE + 'refresh --batch-report "$PIPELINE_REPORT_DIR/batch.json" --source-report "$PIPELINE_REPORT_DIR/source.json" --readiness-report "$PIPELINE_REPORT_DIR/readiness.json" --report "$PIPELINE_REPORT_DIR/warehouse.json"',
        env=REPORT_ENV, append_env=True, retries=2, retry_delay=timedelta(minutes=3))
    quality = BashOperator(task_id='evaluate_quality',
        bash_command=BASE + 'quality --batch-report "$PIPELINE_REPORT_DIR/batch.json" --report "$PIPELINE_REPORT_DIR/quality.json"',
        env=REPORT_ENV, append_env=True)
    report = BashOperator(task_id='evaluate_pipeline_status',
        bash_command=BASE + 'evaluate --batch-report "$PIPELINE_REPORT_DIR/batch.json" --source-report "$PIPELINE_REPORT_DIR/source.json" --readiness-report "$PIPELINE_REPORT_DIR/readiness.json" --warehouse-report "$PIPELINE_REPORT_DIR/warehouse.json" --quality-report "$PIPELINE_REPORT_DIR/quality.json" --report "$PIPELINE_REPORT_DIR/result.json"',
        env=REPORT_ENV, append_env=True, trigger_rule='all_done')
    # Exit code 4 turns the run red when the source is blocked/stale, so a halted
    # pipeline is visible without anyone reading logs.
    health = BashOperator(task_id='health_check',
        bash_command=BASE + 'health --report "$PIPELINE_REPORT_DIR/health.json"',
        env=REPORT_ENV, append_env=True, trigger_rule='all_done')
    report >> health
    seed >> crawl >> readiness >> warehouse >> quality >> report
    seed >> readiness
    crawl >> report
