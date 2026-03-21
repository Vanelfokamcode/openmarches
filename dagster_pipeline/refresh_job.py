from dagster import job, op, schedule, ScheduleDefinition, Definitions
import subprocess

@op
def refresh_openmarches(context):
    """Telecharge DECP, rebuild dbt, push prod DB."""
    context.log.info("Lancement refresh OpenMarches...")
    result = subprocess.run(
        ["python", "ingestion/refresh_pipeline.py"],
        capture_output=True, text=True, cwd="."
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception("Refresh echoue : " + result.stderr)
    context.log.info("Refresh termine")

@job
def openmarches_refresh_job():
    refresh_openmarches()

# Schedule : 1er de chaque mois a 6h du matin
monthly_schedule = ScheduleDefinition(
    job=openmarches_refresh_job,
    cron_schedule="0 6 1 * *",
    name="monthly_decp_refresh"
)

defs = Definitions(
    jobs=[openmarches_refresh_job],
    schedules=[monthly_schedule]
)