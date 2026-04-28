from dagster import schedule, RunRequest, define_asset_job

daily_job = define_asset_job(name="daily_job")

@schedule(cron_schedule="0 17 * * *", job=daily_job)  # every day at 5:00 PM
def daily_schedule(context):
    return RunRequest()