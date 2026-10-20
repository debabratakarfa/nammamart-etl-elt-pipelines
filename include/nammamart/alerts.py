"""Failure alerting (bonus). Posts to Slack if the Airflow Variable
SLACK_WEBHOOK_URL is set; otherwise only logs. The webhook URL is a secret:
it lives in an Airflow Variable (or a secrets backend), never in code or git.

    astro dev run variables set SLACK_WEBHOOK_URL https://hooks.slack.com/services/...
"""
from __future__ import annotations

import json
import logging
import urllib.request

from .config import get_var

log = logging.getLogger(__name__)


def on_failure_alert(context) -> None:
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    run_id = context.get("run_id", "?")
    exc = context.get("exception")
    text = (f":red_circle: *{dag_id}* failed at task `{task_id}`\n"
            f"run: `{run_id}`\nerror: `{str(exc)[:300]}`\n"
            f"The 9:00 AM numbers are NOT ready. Check the Airflow UI, then pipeline_runs.")
    url = get_var("SLACK_WEBHOOK_URL")
    if not url:
        log.warning("ALERT (no SLACK_WEBHOOK_URL set): %s", text)
        return
    try:
        req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
    except Exception as err:  # an alert failure must never mask the real error
        log.error("Slack alert failed: %s", err)
