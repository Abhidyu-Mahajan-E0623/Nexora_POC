# Databricks Notebook: Pipeline Gatekeeper & Notifications
# -------------------------------------------------------------------------
# This notebook handles the logic for Task 2 (Gatekeeper) in your pipeline.
# It supports both SCHEMA validation (pre-ingestion) and DATA validation (post-silver).
# -------------------------------------------------------------------------

import requests
import json
from datetime import datetime

# =========================================================================
# 1. CONFIGURATION
# =========================================================================
# The URL of your FastAPI server hosted on Render
RENDER_API_URL = "https://nexora-anomaly-dashboard.onrender.com"
DASHBOARD_URL = "https://nexora-anomaly-dashboard.onrender.com/"
NEXORA_API_TOKEN = "abhidyu_made_this_code_by_himself_nexora_secure_2026"

# SendGrid Configuration
SENDGRID_API_KEY = "YOUR_SENDGRID_API_KEY"
SENDER_EMAIL = "aakash.lal@procdna.com"
RECIPIENTS = ["naincy.saxena@procdna.com", "vaibhav.maheshwari@procdna.com", "aakash.lal@procdna.com"]

# =========================================================================
# 2. EMAIL NOTIFICATION FUNCTIONS
# =========================================================================
def send_alert_email(report, type="Anomaly"):
    """Sends an alert email via SendGrid."""
    timestamp = datetime.now().strftime("%B %d, %Y at %H:%M:%S UTC")
    run_id = report.get("run_id", "unknown")
    total = report.get("total_anomalies", 0)
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background:#f5f5f5;font-family:sans-serif;">
        <div style="max-width:700px;margin:0 auto;background:#ffffff;border-top:6px solid #b91c1c;">
            <div style="background:#1e293b;color:white;padding:30px 40px">
                <h1 style="margin:0;font-size:22px;">Pipeline Execution Halted</h1>
                <p style="margin:5px 0 0;color:#94a3b8;font-size:12px;text-transform:uppercase;">{type} Quality Alert</p>
            </div>
            <div style="padding:30px 40px">
                <div style="background:#fef2f2;border:1px solid #fecaca;padding:20px;margin-bottom:25px;border-radius:8px;">
                    <h3 style="margin:0 0 8px;color:#991b1b;">{total} {type} Issues Detected</h3>
                    <p style="margin:0;font-size:14px;color:#7f1d1d;">
                        The automated gatekeeper has identified critical {type} issues. 
                        Pipeline execution has been suspended.
                    </p>
                </div>
                <div style="text-align:center;">
                    <a href="{DASHBOARD_URL}" style="display:inline-block;background:#2563eb;color:white;padding:14px 32px;text-decoration:none;border-radius:6px;font-weight:600;">Open Dashboard →</a>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    
    requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"},
        json={
            "personalizations": [{"to": [{"email": e} for e in RECIPIENTS]}],
            "from": {"email": SENDER_EMAIL, "name": "Nexora Monitor"},
            "subject": f"🚨 [Action Required] {type} Alert | {run_id}",
            "content": [{"type": "text/html", "value": html}]
        }
    )

def send_success_email(run_id):
    """Sends a success email at the end of the pipeline."""
    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background:#f5f5f5;font-family:sans-serif;">
        <div style="max-width:700px;margin:0 auto;background:#ffffff;border-top:6px solid #10b981;">
            <div style="background:#1e293b;color:white;padding:30px 40px">
                <h1 style="margin:0;font-size:22px;">Pipeline Completed Successfully</h1>
                <p style="margin:5px 0 0;color:#94a3b8;font-size:12px;text-transform:uppercase;">Execution Report</p>
            </div>
            <div style="padding:30px 40px">
                <p style="color:#1e293b;font-size:16px;">The <b>Bronze to Gold</b> pipeline has completed all stages without critical errors.</p>
                <p style="color:#64748b;font-size:14px;">Run ID: {run_id}</p>
            </div>
        </div>
    </body>
    </html>
    """
    requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"},
        json={
            "personalizations": [{"to": [{"email": e} for e in RECIPIENTS]}],
            "from": {"email": SENDER_EMAIL, "name": "Nexora Monitor"},
            "subject": f"✅ Pipeline Completed Successfully | {run_id}",
            "content": [{"type": "text/html", "value": html}]
        }
    )

# =========================================================================
# 3. UPLOAD FUNCTIONS (FOR INGESTION NOTEBOOKS)
# =========================================================================
def upload_schema_report(report_dict):
    headers = {"X-API-Key": NEXORA_API_TOKEN}
    requests.post(f"{RENDER_API_URL}/api/report/schema", json={"report_data": report_dict}, headers=headers)

def upload_data_report(report_dict):
    headers = {"X-API-Key": NEXORA_API_TOKEN}
    requests.post(f"{RENDER_API_URL}/api/report/data", json={"report_data": report_dict}, headers=headers)

# =========================================================================
# 4. GATEKEEPER FUNCTIONS
# =========================================================================
def check_schema_gatekeeper():
    resp = requests.get(f"{RENDER_API_URL}/api/gatekeeper/schema")
    if resp.status_code == 200:
        report = resp.json()
        if report and report.get("schema_anomalies", 0) > 0:
            send_alert_email(report, type="Schema")
            raise Exception("Pipeline Halted: Schema Drift Detected.")
    print("Schema OK. Proceeding.")

def check_data_gatekeeper():
    resp = requests.get(f"{RENDER_API_URL}/api/gatekeeper/data")
    if resp.status_code == 200:
        report = resp.json()
        if report and report.get("data_anomalies", 0) > 0:
            send_alert_email(report, type="Data")
            raise Exception("Pipeline Halted: Data Quality Issues Detected.")
    print("Data OK. Proceeding.")

# =========================================================================
# EXAMPLE USAGE
# =========================================================================
# To check schema before ingestion:
# check_schema_gatekeeper()

# To check data quality after silver:
# check_data_gatekeeper()

# To send success mail at the end:
# send_success_email("your_run_id")
