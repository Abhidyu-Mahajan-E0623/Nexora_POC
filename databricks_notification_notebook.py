# Databricks Notebook: Pipeline Gatekeeper & Notifications
# -------------------------------------------------------------------------
# This notebook handles the logic for Task 2 (Gatekeeper) in your pipeline.
# It checks with the FastAPI backend for any detected anomalies.
# -------------------------------------------------------------------------

import requests
import json
from datetime import datetime

# =========================================================================
# 1. CONFIGURATION
# =========================================================================
# The URL of your FastAPI server hosted on Render
RENDER_API_URL = "https://your-app.onrender.com"

# SendGrid Configuration (for the alert email)
SENDGRID_API_KEY = "YOUR_SENDGRID_API_KEY"
SENDER_EMAIL = "aakash.lal@procdna.com"
RECIPIENTS = ["naincy.saxena@procdna.com", "vaibhav.maheshwari@procdna.com", "aakash.lal@procdna.com"]

# Dashboard URL for the "Open Dashboard" button
DASHBOARD_URL = "https://your-app.onrender.com/"

# =========================================================================
# 2. HELPER: SEND EMAIL ALERT
# =========================================================================
def send_data_quality_alert(run_id, total_anomalies, environment="Production"):
    timestamp = datetime.now().strftime("%B %d, %Y at %H:%M:%S UTC")
    incident_id = f"DQ-{datetime.now().strftime('%Y%m%d')}-{run_id}"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background:#f5f5f5;font-family:sans-serif;">
        <div style="max-width:700px;margin:0 auto;background:#ffffff;border-top:6px solid #b91c1c;">
            <div style="background:#1e293b;color:white;padding:30px 40px">
                <h1 style="margin:0;font-size:22px;">Pipeline Execution Halted</h1>
                <p style="margin:5px 0 0;color:#94a3b8;font-size:12px;text-transform:uppercase;">Data Quality Alert</p>
            </div>
            <div style="padding:30px 40px">
                <div style="background:#fef2f2;border:1px solid #fecaca;padding:20px;margin-bottom:25px;border-radius:8px;">
                    <h3 style="margin:0 0 8px;color:#991b1b;">{total_anomalies} Anomalies Detected in Bronze Layer</h3>
                    <p style="margin:0;font-size:14px;color:#7f1d1d;">
                        The automated data quality gate has identified anomalies. 
                        Downstream processing to Silver has been suspended pending review.
                    </p>
                </div>
                <table style="width:100%;border-collapse:collapse;margin-bottom:25px;font-size:13px;">
                    <tr><td style="padding:10px;background:#f8fafc;border:1px solid #e2e8f0;width:30%;">Run ID</td><td style="padding:10px;border:1px solid #e2e8f0;font-family:monospace;">{run_id}</td></tr>
                    <tr><td style="padding:10px;background:#f8fafc;border:1px solid #e2e8f0;">Detected At</td><td style="padding:10px;border:1px solid #e2e8f0;">{timestamp}</td></tr>
                </table>
                <div style="text-align:center;">
                    <a href="{DASHBOARD_URL}" style="display:inline-block;background:#2563eb;color:white;padding:14px 32px;text-decoration:none;border-radius:6px;font-weight:600;">Open Monitoring Dashboard →</a>
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
            "from": {"email": SENDER_EMAIL, "name": "Nexora Alerts"},
            "subject": f"🚨 [Action Required] Data Quality Alert - Pipeline Halted | {incident_id}",
            "content": [{"type": "text/html", "value": html}]
        }
    )

# =========================================================================
# 3. PART A: SUBMIT REPORT (USE THIS IN TASK 1)
# =========================================================================
# After running your anomaly detection scan in Task 1, use this to upload the report to Render:
def upload_report_to_render(report_dict):
    try:
        resp = requests.post(f"{RENDER_API_URL}/api/report", json={"report_data": report_dict})
        if resp.status_code == 200:
            print("Successfully uploaded report to Render.")
        else:
            print(f"Failed to upload report: {resp.text}")
    except Exception as e:
        print(f"Error connecting to Render: {e}")

# =========================================================================
# 4. PART B: GATEKEEPER CHECK (USE THIS IN TASK 2)
# =========================================================================
def run_gatekeeper_check():
    try:
        print(f"Checking gatekeeper status at {RENDER_API_URL}...")
        resp = requests.get(f"{RENDER_API_URL}/api/gatekeeper")
        
        if resp.status_code == 200:
            report = resp.json()
            
            # If the response is NOT empty, it means anomalies were found
            if report and report.get("total_anomalies", 0) > 0:
                print(f"!!!! ANOMALIES DETECTED: {report['total_anomalies']} found !!!!")
                
                # 1. Send the Alert Email
                send_data_quality_alert(report.get("run_id", "unknown"), report["total_anomalies"])
                
                # 2. Halt the Pipeline (Pause)
                # In Databricks, this will fail the notebook and stop downstream tasks
                raise Exception(f"Pipeline Halted: {report['total_anomalies']} anomalies detected in Bronze.")
            else:
                print("No anomalies detected. Proceeding to next task.")
        else:
            print(f"Warning: Gatekeeper API returned {resp.status_code}. Proceeding with caution.")
            
    except Exception as e:
        # If the API is down or errors, we typically halt or alert
        if "Pipeline Halted" in str(e):
            raise e
        print(f"Gatekeeper error: {e}")

# =========================================================================
# MAIN EXECUTION
# =========================================================================
# In Task 2 (Gatekeeper Notebook), just run this:
run_gatekeeper_check()
