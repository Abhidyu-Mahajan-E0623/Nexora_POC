# Databricks Notebook: Anomaly Notification Alert
# -------------------------------------------------------------------------
# Copy this code into a Databricks Python Notebook.
# This notebook can be added as a task to your Databricks Workflow.
# -------------------------------------------------------------------------

import requests
import json
from datetime import datetime
from pathlib import Path

# =========================================================================
# 1. CONFIGURATION
# =========================================================================
# For production, use Databricks Secrets to fetch these values:
# SENDGRID_API_KEY = dbutils.secrets.get(scope="your_scope", key="sendgrid_api_key")

SENDGRID_API_KEY = "YOUR_SENDGRID_API_KEY"
SENDER_EMAIL = "aakash.lal@procdna.com"
RECIPIENTS = ["naincy.saxena@procdna.com", "vaibhav.maheshwari@procdna.com", "aakash.lal@procdna.com"]

# Update this path to where your anomalies.json is stored (e.g., /dbfs/mnt/...)
ANOMALY_JSON_PATH = "/dbfs/mnt/nexora/Output/Anomaly/latest/anomalies.json"
DASHBOARD_URL = "https://azureapi-instance-gzd2h9dzhafbbcgv.centralus-01.azurewebsites.net/"

# =========================================================================
# 2. EMAIL ALERT FUNCTION
# =========================================================================
def send_data_quality_alert(run_id, total_anomalies, environment="Production"):
    
    timestamp = datetime.now().strftime("%B %d, %Y at %H:%M:%S UTC")
    incident_id = f"DQ-{datetime.now().strftime('%Y%m%d')}-{run_id}"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="margin:0;padding:0;background:#f5f5f5">
        <div style="font-family:'Segoe UI',Arial,sans-serif;max-width:700px;margin:0 auto;background:#ffffff">
            <div style="background:#b91c1c;height:6px"></div>
            <div style="background:linear-gradient(135deg,#1e293b 0%,#334155 100%);color:white;padding:30px 40px">
                <table style="width:100%">
                    <tr>
                        <td>
                            <p style="margin:0;font-size:12px;color:#94a3b8;text-transform:uppercase;letter-spacing:1px">Data Quality Alert</p>
                            <h1 style="margin:10px 0 0;font-size:22px;font-weight:600">Pipeline Execution Halted</h1>
                        </td>
                        <td style="text-align:right;vertical-align:top">
                            <span style="background:#fbbf24;color:#1e293b;padding:6px 14px;border-radius:4px;font-size:12px;font-weight:600">HIGH PRIORITY</span>
                        </td>
                    </tr>
                </table>
            </div>
            
            <div style="background:#f8fafc;padding:15px 40px;border-bottom:1px solid #e2e8f0">
                <table style="width:100%;font-size:13px;color:#64748b">
                    <tr>
                        <td><b>Incident ID:</b> {incident_id}</td>
                        <td style="text-align:center"><b>Environment:</b> {environment}</td>
                        <td style="text-align:right"><b>Detected:</b> {timestamp}</td>
                    </tr>
                </table>
            </div>
            
            <div style="padding:30px 40px">
                <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:20px;margin-bottom:25px">
                    <table style="width:100%">
                        <tr>
                            <td style="width:50px;vertical-align:top">
                                <div style="background:#dc2626;width:40px;height:40px;border-radius:50%;text-align:center;line-height:40px">
                                    <span style="color:white;font-size:20px">!</span>
                                </div>
                            </td>
                            <td style="padding-left:15px">
                                <h3 style="margin:0 0 8px;color:#991b1b;font-size:16px">{total_anomalies} Anomalies Detected in Bronze Layer</h3>
                                <p style="margin:0;color:#7f1d1d;font-size:14px;line-height:1.5">
                                    The automated data quality gate has identified anomalies in the source data. 
                                    Downstream processing has been suspended pending review.
                                </p>
                            </td>
                        </tr>
                    </table>
                </div>

                <div style="text-align:center;margin:30px 0;">
                    <a href="{DASHBOARD_URL}"
                    style="display:inline-block;background-color:#2563eb;color:#ffffff;padding:14px 32px;text-decoration:none;border-radius:6px;font-weight:600;">
                    Open Monitoring Dashboard →
                </a>
            </div>
            </div>
            
            <div style="background:#f8fafc;padding:20px 40px;border-top:1px solid #e2e8f0">
                <p style="text-align:center;color:#94a3b8;font-size:12px;margin:0">Automated Alert • Incident ID: {incident_id}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    subject = f"🚨 [Action Required] Data Quality Alert - Pipeline Halted | {incident_id}"
    
    response = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"},
        json={
            "personalizations": [{"to": [{"email": e} for e in RECIPIENTS]}],
            "from": {"email": SENDER_EMAIL, "name": "Data Quality alerts"},
            "subject": subject,
            "content": [{"type": "text/html", "value": html}]
        }
    )
    
    if response.status_code in [200, 202]:
        print(f"✅ Alert dispatched successfully: {incident_id}")
    else:
        print(f"❌ Dispatch failed: {response.text}")

# =========================================================================
# 3. EXECUTION LOGIC
# =========================================================================
try:
    with open(ANOMALY_JSON_PATH, "r") as f:
        data = json.load(f)
        total_anomalies = data.get("total_anomalies", 0)
        run_id = data.get("run_id", "unknown")
        
        if total_anomalies > 0:
            send_data_quality_alert(run_id, total_anomalies)
        else:
            print("No anomalies detected. Skipping notification.")

except FileNotFoundError:
    print(f"Error: Anomaly report not found at {ANOMALY_JSON_PATH}")
except Exception as e:
    print(f"Unexpected error: {e}")
