import csv
import os
import threading
import time
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import FileResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()

# Setup download directory
DOWNLOAD_DIR = "/home/apps/backup-automation/downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Service account setup
SERVICE_ACCOUNT_FILE = "/home/apps/backup-automation/drive-audit-service.json"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

class AuditRequest(BaseModel):
    user_email: str

def schedule_file_deletion(path, delay=300):
    def delete_file():
        time.sleep(delay)
        if os.path.exists(path):
            os.remove(path)
    threading.Thread(target=delete_file, daemon=True).start()

@app.post("/audit-user")
def audit_user(request: AuditRequest):
    user_email = request.user_email
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    file_id = f"{user_email.replace('@', '_')}_{timestamp}"
    file_name = f"shared_files_{file_id}.csv"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    # Authenticate
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
        subject=user_email
    )
    service = build('drive', 'v3', credentials=credentials)

    # Query files
    query = f"('{user_email}' in readers or '{user_email}' in writers) and not '{user_email}' in owners and trashed = false"
    results = service.files().list(
        q=query,
        pageSize=100,
        fields="nextPageToken, files(id, name, owners)"
    ).execute()

    files = results.get('files', [])
    headers = ["File Name", "File ID", "Owner"]

    # Write to CSV
    with open(file_path, mode='w', newline='', encoding='utf-8') as file_out:
        writer = csv.writer(file_out)
        writer.writerow(headers)
        for file in files:
            writer.writerow([
                file['name'],
                file['id'],
                file['owners'][0]['emailAddress']
            ])

    schedule_file_deletion(file_path, delay=300)  # Auto-delete after 5 mins

    return FileResponse(
        path=file_path,
        filename=file_name,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{file_name}"'}
    )

@app.get("/")
def root():
    return {"status": "Drive Audit API is running"}
