import csv
import os
from datetime import datetime
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()

# Mount downloads directory to serve files
DOWNLOAD_DIR = "/home/apps/backup-automation/downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
app.mount("/downloads", StaticFiles(directory=DOWNLOAD_DIR), name="downloads")

# Google credentials
SERVICE_ACCOUNT_FILE = "/home/apps/backup-automation/drive-audit-service.json"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

class AuditRequest(BaseModel):
    user_email: str

@app.post("/audit-user")
def audit_user(request: AuditRequest):
    user_email = request.user_email
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    file_name = f"shared_files_{user_email.replace('@', '_')}_{timestamp}.csv"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
        subject=user_email
    )

    service = build('drive', 'v3', credentials=credentials)

    query = f"('{user_email}' in readers or '{user_email}' in writers) and not '{user_email}' in owners and trashed = false"
    results = service.files().list(
        q=query,
        pageSize=100,
        fields="nextPageToken, files(id, name, owners)"
    ).execute()

    files = results.get('files', [])
    headers = ["File Name", "File ID", "Owner"]

    with open(file_path, mode='w', newline='', encoding='utf-8') as file_out:
        writer = csv.writer(file_out)
        writer.writerow(headers)
        for file in files:
            writer.writerow([
                file['name'],
                file['id'],
                file['owners'][0]['emailAddress']
            ])

    public_url = f"http://35.202.229.82:8000/downloads/{file_name}"
    return JSONResponse(content={"status": "success", "download_url": public_url})

@app.get("/")
def root():
    return {"status": "Drive Audit API is up"}
