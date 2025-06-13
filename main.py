import csv
import os
import threading
import time
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()

# Setup download directory
DOWNLOAD_DIR = "/home/apps/backup-automation/downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Mount static file serving for actual downloads
app.mount("/files", StaticFiles(directory=DOWNLOAD_DIR), name="files")

# Service account setup
SERVICE_ACCOUNT_FILE = "/home/apps/backup-automation/drive-audit-service.json"
SCOPES = ['https://www.googleapis.com/auth/drive']

class AuditRequest(BaseModel):
    user_email: str

class TransferRequest(BaseModel):
    from_email: str
    to_email: str

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

    schedule_file_deletion(file_path, delay=300)

    return {
        "status": "success",
        "file_id": file_id,
        "download_link": f"http://35.202.229.82:8000/download/{file_id}"
    }

@app.post("/transfer-ownership")
def transfer_ownership(request: TransferRequest):
    from_email = request.from_email
    to_email = request.to_email

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
        subject=from_email
    )
    service = build('drive', 'v3', credentials=credentials)

    # Create target folder in to_email's Drive
    to_creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
        subject=to_email
    )
    to_service = build('drive', 'v3', credentials=to_creds)
    folder_metadata = {
        'name': from_email,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    folder = to_service.files().create(body=folder_metadata, fields='id').execute()
    folder_id = folder['id']

    # Transfer files
    query = f"'{from_email}' in owners and trashed = false"
    page_token = None
    while True:
        response = service.files().list(
            q=query,
            spaces='drive',
            fields="nextPageToken, files(id, name, parents)",
            pageToken=page_token
        ).execute()

        for file in response.get('files', []):
            try:
                # Move file into target folder in recipient account
                service.files().update(
                    fileId=file['id'],
                    addParents=folder_id,
                    removeParents=','.join(file.get('parents', [])),
                    fields='id, parents'
                ).execute()

                # Transfer ownership
                service.permissions().create(
                    fileId=file['id'],
                    body={
                        'type': 'user',
                        'role': 'owner',
                        'transferOwnership': True,
                        'emailAddress': to_email
                    },
                    transferOwnership=True
                ).execute()
            except Exception as e:
                print(f"Failed to process {file['id']}: {str(e)}")

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    return {"status": "success", "message": f"Ownership transferred from {from_email} to {to_email} with folder structure."}

@app.get("/download/{file_id}")
def download_page(file_id: str):
    file_name = f"shared_files_{file_id}.csv"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    html = f"""
    <html>
      <head>
        <script>
          window.onload = function() {{
            const link = document.createElement('a');
            link.href = '/files/{file_name}';
            link.download = '{file_name}';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
          }};
        </script>
      </head>
      <body>
        <p>If your file did not start downloading automatically, <a href='/files/{file_name}'>click here</a>.</p>
      </body>
    </html>
    """
    return HTMLResponse(content=html)

@app.get("/")
def root():
    return {"status": "Drive Audit API is running"}
