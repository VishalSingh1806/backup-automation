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

# ─── Models ────────────────────────────────────────────────────────────────────
class AuditRequest(BaseModel):
    user_email: str

class TransferRequest(BaseModel):
    from_email: str
    to_email: str
    transfer_type: str  # 'backup' or 'direct'

class ReplaceShareRequest(BaseModel):
    from_email: str
    to_email: str

class SingleFileTransferRequest(BaseModel):
    file_id: str
    from_email: str
    to_email: str

# ─── Helpers ───────────────────────────────────────────────────────────────────
def schedule_file_deletion(path, delay=300):
    def delete_file():
        time.sleep(delay)
        if os.path.exists(path):
            os.remove(path)
    threading.Thread(target=delete_file, daemon=True).start()

def recursively_transfer(service, file_id, to_email, processed, errors):
    """Transfer ownership of a file/folder and all its children."""
    try:
        file = service.files().get(
            fileId=file_id,
            fields="id, name, mimeType"
        ).execute()

        # Transfer this item
        service.permissions().create(
            fileId=file_id,
            body={
                'type': 'user',
                'role': 'owner',
                'transferOwnership': True,
                'emailAddress': to_email
            },
            transferOwnership=True
        ).execute()
        processed.append(file_id)

        # If it's a folder, recurse into its contents
        if file['mimeType'] == 'application/vnd.google-apps.folder':
            page_token = None
            while True:
                children = service.files().list(
                    q=f"'{file_id}' in parents and trashed = false",
                    fields="nextPageToken, files(id)",
                    pageToken=page_token
                ).execute()

                for child in children.get('files', []):
                    recursively_transfer(service, child['id'], to_email, processed, errors)

                page_token = children.get('nextPageToken')
                if not page_token:
                    break

    except Exception as e:
        errors.append({"file_id": file_id, "error": str(e)})

# ─── Endpoints ─────────────────────────────────────────────────────────────────

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

    query = (
        f"('{user_email}' in readers or '{user_email}' in writers) "
        f"and not '{user_email}' in owners and trashed = false"
    )
    results = service.files().list(
        q=query,
        pageSize=100,
        fields="nextPageToken, files(id, name, owners)"
    ).execute()

    files = results.get('files', [])
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["File Name", "File ID", "Owner"])
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

@app.post("/transfer-owned-files")
def transfer_owned_files(request: TransferRequest):
    from_email = request.from_email
    to_email = request.to_email

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
        subject=from_email
    )
    service = build('drive', 'v3', credentials=credentials)

    # Create a new parent folder for everything we move
    folder = service.files().create(
        body={'name': f"Transferred from {from_email}",
              'mimeType': 'application/vnd.google-apps.folder'},
        fields='id'
    ).execute()
    folder_id = folder['id']

    query = (
        f"'{from_email}' in owners and trashed = false "
        f"and mimeType = 'application/vnd.google-apps.folder'"
    )
    processed_count = [0]
    error_log = []

    # Process each top-level folder
    resp = service.files().list(q=query, fields="files(id, parents)").execute()
    for file in resp.get('files', []):
        try:
            # Move into our new parent
            parents = file.get('parents', [])
            if folder_id not in parents:
                service.files().update(
                    fileId=file['id'],
                    addParents=folder_id,
                    removeParents=','.join(parents),
                    fields='id, parents'
                ).execute()

            # Transfer ownership of folder and its contents
            recursively_transfer(service, file['id'], to_email, processed_count, error_log)

        except Exception as e:
            error_log.append({"file_id": file['id'], "error": str(e)})

    return {
        "status": "success",
        "message": f"Transferred owned folders and contents from {from_email} to {to_email}",
        "folder_id": folder_id,
        "files_processed": processed_count[0],
        "errors": error_log
    }

@app.post("/transfer-single-file")
def transfer_single_file(request: SingleFileTransferRequest):
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
        subject=request.from_email
    )
    service = build('drive', 'v3', credentials=credentials)

    processed = []
    errors = []

    # This will handle both a single file or a folder + its nested children
    recursively_transfer(service, request.file_id, request.to_email, processed, errors)

    return {
        "status": "complete",
        "total_transferred": len(processed),
        "errors": errors
    }

@app.post("/replace-shared-user")
def replace_shared_user(request: ReplaceShareRequest):
    from_email = request.from_email
    to_email = request.to_email

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
        subject=from_email
    )
    service = build('drive', 'v3', credentials=credentials)

    query = f"('{from_email}' in readers or '{from_email}' in writers) and trashed = false"
    page_token = None
    updated_count = 0
    skipped_count = 0

    while True:
        resp = service.files().list(
            q=query,
            fields="nextPageToken, files(id, permissions)",
            pageToken=page_token
        ).execute()

        for file in resp.get('files', []):
            perms = file.get('permissions', [])
            from_perm = next((p for p in perms if p.get('emailAddress') == from_email), None)
            if not from_perm:
                skipped_count += 1
                continue
            try:
                service.permissions().create(
                    fileId=file['id'],
                    body={'type': 'user', 'role': from_perm['role'], 'emailAddress': to_email},
                    fields='id'
                ).execute()
                service.permissions().delete(
                    fileId=file['id'], permissionId=from_perm['id']
                ).execute()
                updated_count += 1
            except:
                skipped_count += 1

        page_token = resp.get('nextPageToken')
        if not page_token:
            break

    return {"status": "done", "updated": updated_count, "skipped": skipped_count}

@app.get("/download/{file_id}")
def download_page(file_id: str):
    file_name = f"shared_files_{file_id}.csv"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    html = f"""
    <html><head><script>
      window.onload = function() {{
        const link = document.createElement('a');
        link.href = '/files/{file_name}';
        link.download = '{file_name}';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      }};
    </script></head>
    <body>
      <p>If download doesn’t start automatically, <a href="/files/{file_name}">click here</a>.</p>
    </body></html>
    """
    return HTMLResponse(content=html)

@app.get("/")
def root():
    return {"status": "Drive Audit API is running"}
