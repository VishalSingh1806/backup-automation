import csv
import os
import threading
import time
import uuid
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
import logging
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI(title="Google Drive Audit API", version="1.0.0")

# Job tracking for async operations
audit_jobs = {}

# Configure CORS to allow requests from Google Apps Script
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (Google Apps Script)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers including Authorization
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "./drive-audit-service.json")
API_BEARER_TOKEN = os.getenv("API_BEARER_TOKEN")
EXTERNAL_BASE_URL = os.getenv("EXTERNAL_BASE_URL")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

if os.path.exists(DOWNLOAD_DIR):
    app.mount("/files", StaticFiles(directory=DOWNLOAD_DIR), name="files")
else:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    app.mount("/files", StaticFiles(directory=DOWNLOAD_DIR), name="files")
security = HTTPBearer(auto_error=False)
SCOPES = ['https://www.googleapis.com/auth/drive']

def verify_bearer_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not API_BEARER_TOKEN:
        return True
    if not credentials:
        raise HTTPException(status_code=401, detail="Bearer token required")
    if credentials.credentials != API_BEARER_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")
    return True

class AuditRequest(BaseModel):
    user_email: EmailStr

class TransferRequest(BaseModel):
    from_email: EmailStr
    to_email: EmailStr
    transfer_type: str = "direct"

class ReplaceShareRequest(BaseModel):
    from_email: EmailStr
    to_email: EmailStr

class SingleFileTransferRequest(BaseModel):
    file_id: str
    from_email: EmailStr
    to_email: EmailStr

def schedule_file_deletion(path, delay=300):
    def delete_file():
        try:
            time.sleep(delay)
            if os.path.exists(path):
                os.remove(path)
                logger.info(f"Deleted temporary file: {path}")
        except Exception as e:
            logger.error(f"Failed to delete file {path}: {e}")
    threading.Thread(target=delete_file, daemon=True).start()

def get_drive_service(user_email):
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES, subject=user_email
        )
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Failed to create Drive service for {user_email}: {e}")
        raise HTTPException(status_code=500, detail=f"Authentication failed: {str(e)}")

def recursively_transfer(service, file_id, to_email, processed, errors):
    try:
        file = service.files().get(fileId=file_id, fields="id, name, mimeType").execute()
        
        service.permissions().create(
            fileId=file_id,
            body={'type': 'user', 'role': 'owner', 'transferOwnership': True, 'emailAddress': to_email},
            transferOwnership=True
        ).execute()
        processed.append(file_id)
        logger.info(f"Transferred {file['name']} to {to_email}")

        if file['mimeType'] == 'application/vnd.google-apps.folder':
            page_token = None
            while True:
                children = service.files().list(
                    q=f"'{file_id}' in parents and trashed = false",
                    fields="nextPageToken, files(id)", pageToken=page_token
                ).execute()
                
                for child in children.get('files', []):
                    recursively_transfer(service, child['id'], to_email, processed, errors)
                
                page_token = children.get('nextPageToken')
                if not page_token:
                    break
    except Exception as e:
        logger.error(f"Transfer failed for {file_id}: {e}")
        errors.append({"file_id": file_id, "error": str(e)})

@app.get("/")
def root():
    return {"status": "healthy", "service": "Google Drive Audit API"}

@app.get("/health")
def health_check():
    checks = {
        "service_account_file": os.path.exists(SERVICE_ACCOUNT_FILE),
        "download_directory": os.path.exists(DOWNLOAD_DIR),
        "bearer_token_configured": bool(API_BEARER_TOKEN)
    }
    return {
        "status": "healthy" if all(checks.values()) else "unhealthy",
        "checks": checks,
        "timestamp": datetime.now().isoformat()
    }

def process_audit_async(job_id, user_email, base_url):
    """Background function to process audit"""
    try:
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        file_id = f"{user_email.replace('@', '_')}_{timestamp}"
        file_name = f"shared_files_{file_id}.csv"
        file_path = os.path.join(DOWNLOAD_DIR, file_name)

        audit_jobs[job_id]["status"] = "processing"
        audit_jobs[job_id]["file_id"] = file_id

        service = get_drive_service(user_email)
        query = f"('{user_email}' in readers or '{user_email}' in writers) and not '{user_email}' in owners and trashed = false"
        total_files = 0

        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["File Name", "File ID", "Owner", "Type", "Last Modified"])
            page_token = None

            while True:
                results = service.files().list(
                    q=query, pageSize=1000,
                    fields="nextPageToken, files(id, name, owners, mimeType, modifiedTime)",
                    pageToken=page_token
                ).execute()

                for file in results.get('files', []):
                    writer.writerow([
                        file.get('name', ''),
                        file.get('id', ''),
                        (file.get('owners') or [{}])[0].get('emailAddress', ''),
                        file.get('mimeType', ''),
                        file.get('modifiedTime', '')
                    ])
                    total_files += 1

                # Update progress
                audit_jobs[job_id]["files_processed"] = total_files

                page_token = results.get('nextPageToken')
                if not page_token:
                    break

        schedule_file_deletion(file_path, delay=300)

        audit_jobs[job_id]["status"] = "completed"
        audit_jobs[job_id]["total_files"] = total_files
        audit_jobs[job_id]["download_link"] = f"{base_url}/download/{file_id}"
        audit_jobs[job_id]["completed_at"] = datetime.now().isoformat()

        logger.info(f"Audit completed for {user_email}: {total_files} files")
    except Exception as e:
        logger.error(f"Audit failed for {user_email}: {e}")
        audit_jobs[job_id]["status"] = "failed"
        audit_jobs[job_id]["error"] = str(e)

@app.post("/audit-user")
def audit_user(request: AuditRequest, http_request: Request, _: bool = Depends(verify_bearer_token)):
    try:
        user_email = request.user_email
        job_id = str(uuid.uuid4())

        base_url = EXTERNAL_BASE_URL.rstrip('/') if EXTERNAL_BASE_URL else f"{http_request.url.scheme}://{http_request.headers.get('host', http_request.url.netloc)}"

        # Initialize job status
        audit_jobs[job_id] = {
            "status": "queued",
            "user_email": user_email,
            "files_processed": 0,
            "created_at": datetime.now().isoformat()
        }

        # Start background thread
        thread = threading.Thread(target=process_audit_async, args=(job_id, user_email, base_url), daemon=True)
        thread.start()

        logger.info(f"Audit job {job_id} started for {user_email}")
        return {
            "status": "queued",
            "job_id": job_id,
            "message": "Audit job started. Use /audit-status/{job_id} to check progress."
        }
    except Exception as e:
        logger.error(f"Failed to start audit for {user_email}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start audit: {str(e)}")

@app.get("/audit-status/{job_id}")
def get_audit_status(job_id: str, _: bool = Depends(verify_bearer_token)):
    if job_id not in audit_jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    return audit_jobs[job_id]

@app.post("/transfer-owned-files")
def transfer_owned_files(request: TransferRequest, _: bool = Depends(verify_bearer_token)):
    try:
        service = get_drive_service(request.from_email)
        folder = service.files().create(
            body={'name': f"Transferred from {request.from_email}", 'mimeType': 'application/vnd.google-apps.folder'},
            fields='id'
        ).execute()
        folder_id = folder['id']

        query = f"'{request.from_email}' in owners and trashed = false and mimeType = 'application/vnd.google-apps.folder'"
        processed = []
        errors = []

        resp = service.files().list(q=query, fields="files(id, parents)").execute()
        for file in resp.get('files', []):
            try:
                parents = file.get('parents', [])
                if folder_id not in parents:
                    service.files().update(
                        fileId=file['id'], addParents=folder_id,
                        removeParents=','.join(parents), fields='id, parents'
                    ).execute()
                recursively_transfer(service, file['id'], request.to_email, processed, errors)
            except Exception as e:
                errors.append({"file_id": file['id'], "error": str(e)})

        logger.info(f"Transfer completed: {len(processed)} files, {len(errors)} errors")
        return {
            "status": "success",
            "message": f"Transferred folders from {request.from_email} to {request.to_email}",
            "folder_id": folder_id,
            "files_processed": len(processed),
            "errors": errors
        }
    except Exception as e:
        logger.error(f"Transfer failed: {e}")
        raise HTTPException(status_code=500, detail=f"Transfer failed: {str(e)}")

@app.post("/transfer-single-file")
def transfer_single_file(request: SingleFileTransferRequest, _: bool = Depends(verify_bearer_token)):
    try:
        service = get_drive_service(request.from_email)
        processed = []
        errors = []
        
        recursively_transfer(service, request.file_id, request.to_email, processed, errors)
        
        logger.info(f"Single file transfer: {len(processed)} files transferred")
        return {"status": "complete", "total_transferred": len(processed), "errors": errors}
    except Exception as e:
        logger.error(f"Single file transfer failed: {e}")
        raise HTTPException(status_code=500, detail=f"Transfer failed: {str(e)}")

@app.post("/replace-shared-user")
def replace_shared_user(request: ReplaceShareRequest, _: bool = Depends(verify_bearer_token)):
    try:
        service = get_drive_service(request.from_email)
        query = f"('{request.from_email}' in readers or '{request.from_email}' in writers) and trashed = false"
        page_token = None
        updated_count = 0
        skipped_count = 0
        errors = []

        while True:
            resp = service.files().list(
                q=query, fields="nextPageToken, files(id, permissions)", pageToken=page_token
            ).execute()

            for file in resp.get('files', []):
                perms = file.get('permissions', [])
                from_perm = next((p for p in perms if p.get('emailAddress') == request.from_email), None)
                
                if not from_perm:
                    skipped_count += 1
                    continue

                try:
                    service.permissions().create(
                        fileId=file['id'],
                        body={'type': 'user', 'role': from_perm['role'], 'emailAddress': request.to_email},
                        fields='id'
                    ).execute()
                    service.permissions().delete(fileId=file['id'], permissionId=from_perm['id']).execute()
                    updated_count += 1
                except Exception as e:
                    skipped_count += 1
                    errors.append({"file_id": file['id'], "error": str(e)})
                    logger.warning(f"Failed to replace user on file {file['id']}: {e}")

            page_token = resp.get('nextPageToken')
            if not page_token:
                break

        logger.info(f"User replacement: {updated_count} updated, {skipped_count} skipped")
        return {"status": "complete", "updated": updated_count, "skipped": skipped_count, "errors": errors}
    except Exception as e:
        logger.error(f"User replacement failed: {e}")
        raise HTTPException(status_code=500, detail=f"User replacement failed: {str(e)}")

@app.get("/download/{file_id}")
def download_page(file_id: str):
    file_name = f"shared_files_{file_id}.csv"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found or expired")

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Download {file_name}</title>
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
        <h2>Google Drive Audit Report</h2>
        <p>Download should start automatically.</p>
        <p>If not, <a href="/files/{file_name}">click here</a>.</p>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)