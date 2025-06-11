import csv
from fastapi import FastAPI, Request
from pydantic import BaseModel
from fastapi.responses import FileResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()

SERVICE_ACCOUNT_FILE = r"D:\Admin-workspace\drive-audit-service.json"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

class AuditRequest(BaseModel):
    user_email: str

@app.post("/audit-user")
def audit_user(request: AuditRequest):
    user_email = request.user_email

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
    file_name = f'shared_files_{user_email.replace("@", "_")}.csv'

    headers = ["File Name", "File ID", "Owner"]
    with open(file_name, mode='w', newline='', encoding='utf-8') as file_out:
        writer = csv.writer(file_out)
        writer.writerow(headers)
        for file in files:
            writer.writerow([
                file['name'],
                file['id'],
                file['owners'][0]['emailAddress']
            ])

    return FileResponse(file_name, filename=file_name)

@app.get("/")
def root():
    return {"status": "Drive Audit API is up"}