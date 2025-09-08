# Google Drive Audit and Management Tool

A comprehensive Google Drive audit and management tool that helps you audit user files, transfer ownership of files and folders, and manage shared file access. Built with FastAPI backend and Google Apps Script frontend.

## 🚀 Features

- **📊 Audit User Files**: Generate CSV reports of all files a user has access to (excluding owned files)
- **📁 Transfer Ownership**: Transfer ownership of all folders and their contents from one user to another
- **📄 Transfer Single File/Folder**: Transfer ownership of specific files or folders (including all contents)
- **🔄 Replace Shared User**: Replace a user's access to all shared files with another user
- **📱 Google Sheet Integration**: User-friendly interface within Google Sheets
- **🐳 Docker Support**: Easy deployment with Docker and Docker Compose
- **🔒 Secure**: Bearer token authentication and proper error handling
- **📈 Monitoring**: Health checks and comprehensive logging

## 📁 Project Structure

```
.
├── main.py                 # Main FastAPI application
├── test.py                 # Enhanced test version with better validation
├── appscript.js           # Google Apps Script code
├── AuditSidebar.html      # HTML sidebar for Google Sheets
├── test_api.py            # Comprehensive test suite
├── requirements.txt       # Python dependencies
├── requirements-test.txt  # Test dependencies
├── Dockerfile            # Docker configuration
├── docker-compose.yml    # Docker Compose setup
├── deploy.sh             # Deployment script
├── .env.example          # Environment variables template
├── downloads/            # Generated audit reports storage
└── drive-audit-service.json  # Google Service Account credentials
```

## 🛠️ Prerequisites

- Python 3.10 or higher
- Docker and Docker Compose (for containerized deployment)
- Google Cloud Platform project with Google Drive API enabled
- Google Service Account with domain-wide delegation
- Google Sheet for the Apps Script frontend

## ⚙️ Installation & Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd google-drive-audit-tool
```

### 2. Google Cloud Setup

1. **Create a Google Cloud Project**
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select existing one

2. **Enable Google Drive API**
   ```bash
   gcloud services enable drive.googleapis.com
   ```

3. **Create Service Account**
   ```bash
   gcloud iam service-accounts create drive-audit-service \
     --display-name="Drive Audit Service Account"
   ```

4. **Create and Download Service Account Key**
   ```bash
   gcloud iam service-accounts keys create drive-audit-service.json \
     --iam-account=drive-audit-service@YOUR-PROJECT-ID.iam.gserviceaccount.com
   ```

5. **Enable Domain-Wide Delegation**
   - Go to Google Cloud Console → IAM & Admin → Service Accounts
   - Click on your service account
   - Go to "Advanced settings" → "Enable Google Workspace Domain-wide Delegation"
   - Note the Client ID for the next step

6. **Configure Domain-Wide Delegation in Google Admin Console**
   - Go to [Google Admin Console](https://admin.google.com/)
   - Navigate to Security → API Controls → Domain-wide Delegation
   - Add your service account Client ID with scope: `https://www.googleapis.com/auth/drive`

### 3. Environment Configuration

1. **Copy environment template**
   ```bash
   cp .env.example .env
   ```

2. **Edit .env file**
   ```bash
   # Required settings
   DOWNLOAD_DIR=./downloads
   SERVICE_ACCOUNT_FILE=./drive-audit-service.json
   API_BEARER_TOKEN=your-secure-random-token-here
   
   # Optional settings
   EXTERNAL_BASE_URL=https://your-domain.com
   LOG_LEVEL=INFO
   ```

### 4. Deployment Options

#### Option A: Docker Deployment (Recommended)

```bash
# Make deployment script executable
chmod +x deploy.sh

# Run deployment
./deploy.sh
```

#### Option B: Manual Python Deployment

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 5. Google Apps Script Setup

1. **Open Google Sheets**
   - Create a new Google Sheet
   - Create sheets named: "Drive File Audit", "Folder Transfer", "Single File Transfer"

2. **Setup Apps Script**
   - Go to Extensions → Apps Script
   - Delete default code and paste content from `appscript.js`
   - Create new HTML file named "AuditSidebar" and paste content from `AuditSidebar.html`

3. **Configure Script Properties**
   - In Apps Script, go to Project Settings
   - Add Script Properties:
     - `API_BASE_URL`: Your API URL (e.g., `https://your-domain.com:8000`)
     - `API_BEARER_TOKEN`: Same token from your .env file

4. **Deploy and Test**
   - Save the project
   - Refresh your Google Sheet
   - You should see a "User Audit" menu

## 📊 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check |
| `/health` | GET | Detailed health status |
| `/audit-user` | POST | Generate user file audit |
| `/transfer-owned-files` | POST | Transfer all owned folders |
| `/transfer-single-file` | POST | Transfer specific file/folder |
| `/replace-shared-user` | POST | Replace user access on shared files |
| `/download/{file_id}` | GET | Download audit report |

### Example API Usage

```bash
# Health check
curl http://localhost:8000/health

# Audit user files
curl -X POST http://localhost:8000/audit-user \
  -H "Authorization: Bearer your-token" \
  -H "Content-Type: application/json" \
  -d '{"user_email": "user@example.com"}'

# Transfer single file
curl -X POST http://localhost:8000/transfer-single-file \
  -H "Authorization: Bearer your-token" \
  -H "Content-Type: application/json" \
  -d '{
    "file_id": "1234567890abcdef",
    "from_email": "old-user@example.com",
    "to_email": "new-user@example.com"
  }'
```

## 🧪 Testing

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run tests
pytest test_api.py -v

# Run with coverage
pytest test_api.py --cov=main --cov-report=html
```

## 🔧 Configuration Options

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DOWNLOAD_DIR` | No | `./downloads` | Directory for CSV files |
| `SERVICE_ACCOUNT_FILE` | Yes | - | Path to service account JSON |
| `API_BEARER_TOKEN` | Recommended | - | API authentication token |
| `EXTERNAL_BASE_URL` | No | Auto-detected | External URL for downloads |
| `LOG_LEVEL` | No | `INFO` | Logging level |

### Google Apps Script Properties

| Property | Required | Description |
|----------|----------|-------------|
| `API_BASE_URL` | Yes | Your API base URL |
| `API_BEARER_TOKEN` | Yes | Same as API token |

## 🚨 Security Considerations

- **Always use HTTPS** in production
- **Set strong bearer tokens** (use `openssl rand -hex 32`)
- **Restrict service account permissions** to minimum required
- **Monitor API access logs** regularly
- **Keep service account keys secure** and rotate periodically

## 📈 Monitoring & Logging

- Health check endpoint: `/health`
- Structured logging with timestamps
- Docker health checks included
- Error tracking and reporting

## 🐛 Troubleshooting

### Common Issues

1. **"Service account not found"**
   - Verify `SERVICE_ACCOUNT_FILE` path is correct
   - Ensure service account JSON file exists

2. **"Domain-wide delegation not enabled"**
   - Check Google Admin Console delegation settings
   - Verify Client ID and scopes are correct

3. **"Bearer token required"**
   - Set `API_BEARER_TOKEN` in environment
   - Configure matching token in Apps Script properties

4. **"File not found" on download**
   - Files auto-delete after 5 minutes
   - Check `DOWNLOAD_DIR` permissions

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG

# Run with debug output
uvicorn main:app --host 0.0.0.0 --port 8000 --log-level debug
```

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs for error details
3. Open an issue with detailed information

---

**⚠️ Important**: This tool has powerful capabilities to modify Google Drive files. Always test in a non-production environment first and ensure you have proper backups.