import json
from pathlib import Path
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Print the service account email being used
with open("read-and-write-491623-13e3c9210eef.json") as f:
    data = json.load(f)
print("Service account email:", data["client_email"])
print("Project ID:", data["project_id"])

# Build service
creds = Credentials.from_service_account_file("read-and-write-491623-13e3c9210eef.json", scopes=SCOPES)
service = build("sheets", "v4", credentials=creds)

# Try the request and print the full error
try:
    result = service.spreadsheets().get(spreadsheetId='1L9ZEl8c8TMElxa6PA_N7zioL6oCElM5KCFJRrOQ-fEQ').execute()
    print("Success! Spreadsheet title:", result["properties"]["title"])
except Exception as e:
    print("Error type:", type(e).__name__)
    print("Full error:", e)
