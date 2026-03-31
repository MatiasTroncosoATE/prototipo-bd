import requests
import csv, io
from dotenv import load_dotenv
import os

load_dotenv()

LMS_DOMAIN = "https://educar.atentamante.com"
CLIENT_ID = os.getenv('EDUNEXT_CLIENT_ID')
CLIENT_SECRET = os.getenv('EDUNEXT_CLIENT_SECRET')

def get_access_token():
    url = f"{LMS_DOMAIN}/oauth2/access_token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "token_type": "jwt",
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]


def get_headers(token):
    return {"Authorization": f"JWT {token}"}

token = get_access_token()
headers = get_headers(token)


def download_enrollments_csv(course_id: str, filename: str = "enrollments.csv"):
    token = get_access_token()
    headers = get_headers(token)

    url = f"{LMS_DOMAIN}/eox-core/api/v1/enrollment/"
    params = {"course_id": course_id, "page_size": 100}
    
    all_rows = []
    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        all_rows.extend(data.get("results", []))
        url = data.get("next")  # follow pagination
        params = {}  # next URL already has params

    # Write to CSV
    if all_rows:
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"Saved {len(all_rows)} rows to {filename}")

download_enrollments_csv("course-v1:MyOrg+CS101+2024")
