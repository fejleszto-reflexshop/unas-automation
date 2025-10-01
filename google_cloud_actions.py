import os
from typing import Optional

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.cloud import bigquery


EXCEL_PATH = r"data/today.xlsx"
SHEET_NAME = 0

# ---- Google Sheets (Drive) ----
# Optional: upload into a specific Drive folder.
# If you have a folder, set its ID (the long string from its URL). Otherwise leave None.
PARENT_FOLDER_ID: Optional[str] = None

# If True, set permission "Anyone with the link can view" on the created sheet
MAKE_LINK_VIEWABLE = True

# ---- BigQuery ----
PROJECT_ID  = "webshop-riport-2025"
DATASET     = "Riportok"
TABLE       = "test"
BQ_LOCATION = "EU"
WRITE_MODE  = "WRITE_APPEND"

# ---- OAuth files ----
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"

# ---- OAuth scopes ----
SCOPES = ["https://www.googleapis.com/auth/drive"]  # full Drive access (adjust if needed)


def get_oauth_credentials() -> Credentials:
    """
    Load OAuth credentials from TOKEN_FILE if present; otherwise run the browser flow
    with CREDENTIALS_FILE. Saves refreshed/obtained token back to TOKEN_FILE.
    """
    creds = None

    # Load user token if previously saved
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    # Refresh or run browser flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Try refreshing with the saved refresh token
            creds.refresh(Request())
        else:
            # Start local server flow to get new credentials
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(
                    f"Missing {CREDENTIALS_FILE}. "
                    "Download an OAuth client ID JSON from Google Cloud Console "
                    "(APIs & Services → Credentials → Create credentials → OAuth client ID → Desktop app) "
                    "and save it next to this script."
                )
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save token for next runs
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return creds


def upload_excel_as_google_sheet(
    drive_service,
    excel_path: str,
    parent_folder_id: Optional[str] = None,
    make_link_viewable: bool = False,
) -> str:
    """
    Uploads an .xlsx to Drive and converts it to a Google Sheet.
    Returns the Google Sheet URL.
    """
    base_name = os.path.splitext(os.path.basename(excel_path))[0]

    file_metadata = {
        "name": base_name,
        "mimeType": "application/vnd.google-apps.spreadsheet",
    }
    if parent_folder_id:
        file_metadata["parents"] = [parent_folder_id]

    media = MediaFileUpload(
        excel_path,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True,
    )

    created = (
        drive_service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )
    sheet_id = created["id"]
    sheet_link = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

    if make_link_viewable:
        drive_service.permissions().create(
            fileId=sheet_id,
            body={"role": "reader", "type": "anyone"},
        ).execute()

    return sheet_link


def load_excel_to_bigquery(
    excel_path: str,
    sheet_name,
    project_id: str,
    dataset: str,
    table: str,
    location: str = "EU",
    write_mode: str = "WRITE_APPEND",
):
    """
    Reads Excel into a pandas DataFrame and loads it to BigQuery.
    """
    # Read Excel with pyarrow dtypes for better type mapping
    df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype_backend="pyarrow")

    # Tidy headers (avoid trailing spaces etc.)
    df.columns = [str(c).strip() for c in df.columns]

    client = bigquery.Client(project=project_id, location=location)
    table_id = f"{project_id}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        autodetect=True,  # Let BQ infer schema; switch off and set job_config.schema for strict typing
    )

    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    result = load_job.result()
    return result.output_rows


def main():
    # ---- OAuth for Drive ----
    creds = get_oauth_credentials()
    drive = build("drive", "v3", credentials=creds)

    # ---- Upload Excel -> Google Sheet (get link) ----
    sheet_link = upload_excel_as_google_sheet(
        drive_service=drive,
        excel_path=EXCEL_PATH,
        parent_folder_id=PARENT_FOLDER_ID,
        make_link_viewable=MAKE_LINK_VIEWABLE,
    )
    print("✅ Google Sheet created:", sheet_link)

    # ---- Load the same Excel into BigQuery ----
    rows = load_excel_to_bigquery(
        excel_path=EXCEL_PATH,
        sheet_name=SHEET_NAME,
        project_id=PROJECT_ID,
        dataset=DATASET,
        table=TABLE,
        location=BQ_LOCATION,
        write_mode=WRITE_MODE,
    )
    print(f"✅ BigQuery upload complete. Rows uploaded: {rows}")


if __name__ == "__main__":
    main()
