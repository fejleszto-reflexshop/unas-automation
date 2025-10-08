import os
from typing import Optional, Any

import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.cloud import bigquery

from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

# =========================
#          CONFIG
# =========================

# popfanatic
EXCEL_POPFANATIC_TODAY_PATH = fr'../data/today_popfanatic_{datetime.now().strftime("%Y-%m-%d")}.xlsx'
EXCEL_POPFANATIC_SUMMARY_PATH = fr'../data/orders_popfanatic_main.xlsx'
EXCEL_POPFANATIC_WORKBOOK_PATH = fr'../data/orders_popfanatic_by_month.xlsx'

# Google Drive / Sheets
PARENT_FOLDER_ID: Optional[str] = None
MAKE_LINK_VIEWABLE = True

# BigQuery
PROJECT_ID  = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
DATASET     = os.getenv("GOOGLE_CLOUD_DATASET")
BQ_LOCATION = os.getenv("GOOGLE_CLOUD_BQ_LOCATION")

PREFIX_POPFANATIC_TABLE_NAME = "test-popfanatic-"

EXTERNAL_TABLE_NAME_POPFANATIC_NAME_DAILY = f"{PREFIX_POPFANATIC_TABLE_NAME}Mai rendelések"
EXTERNAL_TABLE_NAME_POPFANATIC_SUMMATY = f"{PREFIX_POPFANATIC_TABLE_NAME}Napi összegzés"
EXTERNAL_TABLE_NAME_POPFANATIC_WORKBOOK = f"{PREFIX_POPFANATIC_TABLE_NAME}Havi visszatekintés"

SHEET_RANGE: Optional[str] = None
SKIP_ROWS = 1
AUTO_DETECT_SCHEMA = True

# OAuth files
CREDENTIALS_FILE = "../credentials.json"
TOKEN_FILE       = "../token.json"

SCOPES = [os.getenv("GOOGLE_DRIVE_SCOPE"), os.getenv("GOOGLE_BIG_QUERY")]


def get_oauth_credentials() -> Credentials:
    """
    Load OAuth credentials from TOKEN_FILE if present; otherwise run the browser flow
    with CREDENTIALS_FILE. Saves refreshed/obtained token back to TOKEN_FILE.
    """
    creds = None
    if os.path.exists(TOKEN_FILE):
        # token.json must have BOTH scopes; if not, delete it and re-run
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(
                    f"Missing {CREDENTIALS_FILE}. "
                    "Download an OAuth client ID JSON from Google Cloud Console "
                    "(APIs & Services → Credentials → Create credentials → OAuth client ID → Desktop app) "
                    "and save it next to this script."
                )
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        # Optional: attach quota project to quiet the SDK warning
        try:
            creds = creds.with_quota_project(PROJECT_ID)
        except Exception:
            pass

        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return creds


def upload_excel_as_google_sheet(
    drive_service,
    excel_path: str,
    parent_folder_id: Optional[str] = None,
    make_link_viewable: bool = False,
) -> tuple[str, str]:
    """Upload .xlsx to Drive, convert to Google Sheet, return (sheet_id, link)."""
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

    created = drive_service.files().create(
        body=file_metadata, media_body=media, fields="id"
    ).execute()

    sheet_id = created["id"]
    sheet_link = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

    if make_link_viewable:
        drive_service.permissions().create(
            fileId=sheet_id,
            body={"role": "reader", "type": "anyone"},
        ).execute()

    return sheet_id, sheet_link


def create_external_table_pointing_to_sheet(
    project_id: str,
    dataset: str,
    table: str,
    sheet_id: str,
    credentials: Credentials,
    location: str = "EU",
    sheet_range: Optional[str] = None,
    skip_rows: int = 1,
    autodetect: bool = True,
):
    """
    CREATE OR REPLACE a BigQuery EXTERNAL TABLE that points to the Google Sheet.
    Uses the same OAuth user creds (which now include Drive + BigQuery scopes).
    """
    client = bigquery.Client(project=project_id, location=location, credentials=credentials)
    table_id = f"{project_id}.{dataset}.{table}"

    # Handle old/new library versions for source format & options
    try:
        source_fmt = bigquery.SourceFormat.GOOGLE_SHEETS
    except AttributeError:
        source_fmt = "GOOGLE_SHEETS"

    external_config = bigquery.ExternalConfig(source_fmt)
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    external_config.source_uris = [sheet_url]
    external_config.autodetect = autodetect

    try:
        gs_opts = bigquery.GoogleSheetsOptions()
        if sheet_range:
            gs_opts.range = sheet_range
        if skip_rows:
            gs_opts.skip_leading_rows = skip_rows
        external_config.options = gs_opts
    except AttributeError:
        pass

    table_obj = bigquery.Table(table_id)
    table_obj.external_data_configuration = external_config

    client.delete_table(table_id, not_found_ok=True)
    created = client.create_table(table_obj)
    return created, sheet_url


def load_excel_to_bigquery_native(
    excel_path: str,
    sheet_name,
    project_id: str,
    dataset: str,
    table: str,
    credentials: Credentials,
    location: str = "EU",
    write_mode: str = "WRITE_APPEND",
):
    """Load Excel into a native BigQuery table."""
    df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype_backend="pyarrow")
    df.columns = [str(c).strip() for c in df.columns]

    client = bigquery.Client(project=project_id, location=location, credentials=credentials)
    table_id = f"{project_id}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        autodetect=True,
    )
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    result = load_job.result()
    return result.output_rows

def upload_to_google_drive(drive, excel_path, info) -> tuple:

    sheet_id, sheet_link = upload_excel_as_google_sheet(
        drive_service=drive,
        excel_path=excel_path,
        parent_folder_id=PARENT_FOLDER_ID,
        make_link_viewable=MAKE_LINK_VIEWABLE
    )

    print(f"✅ Google Sheet {info} created:", sheet_link)

    return sheet_id, sheet_link

def create_external_table(sheet_id, table, user_creds, info) -> None:
    created_table, source_uri = create_external_table_pointing_to_sheet(
        project_id=PROJECT_ID,
        dataset=DATASET,
        table=table,
        sheet_id=sheet_id,
        credentials=user_creds,
        location=BQ_LOCATION,
        sheet_range=SHEET_RANGE,
        skip_rows=SKIP_ROWS,
        autodetect=AUTO_DETECT_SCHEMA,
    )

    print(f"✅ External table {info} created: {created_table.full_table_id}")
    print(f"   Source URI {info}: {source_uri}")


def delete_drive_files_by_name(
    drive,
    name: str,
    parent_folder_id: Optional[str] = None,
    mime_type: str = "application/vnd.google-apps.spreadsheet",
) -> list[str]:
    """
    Deletes all non-trashed Drive files that exactly match `name`.
    Optionally restricts to a parent folder and a mimeType (default: Google Sheets).
    Returns list of deleted file IDs.
    """
    # Escape single quotes for the Drive query
    safe_name = name.replace("'", r"\'")
    q_parts = [f"name = '{safe_name}'", "trashed = false"]
    if mime_type:
        q_parts.append(f"mimeType = '{mime_type}'")
    if parent_folder_id:
        q_parts.append(f"'{parent_folder_id}' in parents")
    q = " and ".join(q_parts)

    deleted_ids = []
    page_token = None

    while True:
        resp = drive.files().list(
            q=q,
            fields="nextPageToken, files(id, name)",
            pageToken=page_token,
            pageSize=1000,
        ).execute()

        for f in resp.get("files", []):
            drive.files().delete(fileId=f["id"]).execute()
            deleted_ids.append(f["id"])
        page_token = resp.get("nextPageToken")

        if not page_token:
            break

    return deleted_ids


def delete_prev_google_drive_files(drive, shop_prefix: str) -> None:
    today_name = f"{shop_prefix}_today"
    deletes_today = delete_drive_files_by_name(drive, today_name, PARENT_FOLDER_ID)

    # Check if today file is on Google Drive if yes delete it and upload the new today file
    if deletes_today:
        print(f"🗑️ Deleted {len(deletes_today)} old file(s) named '{today_name}'")

    combined_name = f"{shop_prefix}_combined"
    deletes_combined = delete_drive_files_by_name(drive, combined_name, PARENT_FOLDER_ID)

    if deletes_combined:
        print(f"Deleted summary file named '{combined_name}'")

    workbook_name = f"{shop_prefix}_workbook"
    deleted_workbook = delete_drive_files_by_name(drive, workbook_name, PARENT_FOLDER_ID)

    if deleted_workbook:
        print(f"Deleted workbook file named '{workbook_name}'")

def wrapper_upload_to_google_cloud(drive, user_creds, excel_path: list[str], table: list[str], info: list[str]) -> None:
    sheet_id_combined, _ = upload_to_google_drive(
        drive=drive,
        excel_path=excel_path[0],
        info=info[0]
    )
    create_external_table(
        sheet_id=sheet_id_combined,
        table=table[0],
        user_creds=user_creds,
        info=info[0]
    )

    sheet_id_workbook, _ = upload_to_google_drive(
        drive=drive,
        excel_path=excel_path[1],
        info=info[1]
    )
    create_external_table(
        sheet_id=sheet_id_workbook,
        table=table[1],
        user_creds=user_creds,
        info=info[1]
    )


def unas_webshops_upload(drive: Any, user_creds: Credentials, exclude_webshop: list[str]) -> None:
    """
    Collect webshop folders under ../data, exclude some by name, gather files
    (excluding those containing 'daily_summary' or 'today'), and prepare a
    folder->files mapping. The upload loop is left commented out as in the
    original snippet.
    """

    # --- collect top-level subfolders under ../data
    try:
        _, dirs, _ = next(os.walk("../data"))
    except StopIteration:
        print("No ../data directory found or it's empty.")
        return

    # --- exclude specific webshops (compare folder names to strings)
    all_webshop_folders = [d for d in dirs if d not in exclude_webshop]

    print("Webshop folders (after exclude):", all_webshop_folders)

    # --- list files inside each folder, filtering out 'daily_summary' and 'today'
    folder_and_file: dict[str, list[str]] = {}
    for folder in all_webshop_folders:
        folder_path = os.path.join("..", "data", folder)
        try:
            files = [
                fname
                for fname in os.listdir(folder_path)
                if os.path.isfile(os.path.join(folder_path, fname))
                and "daily_summary" not in fname
                and "today" not in fname
            ]
        except FileNotFoundError:
            files = []
        folder_and_file[folder] = files

    print("Folder -> files mapping:")
    print(folder_and_file)

    remaining_folders = len(folder_and_file.keys())

    for folder in folder_and_file.keys():
        print("Remaining folders: ", remaining_folders)

        delete_prev_google_drive_files(drive=drive, shop_prefix=folder)

        excel_path: list[str] = [
            fr"../data/{folder}/{folder_and_file.get(folder)[0]}",
            fr"../data/{folder}/{folder_and_file.get(folder)[1]}"
        ]

        table_path: list[str] = [
            fr"s-{folder}-napi",
            fr"s-{folder}-havi",
        ]

        info: list[str] = ['combined', 'workbook']

        wrapper_upload_to_google_cloud(drive, user_creds, excel_path, table_path, info)

        print("========== next ==========")
        remaining_folders -= 1


def shoprenter_webshops_upload(drive: Any, user_creds: Credentials, exclude_webshop: list[str]) -> None:
    raise NotImplementedError("TODO")

def popfanatic_upload(drive, user_creds) -> None:
    delete_prev_google_drive_files(drive, "%Y-%m-%d", "popfanatic")

    excel_path: list[str] = [EXCEL_POPFANATIC_TODAY_PATH, EXCEL_POPFANATIC_SUMMARY_PATH, EXCEL_POPFANATIC_WORKBOOK_PATH]
    table_path: list[str] = [EXTERNAL_TABLE_NAME_POPFANATIC_NAME_DAILY, EXTERNAL_TABLE_NAME_POPFANATIC_SUMMATY, EXTERNAL_TABLE_NAME_POPFANATIC_WORKBOOK]
    info: list[str] = ["today", "summary", "workbook"]

    wrapper_upload_to_google_cloud(drive, user_creds, excel_path, table_path, info)

def main():
    # OAuth user creds with BOTH scopes (Drive + BigQuery)
    user_creds: Credentials = get_oauth_credentials()

    # Drive client
    drive = build("drive", "v3", credentials=user_creds)

    unas_webshops_upload(drive=drive, user_creds=user_creds, exclude_webshop=[''])


if __name__ == "__main__":
    main()
