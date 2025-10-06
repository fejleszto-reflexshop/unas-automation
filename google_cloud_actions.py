import os
from typing import Optional

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

# reflexshop
EXCEL_REFLEXSHOP_PATH = fr"data/today_{datetime.now().strftime('%Y.%m.%d')}.xlsx"
EXCEL_REFLEXSHOP_SUMMARY_PATH = fr"data/orders_main.xlsx"
EXCEL_REFLEXSHOP_WORKBOOK_PATH = fr"data/orders_by_month.xlsx"

# popfanatic
EXCEL_POPFANATIC_TODAY_PATH = fr'data/orders_popfanatic_{datetime.now().strftime("%Y-%m-%d")}.xlsx'
EXCEL_POPFANATIC_SUMMARY_PATH = fr'data/orders_popfanatic_main.xlsx'
EXCEL_POPFANATIC_WORKBOOK_PATH = fr'data/orders_popfanatic_by_month.xlsx'

SHEET_NAME = 0

# Google Drive / Sheets
PARENT_FOLDER_ID: Optional[str] = None
MAKE_LINK_VIEWABLE = True

# BigQuery
PROJECT_ID  = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
DATASET     = os.getenv("GOOGLE_CLOUD_DATASET")
BQ_LOCATION = os.getenv("GOOGLE_CLOUD_BQ_LOCATION")


PREFIX_REFLEXSHOP_TABLE_NAME = "test-reflexshop-"
PREFIX_POPFANATIC_TABLE_NAME = "test-popfanatic-"

EXTERNAL_TABLE_NAME_REFLEXSHOP_NAME_DAILY   = f"{PREFIX_REFLEXSHOP_TABLE_NAME}Mai rendelések"
EXTERNAL_TABLE_NAME_REFLEXSHOP_NAME_SUMMARY = f"{PREFIX_REFLEXSHOP_TABLE_NAME}Napi összegzés"
EXTERNAL_TABLE_NAME_REFLEXSHOP_WORKBOOK = f"{PREFIX_REFLEXSHOP_TABLE_NAME}Havi visszatekintés"

EXTERNAL_TABLE_NAME_POPFANATIC_NAME_DAILY = f"{PREFIX_POPFANATIC_TABLE_NAME}Mai rendelések"
EXTERNAL_TABLE_NAME_POPFANATIC_SUMMATY = f"{PREFIX_POPFANATIC_TABLE_NAME}Napi összegzés"
EXTERNAL_TABLE_NAME_POPFANATIC_WORKBOOK = f"{PREFIX_POPFANATIC_TABLE_NAME}Havi visszatekintés"

SHEET_RANGE: Optional[str] = None
SKIP_ROWS = 1
AUTO_DETECT_SCHEMA = True

# OAuth files
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"

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


def reflexshop_upload(drive, user_creds) -> None:
    # Upload today orders
    sheet_id_today, sheet_link_today = upload_to_google_drive(
        drive=drive,
        excel_path=EXCEL_REFLEXSHOP_PATH,
        info="today"
    )

    create_external_table(
        sheet_id=sheet_id_today,
        table=EXTERNAL_TABLE_NAME_REFLEXSHOP_NAME_DAILY,
        user_creds=user_creds,
        info="today"
    )

    # Upload daily summary orders
    sheet_id_summary, sheet_link_summary = upload_to_google_drive(
        drive=drive,
        excel_path=EXCEL_REFLEXSHOP_SUMMARY_PATH,
        info="summary"
    )

    create_external_table(
        sheet_id=sheet_id_summary,
        table=EXTERNAL_TABLE_NAME_REFLEXSHOP_NAME_SUMMARY,
        user_creds=user_creds,
        info="summary"
    )

    # Upload workbook for previous months
    sheet_id_workbook, sheet_link_workbook = upload_to_google_drive(
        drive=drive,
        excel_path=EXCEL_REFLEXSHOP_WORKBOOK_PATH,
        info="workbook"
    )

    create_external_table(
        sheet_id=sheet_id_workbook,
        table=EXTERNAL_TABLE_NAME_REFLEXSHOP_WORKBOOK,
        user_creds=user_creds,
        info="workbook"
    )


def popfanatic_upload(drive, user_creds) -> None:
    sheet_id_today, sheet_link_today = upload_to_google_drive(
        drive=drive,
        excel_path=EXCEL_POPFANATIC_TODAY_PATH,
        info="today"
    )
    create_external_table(
        sheet_id=sheet_id_today,
        table=EXTERNAL_TABLE_NAME_POPFANATIC_NAME_DAILY,
        user_creds=user_creds,
        info="today"
    )

    sheet_id_summary, sheet_link_summary = upload_to_google_drive(
        drive=drive,
        excel_path=EXCEL_POPFANATIC_SUMMARY_PATH,
        info="summary"
    )
    create_external_table(
        sheet_id=sheet_id_summary,
        table=EXTERNAL_TABLE_NAME_POPFANATIC_SUMMATY,
        user_creds=user_creds,
        info="summary"
    )

    sheet_id_workbook, sheet_link_workbook = upload_to_google_drive(
        drive=drive,
        excel_path=EXCEL_POPFANATIC_WORKBOOK_PATH,
        info="workbook"
    )
    create_external_table(
        sheet_id=sheet_id_workbook,
        table=EXTERNAL_TABLE_NAME_POPFANATIC_WORKBOOK,
        user_creds=user_creds,
        info="workbook"
    )


# TODO: delete yesterday today_{...} excel file before uploading the new today_{...} excel file
def main():
    # OAuth user creds with BOTH scopes (Drive + BigQuery)
    user_creds = get_oauth_credentials()

    # Drive client
    drive = build("drive", "v3", credentials=user_creds)

    reflexshop_upload(drive=drive, user_creds=user_creds)

    # popfanatic_upload(drive=drive, user_creds=user_creds)


if __name__ == "__main__":
    main()
