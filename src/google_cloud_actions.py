import os
import shutil
from typing import Optional, Any
import re

import pandas as pd
import unicodedata

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

# Google Sheets IMPORTRANGE source (env)
GS_SOURCE_SHEET_ID = os.getenv("GS_SOURCE_SHEET_ID")  # the spreadsheet you import FROM
GS_KLUBTAGSAG_SOURCE_RANGE = os.getenv("GS_KLUBTAGSAG_SOURCE_RANGE")

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

# IMPORTANT: we’ll provide an explicit schema -> no autodetect
AUTO_DETECT_SCHEMA = False

# OAuth files
CREDENTIALS_FILE = "../credentials.json"
TOKEN_FILE       = "../token.json"

# Scopes
SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/bigquery",
]

# =========================
#     AUTH / DRIVE / BQ
# =========================

def get_oauth_credentials() -> Credentials:
    """
    Load OAuth credentials from TOKEN_FILE if present; otherwise run the browser flow
    with CREDENTIALS_FILE. Saves refreshed/obtained token back to TOKEN_FILE.
    If the existing token is missing required scopes, it will be discarded and re-created.
    """
    creds = None
    token_path = TOKEN_FILE
    creds_path = CREDENTIALS_FILE

    def run_flow() -> Credentials:
        if not os.path.exists(creds_path):
            raise FileNotFoundError(
                f"Missing {creds_path}. Download an OAuth client ID JSON "
                "from Google Cloud Console (APIs & Services → Credentials → "
                "Create credentials → OAuth client ID → Desktop app)."
            )
        flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
        c = flow.run_local_server(port=0)
        with open(token_path, "w") as f:
            f.write(c.to_json())
        return c

    # Try loading existing token
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            # If token scopes don’t include all required, force re-auth
            token_scopes = set(getattr(creds, "scopes", []) or [])
            required = set(SCOPES)
            if not required.issubset(token_scopes):
                # discard bad token
                try:
                    os.remove(token_path)
                except Exception:
                    pass
                creds = run_flow()
        except Exception:
            # corrupted token file -> reauth
            try:
                os.remove(token_path)
            except Exception:
                pass
            creds = run_flow()
    else:
        creds = run_flow()

    # Refresh if expired
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as e:
            # If refresh fails with invalid_scope, delete token and re-auth
            if "invalid_scope" in str(e):
                try:
                    os.remove(token_path)
                except Exception:
                    pass
                creds = run_flow()
            else:
                raise

    # Optional: attach quota project
    try:
        if PROJECT_ID:
            creds = creds.with_quota_project(PROJECT_ID)
    except Exception:
        pass

    # Ensure on-disk token matches latest creds
    try:
        with open(token_path, "w") as f:
            f.write(creds.to_json())
    except Exception:
        pass

    return creds

def only_space_to_underscore(name: str) -> str:
    return str(name).replace(" ", "_")

def upload_excel_as_google_sheet(
    drive_service,
    excel_path: str,
    parent_folder_id: Optional[str] = None,
    make_link_viewable: bool = False,
    desired_title: Optional[str] = None,       # NEW: readable name (Hungarian kept)
) -> tuple[str, str]:
    """Upload .xlsx to Drive, convert to Google Sheet, return (sheet_id, link)."""
    base_name = os.path.splitext(os.path.basename(excel_path))[0]
    title = desired_title or base_name
    # keep Hungarian letters, only spaces -> underscores
    title = only_space_to_underscore(title)

    file_metadata = {
        "name": title,
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
    autodetect: bool = False,
    provided_bq_cols: list[str] | None = None
):
    client = bigquery.Client(project=project_id, location=location, credentials=credentials)
    table_id = f"{project_id}.{dataset}.{table}"

    # --- source format compat (new SDK has enum, old expects string)
    try:
        source_fmt = bigquery.SourceFormat.GOOGLE_SHEETS  # new versions
    except Exception:
        source_fmt = "GOOGLE_SHEETS"                      # older versions

    external_config = bigquery.ExternalConfig(source_fmt)

    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    external_config.source_uris = [sheet_url]

    # autodetect flag exists broadly; still guard just in case
    try:
        external_config.autodetect = autodetect
    except Exception:
        pass

    # --- GoogleSheetsOptions compat (class may not exist on old SDK)
    try:
        gs_opts = bigquery.GoogleSheetsOptions()
        if sheet_range:
            gs_opts.range = sheet_range
        if skip_rows:
            gs_opts.skip_leading_rows = skip_rows
        external_config.options = gs_opts
    except Exception:
        # fall back to setting raw properties for very old SDKs
        edc = external_config._properties.setdefault("externalDataConfiguration", {})
        gso = edc.setdefault("googleSheetsOptions", {})
        edc["sourceFormat"] = "GOOGLE_SHEETS"
        if sheet_range:
            gso["range"] = sheet_range
        if skip_rows:
            edc["skipLeadingRows"] = skip_rows

    table_obj = bigquery.Table(table_id)
    table_obj.external_data_configuration = external_config

    # explicit schema if we disabled autodetect
    if not autodetect and provided_bq_cols:
        table_obj.schema = [bigquery.SchemaField(name, "STRING") for name in provided_bq_cols]

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

def upload_to_google_drive(drive, excel_path, info, desired_title: Optional[str] = None) -> tuple:
    sheet_id, sheet_link = upload_excel_as_google_sheet(
        drive_service=drive,
        excel_path=excel_path,
        parent_folder_id=PARENT_FOLDER_ID,
        make_link_viewable=MAKE_LINK_VIEWABLE,
        desired_title=desired_title or info,   # readable Hungarian title OK
    )

    print(f"✅ Google Sheet {info} created:", sheet_link)
    return sheet_id, sheet_link

def create_external_table(sheet_id, table, user_creds, info, provided_bq_cols: Optional[list[str]] = None) -> None:
    created_table, source_uri = create_external_table_pointing_to_sheet(
        project_id=PROJECT_ID,
        dataset=DATASET,
        table=table,
        sheet_id=sheet_id,
        credentials=user_creds,
        location=BQ_LOCATION,
        sheet_range=SHEET_RANGE,
        skip_rows=SKIP_ROWS,
        autodetect=AUTO_DETECT_SCHEMA,            # False -> explicit schema below
        provided_bq_cols=provided_bq_cols
    )

    print(f"✅ External table {info} created: {created_table.full_table_id}")
    print(f"   Source URI {info}: {source_uri}")

# =========================
#    SHEETS (IMPORTRANGE)
# =========================

def ensure_sheet_exists(sheets_service, spreadsheet_id: str, sheet_title: str):
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if sheet_title in titles:
        return
    requests = [{"addSheet": {"properties": {"title": sheet_title}}}]
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()

def set_klubtagsag_importrange(
    sheets_service,
    spreadsheet_id: str,
    source_sheet_id: str,
    source_range: str = "Előfizetői kategória!A:A",
    target_tab: str = "Klubtagsag",
    target_cell: str = "A1",
) -> None:
    """
    Creates 'Klubtagsag' tab (if missing) and writes the IMPORTRANGE formula into A1.
    """
    ensure_sheet_exists(sheets_service, spreadsheet_id, target_tab)
    formula = f'=IMPORTRANGE("https://docs.google.com/spreadsheets/d/{source_sheet_id}";"{source_range}")'
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{target_tab}!{target_cell}",
        valueInputOption="USER_ENTERED",
        body={"values": [[formula]]}
    ).execute()
    print("✅ Klubtagsag IMPORTRANGE set.")


def create_sheet_if_missing(sheets_service, spreadsheet_id: str, sheet_name: str) -> None:
    """Ensure a sheet with the given name exists."""
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if sheet_name not in titles:
        request = {"addSheet": {"properties": {"title": sheet_name}}}
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [request]}
        ).execute()


def set_korrigalt_query_sheet(sheets_service, spreadsheet_id: str) -> None:
    """
    Creates a sheet named '<year>-Korrigalt' and inserts the given QUERY formula into A1.
    """
    year_str = str(datetime.today().year)
    sheet_name = f"{year_str}-Korrigalt"

    create_sheet_if_missing(sheets_service, spreadsheet_id, sheet_name)

    # A1 — 3 columns
    formula_A1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col1, Col2, Col3 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') "
        "or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    # D1 — just Col18
    formula_D1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col18 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') "
        "or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    # E1 — just Col4 (Dátum)
    formula_E1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col4 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') "
        "or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    formula_F1 = """=ARRAYFORMULA(HA(ARRAYFORMULA( HA( SZÁM(SZÖVEG.KERES("GLS - csomagautomata"; ))); "GLS - csomagautomata"; HA( SZÁM(SZÖVEG.KERES("GLS - csomagpont"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - csomagpont"; HA( SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 1."; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - Nemzetközi 1."; HA( SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 2."; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - Nemzetközi 2."; HA( SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 3."; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - Nemzetközi 3."; HA( SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 4."; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - Nemzetközi 4."; HA( SZÁM(SZÖVEG.KERES("GLS Futárszolgálat"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS Futárszolgálat"; HA( SZÁM(SZÖVEG.KERES("MPL csomagautomata"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "MPL csomagautomata"; HA( SZÁM(SZÖVEG.KERES("MPL házhozszállítás"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "MPL házhozszállítás"; HA( SZÁM(SZÖVEG.KERES("MPL posta pont"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "MPL posta pont"; HA( SZÁM(SZÖVEG.KERES("MPL postán maradó"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "MPL postán maradó"; HA( SZÁM(SZÖVEG.KERES("Személyes átvétel - Buda"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Személyes átvétel - Buda"; HA( SZÁM(SZÖVEG.KERES("Személyes átvétel - Debrecen"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Személyes átvétel - Debrecen"; HA( SZÁM(SZÖVEG.KERES("Személyes átvétel - Pest"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Személyes átvétel - Pest"; HA( SZÁM(SZÖVEG.KERES("MPL Postán maradó"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "MPL postán maradó"; HA( SZÁM(SZÖVEG.KERES("GLS Csomagpont"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - csomagpont"; HA( SZÁM(SZÖVEG.KERES("Express One csomagpont"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Express One csomagpont"; HA( SZÁM(SZÖVEG.KERES("Packeta csomagpont és csomagautomata"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Packeta csomagpont és csomagautomata"; HA( SZÁM(SZÖVEG.KERES("GLS Csomagautomata"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - csomagautomata"; HA( SZÁM(SZÖVEG.KERES("Express One házhozszállítás"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Express One házhozszállítás"; HA( SZÁM(SZÖVEG.KERES("Előfizetés szállítás"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Előfizetés"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1) ) ))))) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) )="";HA(N:N="WELCOMEPACK";"GLS Futárszolgálat";"Előfizetés"); ARRAYFORMULA( HA( SZÁM(SZÖVEG.KERES("GLS - csomagautomata"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - csomagautomata"; HA( SZÁM(SZÖVEG.KERES("GLS - csomagpont"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - csomagpont"; HA( SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 1."; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - Nemzetközi 1."; HA( SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 2."; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - Nemzetközi 2."; HA( SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 3."; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - Nemzetközi 3."; HA( SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 4."; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - Nemzetközi 4."; HA( SZÁM(SZÖVEG.KERES("GLS Futárszolgálat"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS Futárszolgálat"; HA( SZÁM(SZÖVEG.KERES("MPL csomagautomata"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "MPL csomagautomata"; HA( SZÁM(SZÖVEG.KERES("MPL házhozszállítás"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "MPL házhozszállítás"; HA( SZÁM(SZÖVEG.KERES("MPL posta pont"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "MPL posta pont"; HA( SZÁM(SZÖVEG.KERES("MPL postán maradó"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "MPL postán maradó"; HA( SZÁM(SZÖVEG.KERES("Személyes átvétel - Buda"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Személyes átvétel - Buda"; HA( SZÁM(SZÖVEG.KERES("Személyes átvétel - Debrecen"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Személyes átvétel - Debrecen"; HA( SZÁM(SZÖVEG.KERES("Személyes átvétel - Pest"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Személyes átvétel - Pest"; HA( SZÁM(SZÖVEG.KERES("MPL Postán maradó"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "MPL postán maradó"; HA( SZÁM(SZÖVEG.KERES("GLS Csomagpont"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - csomagpont"; HA( SZÁM(SZÖVEG.KERES("Packeta csomagpont és csomagautomata"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Packeta csomagpont és csomagautomata"; HA( SZÁM(SZÖVEG.KERES("Express One csomagpont"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Express One csomagpont"; HA( SZÁM(SZÖVEG.KERES("GLS Csomagautomata"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "GLS - csomagautomata"; HA( SZÁM(SZÖVEG.KERES("Express One házhozszállítás"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Express One házhozszállítás"; HA( SZÁM(SZÖVEG.KERES("Előfizetés szállítás"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1))); "Előfizetés"; QUERY('Sheet1'!A:S;"select Col5 where where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)))))))))))))))))))))))))"""

    # G1 — multiple columns
    formula_G1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col6, Col7, Col9, Col10, Col8, Col11 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') "
        "or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    formula_M1 = """=QUERY(ARRAYFORMULA(IFERROR(ÉRTÉK(QUERY(Sheet1!A:S;"select Col17 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 is null or Col2 matches '^\\s*$') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')";1))));"select Col1 label Col1 'Termék mennyisége'";0)"""

    # N1 — coupon / Col16
    formula_N1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col16 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') "
        "or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    formula_O1 = """=query(ARRAYFORMULA(ÉRTÉK(HELYETTE(QUERY(to_text('Sheet1'!A:S);"select Col18 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1);".";",")));"select * label Col1 'Termék egységára'")"""

    formula_P1 = """={"Rendelés nettó részösszege";ARRAYFORMULA(HA(M2:M="";"";(M2:M*O2:O)))}"""

    formula_Q1 = """={"Összesített Áfa kulcs";ARRAYFORMULA(ifna(FKERES(N2:N;'ÁFA kulcsok'!A:B;2;HAMIS);""))}"""

    formula_R1 = """={"Rendelés bruttó részösszege";ARRAYFORMULA(HA(M2:M="";"";(KEREK.FEL(P2:P*(1+(Q2:Q/100));1))))}"""

    formula_S1 = """=query(ARRAYFORMULA(ARRAYFORMULA(ÉRTÉK(HELYETTE(QUERY(to_text('Sheet1'!A:S);"select Col12 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1);".";",")))/DARABHATÖBB(A:A;A:A));"select * label Col1 'Szállítási díj'")"""

    formula_T1 = """=query(ARRAYFORMULA(ARRAYFORMULA(ÉRTÉK(HELYETTE(QUERY(to_text('Sheet1'!A:BB);"select Col13 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')";1);".";",")))/DARABHATÖBB(A:A;A:A));"select * label Col1 'Fizetési illeték'")"""

    formula_U1 = """=query(ARRAYFORMULA(ARRAYFORMULA(ÉRTÉK(HELYETTE(QUERY(to_text('Sheet1'!A:BB);"select Col14 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')";1);".";",")))/DARABHATÖBB(A:A;A:A));"select * label Col1 'Kupon összege'")"""

    formula_V1 = """={"Összesen bruttó";ARRAYFORMULA(HA(M2:M="";"";(R2:R+S2:S+T2:T+U2:U)))}"""

    formula_W1 = """=QUERY(Sheet1!A:S;"select Col15 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')";1)"""

    formula_X1 = """={"összesen nettó";ARRAYFORMULA(HA(A2:A="";"";P2:P+(S2:S/1,27)+(T2:T/1,27)+(U2:U/1,27)))}"""

    formula_Y1 = """=ARRAYFORMULA(HELYETTE(QUERY('Sheet1'!A:BB;"select Col19 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')";1);"Árukereső Marketplace";"Reflexshop"))"""

    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": f"{sheet_name}!A1", "values": [[formula_A1]]},
                {"range": f"{sheet_name}!D1", "values": [[formula_D1]]},
                {"range": f"{sheet_name}!E1", "values": [[formula_E1]]},
                # {"range": f"{sheet_name}!F1", "values": [[formula_F1]]},
                {"range": f"{sheet_name}!G1", "values": [[formula_G1]]},
                {"range": f"{sheet_name}!M1", "values": [[formula_M1]]},
                {"range": f"{sheet_name}!N1", "values": [[formula_N1]]},
                {"range": f"{sheet_name}!O1", "values": [[formula_O1]]},
                {"range": f"{sheet_name}!P1", "values": [[formula_P1]]},
                {"range": f"{sheet_name}!Q1", "values": [[formula_Q1]]},
                {"range": f"{sheet_name}!R1", "values": [[formula_R1]]},
                {"range": f"{sheet_name}!S1", "values": [[formula_S1]]},
                {"range": f"{sheet_name}!T1", "values": [[formula_T1]]},
                {"range": f"{sheet_name}!U1", "values": [[formula_U1]]},
                {"range": f"{sheet_name}!V1", "values": [[formula_V1]]},
                {"range": f"{sheet_name}!W1", "values": [[formula_W1]]},
                {"range": f"{sheet_name}!X1", "values": [[formula_X1]]},
                {"range": f"{sheet_name}!Y1", "values": [[formula_Y1]]},
                # {"range": f"{sheet_name}!Z1", "values": [[formula_Z1]]},
            ],
        },
    ).execute()

    print(f"✅ Added sheet '{sheet_name}' with QUERY formulas.")
    print(f"✅ Added sheet '{sheet_name}' with QUERY formulas.")

def create_afa_kulcsok_sheet(sheets_service, spreadsheet_id: str) -> None:
    sheet_name = f"ÁFA kulcsok"

    create_sheet_if_missing(sheets_service, spreadsheet_id, sheet_name)

    formula_A1 = """=IMPORTRANGE("1Q6njvwWkLRS_ZVMcbksXNy9gysDfRdGUVxInln7P9O0";"fő!A:A")"""
    formula_B1 = """=IMPORTRANGE("1Q6njvwWkLRS_ZVMcbksXNy9gysDfRdGUVxInln7P9O0";"fő!C:C")"""

    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": f"{sheet_name}!A1", "values": [[formula_A1]]},
                {"range": f"{sheet_name}!B1", "values": [[formula_B1]]},
            ]
        }
    ).execute()

    print(f"✅ Added sheet '{sheet_name}' with IMPORTRANGE formula.")


# =========================
#      DRIVE UTILITIES
# =========================

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

def delete_prev_google_drive_files(drive, webshop: str) -> None:
    daily_name = f"{webshop}_daily_summary"
    deletes_today = delete_drive_files_by_name(drive, daily_name, PARENT_FOLDER_ID)

    # Check if today file is on Google Drive if yes delete it and upload the new today file
    if deletes_today:
        print(f"🗑️ Deleted {len(deletes_today)} old file(s) named '{daily_name}'")

    workbook_name = f"{webshop}_year-{datetime.now().strftime('%Y')}"
    deleted_workbook = delete_drive_files_by_name(drive, workbook_name, PARENT_FOLDER_ID)

    if deleted_workbook:
        print(f"Deleted workbook file named '{workbook_name}'")

# =========================
#   HEADER SANITIZATION
# =========================

def ascii_bq_safe(name: str) -> str:
    # strip accents to ASCII, then enforce [A-Za-z0-9_], start with letter/_
    s = unicodedata.normalize('NFKD', name)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))  # remove accents
    s = re.sub(r'[^A-Za-z0-9_]', '_', s)
    if re.match(r'^[0-9]', s):
        s = f'col_{s}'
    if not s:
        s = 'col'
    return s

def sanitize_excel_headers_for_bq(in_xlsx: str, sheet_name=0, output_name: str | None = None):
    """
    - Drops 'Unnamed:' columns.
    - Keeps Hungarian letters in the SHEET headers; only spaces -> underscores.
    - Ensures display header uniqueness.
    - Saves a cleaned Excel copy (readable filename).
    - Returns (out_path, bq_cols) where bq_cols are ASCII-safe names for BigQuery schema.
    """
    df = pd.read_excel(in_xlsx, sheet_name=sheet_name)

    # 1) drop unnamed columns
    mask_named = ~df.columns.to_series().astype(str).str.match(r'^Unnamed')
    df = df.loc[:, mask_named]

    # 2) keep Hungarian in SHEET headers, only space→underscore
    human_cols = [only_space_to_underscore(c) for c in df.columns]
    # ensure uniqueness for display too
    seen = {}
    final_human = []
    for c in human_cols:
        if c not in seen:
            seen[c] = 1
            final_human.append(c)
        else:
            seen[c] += 1
            final_human.append(f"{c}_{seen[c]}")
    df.columns = final_human

    # 3) save cleaned Excel next to input
    if output_name is None:
        base, ext = os.path.splitext(os.path.basename(in_xlsx))
        output_name = f"{base}_cleaned.xlsx"
    out_path = os.path.join(os.path.dirname(in_xlsx), output_name)
    df.to_excel(out_path, index=False)

    # 4) ALSO return a BigQuery-safe schema (ASCII) in the same order
    bq_cols = []
    seen_bq = {}
    for c in final_human:
        safe = ascii_bq_safe(c)
        if safe not in seen_bq:
            seen_bq[safe] = 1
            bq_cols.append(safe)
        else:
            seen_bq[safe] += 1
            bq_cols.append(f"{safe}_{seen_bq[safe]}")

    return out_path, bq_cols

# =========================
#        WORKFLOW
# =========================

def wrapper_upload_to_google_cloud(
    drive,
    user_creds,
    excel_path: str,
    table: str,
    info: str,
    *,
    # New: pass sheets service and IMPORTRANGE config; used only when we want Klubtagsag
    sheets_service=None,
    add_klubtagsag: bool = False,
    importrange_source_sheet_id: Optional[str] = None,
    importrange_source_range: Optional[str] = None,
) -> None:
    # Clean headers (keep Hungarian visually), and build BigQuery-safe schema
    cleaned_xlsx, bq_cols = sanitize_excel_headers_for_bq(excel_path, output_name="napi.xlsx")

    # Upload to Drive as Google Sheet; keep Hungarian title, spaces -> _
    desired_title = f"{info}"
    sheet_id, _ = upload_to_google_drive(
        drive=drive,
        excel_path=cleaned_xlsx,
        info=info,
        desired_title=desired_title
    )

    # If asked, create Klubtagsag tab and set IMPORTRANGE on the Google Sheet
    if add_klubtagsag:
        if sheets_service is None:
            sheets_service = build("sheets", "v4", credentials=user_creds)
        if not importrange_source_sheet_id:
            importrange_source_sheet_id = GS_SOURCE_SHEET_ID
        if not importrange_source_range:
            importrange_source_range = GS_KLUBTAGSAG_SOURCE_RANGE

        set_klubtagsag_importrange(
            sheets_service=sheets_service,
            spreadsheet_id=sheet_id,
            source_sheet_id=importrange_source_sheet_id,
            source_range=importrange_source_range,
        )

        set_korrigalt_query_sheet(
            sheets_service=sheets_service,
            spreadsheet_id=sheet_id
        )

        create_afa_kulcsok_sheet(sheets_service=sheets_service, spreadsheet_id=sheet_id)

    # Create external table with explicit schema (no autodetect)
    create_external_table(
        sheet_id=sheet_id,
        table=table,
        user_creds=user_creds,
        info=info,
        provided_bq_cols=bq_cols
    )


def create_table_pointing_to_yearly_summary(drive, user_creds, sheets_service) -> None:
    base_dir = os.getenv("DOWNLOAD_DIR")
    excel_path: str = os.path.join(base_dir, f"year-{datetime.today().year}.xlsx")

    wrapper_upload_to_google_cloud(
        drive=drive,
        user_creds=user_creds,
        excel_path=excel_path,
        table=f"yearly-summary",
        info=f"yearly-summary",
        sheets_service=sheets_service
    )

def create_table_pointing_to_daily_summary(drive, user_creds, sheets_service) -> None:
    base_dir: str = os.getenv("DOWNLOAD_DIR")
    excel_path: str = os.path.join(base_dir, "days", f"daily-summary.xlsx")

    wrapper_upload_to_google_cloud(
        drive=drive,
        user_creds=user_creds,
        excel_path=excel_path,
        table=f"daily-summary",
        info=f"daily-summary",
        sheets_service=sheets_service
    )

def main_upload(drive, user_creds, folder: str | None=None) -> None:
    base_dir = os.getenv("DOWNLOAD_DIR")
    excel_path_overall: str = os.path.join(folder, f"year-{datetime.today().strftime('%Y')}.xlsx")
    excel_path_daily: str = os.path.join(folder, "daily-summary.xlsx")

    file_overall: str = os.path.join(base_dir, excel_path_overall)
    file_daily: str = os.path.join(base_dir, excel_path_daily)

    if not os.path.exists(file_overall):
        print("File not found:", file_overall)
        raise FileNotFoundError(file_overall)

    delete_prev_google_drive_files(drive=drive, webshop=folder)

    sheets_service = build("sheets", "v4", credentials=user_creds)

    # Upload the YEAR workbook and add Klubtagsag IMPORTRANGE on the Google Sheet
    wrapper_upload_to_google_cloud(
        drive=drive,
        user_creds=user_creds,
        excel_path=file_overall,
        table=f"{folder}{"-o-h" if folder else "Oktobertol"}",
        info=f"{folder} year-{datetime.today().strftime('%Y')}",
        sheets_service=sheets_service,
        add_klubtagsag=True,  # <-- this creates the Klubtagsag tab + formula
        importrange_source_sheet_id=GS_SOURCE_SHEET_ID,
        importrange_source_range=GS_KLUBTAGSAG_SOURCE_RANGE,
    )

    wrapper_upload_to_google_cloud(
        drive=drive,
        user_creds=user_creds,
        excel_path=file_daily,
        table=f"{folder}{"-n-h" if folder else "Napi"}",
        info=f"{folder} daily_summary",
        sheets_service=sheets_service,
        add_klubtagsag=False
    )

def delete_all_contents(root_folder: str) -> None:
    if not os.path.isdir(root_folder):
        raise NotADirectoryError(f"Not a folder: {root_folder}")

    for name in os.listdir(root_folder):
        full_path = os.path.join(root_folder, name)
        if os.path.isdir(full_path):
            shutil.rmtree(full_path)
            print(f"🗑️ Deleted folder: {full_path}")
        else:
            os.remove(full_path)
            print(f"🗑️ Deleted file:   {full_path}")


def main():
    user_creds: Credentials = get_oauth_credentials()

    drive = build("drive", "v3", credentials=user_creds)

    for folder in os.listdir(os.getenv("DOWNLOAD_DIR")):
        main_upload(drive=drive, user_creds=user_creds, folder=folder)

if __name__ == "__main__":
    main()

    # delete_all_contents(os.getenv("DOWNLOAD_DIR"))

    print("Done!")

