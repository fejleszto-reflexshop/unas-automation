import os
import shutil
from http.client import HTTPException
from typing import Optional
import re

import pandas as pd
import unicodedata
import logging

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.cloud import bigquery
import time, random
from googleapiclient.errors import HttpError

from datetime import datetime, date

from dotenv import load_dotenv
load_dotenv()

# =========================
#          CONFIG
# =========================

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Create log file (append mode, one file per day)
log_file = os.path.join(LOG_DIR, f"cloud_{datetime.now().strftime('%Y-%m-%d')}.log")

logging.basicConfig(
    level=logging.INFO,  # INFO = normal messages; use DEBUG for more verbosity
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode="a", encoding="utf-8"),  # append instead of overwrite
        logging.StreamHandler()  # show logs also in terminal
    ]
)

logger = logging.getLogger(__name__)
logger.info("Logging initialized -> %s", log_file)

# Google Drive / Sheets
PARENT_FOLDER_ID: Optional[str] = None
MAKE_LINK_VIEWABLE = True
BASE_SHEET_NAME = "Sheet1"  # the tab we overwrite with raw data

# Google Sheets IMPORTRANGE source (env)
GS_SOURCE_SHEET_ID = os.getenv("GS_SOURCE_SHEET_ID")  # the spreadsheet you import FROM
GS_KLUBTAGSAG_SOURCE_RANGE = os.getenv("GS_KLUBTAGSAG_SOURCE_RANGE")

# BigQuery
PROJECT_ID  = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
DATASET     = os.getenv("GOOGLE_CLOUD_DATASET")
BQ_LOCATION = os.getenv("GOOGLE_CLOUD_BQ_LOCATION")

SHEET_RANGE: Optional[str] = None
SKIP_ROWS = 1

# IMPORTANT: explicit schema for external tables unless omitted
AUTO_DETECT_SCHEMA = False

# OAuth files
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"

# Scopes
SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/bigquery",
]

RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}
def execute_with_retry(request, *, retries: int = 6, base_delay: float = 0.8, jitter: float = 0.4):
    attempt = 0
    while True:
        try:
            return request.execute()
        except HttpError as e:
            status = getattr(e, "resp", None).status if getattr(e, "resp", None) else None
            if status in RETRYABLE_HTTP_STATUSES and attempt < retries - 1:
                sleep_s = (base_delay * (2 ** attempt)) + random.uniform(0, jitter)
                time.sleep(sleep_s)
                attempt += 1
                continue
            raise

# =========================
#     AUTH / DRIVE / BQ
# =========================

def get_oauth_credentials() -> Credentials:
    token_path = TOKEN_FILE
    creds_path = CREDENTIALS_FILE

    def run_flow() -> Credentials:
        if not os.path.exists(creds_path):
            logger.error("No credentials file found at %s", creds_path)
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

    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            token_scopes = set(getattr(creds, "scopes", []) or [])
            required = set(SCOPES)

            if not required.issubset(token_scopes):
                try:
                    os.remove(token_path)
                except Exception:
                    pass
                creds = run_flow()
        except Exception:
            try:
                os.remove(token_path)
            except Exception:
                pass
            creds = run_flow()
    else:
        creds = run_flow()

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as e:
            if "invalid_scope" in str(e):
                try:
                    os.remove(token_path)
                except Exception:
                    pass
                creds = run_flow()
            else:
                raise

    try:
        if PROJECT_ID:
            creds = creds.with_quota_project(PROJECT_ID)
    except Exception:
        pass

    try:
        with open(token_path, "w") as f:
            f.write(creds.to_json())
    except Exception:
        pass

    return creds

def only_space_to_underscore(name: str) -> str:
    return str(name).replace(" ", "_")

# --------- Drive helpers (re-use existing file to keep static ID)

def replace_sheet_from_dataframe(sheets_service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame):
    # ensure the sheet exists
    meta = execute_with_retry(
        sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id)
    )
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}

    if sheet_name not in titles:
        execute_with_retry(
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
            )
        )

    # clear existing content
    execute_with_retry(
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        )
    )

    # prepare values (headers + rows)
    values = [list(df.columns)]
    values.extend(df.fillna("").astype(str).values.tolist())

    execute_with_retry(
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body={"values": values}
        )
    )

    logger.info(f"✅ Replaced content of {spreadsheet_id} / {sheet_name}")

# --------- External table creation

def canonical_title(name: str) -> str:
    # egységes név: körülvág, többszörös whitespace -> 1 space, majd space->underscore
    name = " ".join(str(name).strip().split())
    return only_space_to_underscore(name)

def find_drive_file_by_name(
    drive_service,
    name: str,
    parent_folder_id: Optional[str] = None,
    mime_type: str = "application/vnd.google-apps.spreadsheet",
) -> Optional[str]:
    # FONTOS: ugyanazzal a kanonikus névvel keressünk, mint amivel létrehozunk

    safe_name = canonical_title(name).replace("'", r"\'")
    q = f"name = '{safe_name}' and trashed = false"

    if mime_type:
        q += f" and mimeType = '{mime_type}'"
    if parent_folder_id:
        q += f" and '{parent_folder_id}' in parents"

    resp = execute_with_retry(
        drive_service.files().list(q=q, fields="files(id,name)", pageSize=1000)
    )
    files = resp.get("files", [])

    return files[0]["id"] if files else None

def upsert_sheet_file_and_overwrite_sheet1(
    drive_service,
    sheets_service,
    excel_path: str,
    desired_title: str,
    parent_folder_id: Optional[str],
    make_link_viewable: bool
) -> str:
    """
    Ha létezik a (kanonizált nevű) spreadsheet -> visszakeresi és a BASE_SHEET_NAME tartalmát felülírja.
    Különben új táblát hoz létre ezzel a NÉVVEL (ugyanez a kanonizált név).
    """
    canonical_name = canonical_title(desired_title)

    # 1) Próbáld megtalálni a meglévő fájlt EZZEL a névvel
    existing_id = find_drive_file_by_name(drive_service, canonical_name, parent_folder_id)

    # Excel első munkalapjának adatát töltjük a BASE_SHEET_NAME-be
    df = pd.read_excel(excel_path, sheet_name=0)

    if existing_id:
        replace_sheet_from_dataframe(sheets_service, existing_id, BASE_SHEET_NAME, df)
        logger.info(f"♻️  Meglévő táblázat felhasználva: {canonical_name}  (id={existing_id})")

        return existing_id

    # 2) Ha nincs, új Google Sheettel indulunk – ugyanazzal a névvel
    file_metadata = {
        "name": canonical_name,
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

    if make_link_viewable:
        drive_service.permissions().create(
            fileId=sheet_id,
            body={"role": "reader", "type": "anyone"},
        ).execute()

    logger.info(f"🆕 Új spreadsheet létrehozva: {canonical_name} → https://docs.google.com/spreadsheets/d/{sheet_id}")

    return sheet_id

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
    """
    Robust external table creation for Google Sheets.
    - If provided_bq_cols is None/empty -> force autodetect True (both public prop + raw properties).
    - If provided_bq_cols is given -> set identical schema on table and external config, disable autodetect.
    """
    client = bigquery.Client(project=project_id, location=location, credentials=credentials)
    table_id = f"{project_id}.{dataset}.{table}"

    try:
        source_fmt = bigquery.SourceFormat.GOOGLE_SHEETS
    except Exception:
        source_fmt = "GOOGLE_SHEETS"

    external_config = bigquery.ExternalConfig(source_fmt)
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    external_config.source_uris = [sheet_url]

    # GoogleSheetsOptions
    try:
        gs_opts = bigquery.GoogleSheetsOptions()
        if sheet_range:
            gs_opts.range = sheet_range
        if skip_rows:
            gs_opts.skip_leading_rows = skip_rows
        external_config.options = gs_opts
    except Exception:
        edc = external_config._properties.setdefault("externalDataConfiguration", {})
        gso = edc.setdefault("googleSheetsOptions", {})
        edc["sourceFormat"] = "GOOGLE_SHEETS"
        if sheet_range:
            gso["range"] = sheet_range
        if skip_rows:
            edc["skipLeadingRows"] = skip_rows

    table_obj = bigquery.Table(table_id)
    table_obj.external_data_configuration = external_config

    edc = external_config._properties.setdefault("externalDataConfiguration", {})

    if provided_bq_cols and len(provided_bq_cols) > 0:
        # Explicit STRING schema everywhere
        bq_schema = [bigquery.SchemaField(name, "STRING") for name in provided_bq_cols]
        table_obj.schema = bq_schema
        try:
            external_config.schema = bq_schema
        except AttributeError:
            edc["schema"] = {"fields": [{"name": f.name, "type": f.field_type} for f in bq_schema]}
        # Ensure autodetect OFF
        try:
            external_config.autodetect = False
        except Exception:
            pass
        edc["autodetect"] = False
    else:
        # No schema -> FORCE autodetect True
        try:
            external_config.autodetect = True
        except Exception:
            pass
        edc["autodetect"] = True

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
    # kept for backward compatibility; not used in overwrite workflow
    sheet_id = upsert_sheet_file_and_overwrite_sheet1(
        drive_service=drive,
        sheets_service=build("sheets", "v4", credentials=get_oauth_credentials()),
        excel_path=excel_path,
        desired_title=desired_title or info,
        parent_folder_id=PARENT_FOLDER_ID,
        make_link_viewable=MAKE_LINK_VIEWABLE
    )
    sheet_link = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    logger.info(f"✅ Google Sheet {info} ready:", sheet_link)
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
        autodetect=AUTO_DETECT_SCHEMA,
        provided_bq_cols=provided_bq_cols
    )

    logger.info(f"✅ External table {info} created: {created_table.full_table_id}")
    logger.info(f"   Source URI {info}: {source_uri}")

# =========================
#    SHEETS (IMPORTRANGE & FORMULAS)
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
    ensure_sheet_exists(sheets_service, spreadsheet_id, target_tab)
    formula = f'=IMPORTRANGE("https://docs.google.com/spreadsheets/d/{source_sheet_id}";"{source_range}")'
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{target_tab}!{target_cell}",
        valueInputOption="USER_ENTERED",
        body={"values": [[formula]]}
    ).execute()

    logger.info("✅ Klubtagsag IMPORTRANGE set.")

def create_sheet_if_missing(sheets_service, spreadsheet_id: str, sheet_name: str) -> None:
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if sheet_name not in titles:
        request = {"addSheet": {"properties": {"title": sheet_name}}}
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [request]}
        ).execute()

def set_mindenmas_query_sheet(sheets_service, spreadsheet_id: str) -> None:
    """
        Creates a sheet named '<year>-mindenmas' and inserts the given QUERY formulas.
        """
    year_str = str(datetime.today().year)
    sheet_name = f"{year_str}-minden_mas"

    create_sheet_if_missing(sheets_service, spreadsheet_id, sheet_name)

    formula_A1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col1, Col2, Col3 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') "
        "or (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    formula_D1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col18 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') "
        "or (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    formula_E1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col4 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') "
        "or (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    formula_G1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col6, Col7, Col9, Col10, Col8, Col11 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') "
        "or (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    formula_M1 = """=QUERY(ARRAYFORMULA(IFERROR(ÉRTÉK(QUERY(Sheet1!A:S;"select Col17 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'   or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') or (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1))));"select Col1 label Col1 'Termék mennyisége'";0)"""

    formula_N1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col16 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') "
        "or (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    formula_O1 = """=query(ARRAYFORMULA(ÉRTÉK(HELYETTE(QUERY(to_text('Sheet1'!A:S);"select Col18 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') or (not Col16 contains 'WELCOMEPACK' and not Col16 contains 'KLUBEVES' and not Col16 contains 'KLUB3HONAPOS' and not Col16 contains 'KLUB6HONAPOS')";1);".";",")));"select * label Col1 'Termék egységára'")"""

    formula_P1 = """={"Rendelés nettó részösszege";ARRAYFORMULA(HA(M2:M="";"";(M2:M*O2:O)))}"""
    formula_Q1 = """={"Összesített Áfa kulcs";ARRAYFORMULA(ifna(FKERES(N2:N;'ÁFA kulcsok'!A:B;2;HAMIS);""))}"""
    formula_R1 = """={"Rendelés bruttó részösszege";ARRAYFORMULA(HA(M2:M="";"";(KEREK.FEL(P2:P*(1+(Q2:Q/100));1))))}"""

    formula_S1 = """=query(ARRAYFORMULA(ARRAYFORMULA(ÉRTÉK(HELYETTE(QUERY(to_text('Sheet1'!A:S);"select Col12 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') or not (Col16 contains 'WELCOMEPACK' and not Col16 contains 'KLUBEVES' and not Col16 contains 'KLUB3HONAPOS' and not Col16 contains 'KLUB6HONAPOS')";1);".";",")))/DARABHATÖBB(A:A;A:A));"select * label Col1 'Szállítási díj'")"""
    formula_T1 = """=query(ARRAYFORMULA(ARRAYFORMULA(ÉRTÉK(HELYETTE(QUERY(to_text('Sheet1'!A:BB);"select Col13 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') or not (Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1);".";",")))/DARABHATÖBB(A:A;A:A));"select * label Col1 'Fizetési illeték'")"""
    formula_U1 = """=query(ARRAYFORMULA(ARRAYFORMULA(ÉRTÉK(HELYETTE(QUERY(to_text('Sheet1'!A:BB);"select Col14 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') or not (Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1);".";",")))/DARABHATÖBB(A:A;A:A));"select * label Col1 'Kupon összege'")"""
    formula_V1 = """={"Összesen bruttó";ARRAYFORMULA(HA(M2:M="";"";(R2:R+S2:S+T2:T+U2:U)))}"""

    formula_W1 = """=QUERY(Sheet1!A:S;"select Col15 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') or (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)"""
    formula_X1 = """={"összesen nettó";ARRAYFORMULA(HA(A2:A="";"";P2:P+(S2:S/1,27)+(T2:T/1,27)+(U2:U/1,27)))}"""
    formula_Y1 = """=ARRAYFORMULA(HELYETTE(QUERY('Sheet1'!A:BB;"select Col19 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' or not Col7 contains 'Személyesen átvéve' or not Col7 contains 'Részben számlázva, átadva a futárnak' or not Col7 contains 'Előfizetés számlázva') or (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1);"Árukereső Marketplace";"Reflexshop"))"""

    formula_F1 = """=ARRAYFORMULA(
  HA(
    ARRAYFORMULA(
      HA(
        SZÁM(SZÖVEG.KERES(
          "GLS - csomagautomata";
          QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)
        ));
        "GLS - csomagautomata";
        HA(
          SZÁM(SZÖVEG.KERES("GLS - csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
          "GLS - csomagpont";
          HA(
            SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 1."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
            "GLS - Nemzetközi 1.";
            HA(
              SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 2."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
              "GLS - Nemzetközi 2.";
              HA(
                SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 3."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                "GLS - Nemzetközi 3.";
                HA(
                  SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 4."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                  "GLS - Nemzetközi 4.";
                  HA(
                    SZÁM(SZÖVEG.KERES("GLS Futárszolgálat"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                    "GLS Futárszolgálat";
                    HA(
                      SZÁM(SZÖVEG.KERES("MPL csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                      "MPL csomagautomata";
                      HA(
                        SZÁM(SZÖVEG.KERES("MPL házhozszállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                        "MPL házhozszállítás";
                        HA(
                          SZÁM(SZÖVEG.KERES("MPL posta pont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                          "MPL posta pont";
                          HA(
                            SZÁM(SZÖVEG.KERES("MPL postán maradó"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                            "MPL postán maradó";
                            HA(
                              SZÁM(SZÖVEG.KERES("Személyes átvétel - Buda"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                              "Személyes átvétel - Buda";
                              HA(
                                SZÁM(SZÖVEG.KERES("Személyes átvétel - Debrecen"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                "Személyes átvétel - Debrecen";
                                HA(
                                  SZÁM(SZÖVEG.KERES("Személyes átvétel - Pest"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                  "Személyes átvétel - Pest";
                                  HA(
                                    SZÁM(SZÖVEG.KERES("MPL Postán maradó"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                    "MPL postán maradó";
                                    HA(
                                      SZÁM(SZÖVEG.KERES("GLS Csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                      "GLS - csomagpont";
                                      HA(
                                        SZÁM(SZÖVEG.KERES("Express One csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                        "Express One csomagpont";
                                        HA(
                                          SZÁM(SZÖVEG.KERES("Packeta csomagpont és csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                          "Packeta csomagpont és csomagautomata";
                                          HA(
                                            SZÁM(SZÖVEG.KERES("GLS Csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                            "GLS - csomagautomata";
                                            HA(
                                              SZÁM(SZÖVEG.KERES("Express One házhozszállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                              "Express One házhozszállítás";
                                              HA(
                                                SZÁM(SZÖVEG.KERES("Előfizetés szállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                                "Előfizetés";
                                                QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)
                                              )
                                            )
                                          )
                                        )
                                      )
                                    )
                                  )
                                )
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )="";
    HA(N:N="WELCOMEPACK";"GLS Futárszolgálat";"Előfizetés");
    ARRAYFORMULA(
      HA(
        SZÁM(SZÖVEG.KERES("GLS - csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
        "GLS - csomagautomata";
        HA(
          SZÁM(SZÖVEG.KERES("GLS - csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
          "GLS - csomagpont";
          HA(
            SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 1."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
            "GLS - Nemzetközi 1.";
            HA(
              SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 2."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
              "GLS - Nemzetközi 2.";
              HA(
                SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 3."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                "GLS - Nemzetközi 3.";
                HA(
                  SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 4."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                  "GLS - Nemzetközi 4.";
                  HA(
                    SZÁM(SZÖVEG.KERES("GLS Futárszolgálat"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                    "GLS Futárszolgálat";
                    HA(
                      SZÁM(SZÖVEG.KERES("MPL csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                      "MPL csomagautomata";
                      HA(
                        SZÁM(SZÖVEG.KERES("MPL házhozszállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                        "MPL házhozszállítás";
                        HA(
                          SZÁM(SZÖVEG.KERES("MPL posta pont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                          "MPL posta pont";
                          HA(
                            SZÁM(SZÖVEG.KERES("MPL postán maradó"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                            "MPL postán maradó";
                            HA(
                              SZÁM(SZÖVEG.KERES("Személyes átvétel - Buda"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                              "Személyes átvétel - Buda";
                              HA(
                                SZÁM(SZÖVEG.KERES("Személyes átvétel - Debrecen"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                "Személyes átvétel - Debrecen";
                                HA(
                                  SZÁM(SZÖVEG.KERES("Személyes átvétel - Pest"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                  "Személyes átvétel - Pest";
                                  HA(
                                    SZÁM(SZÖVEG.KERES("MPL Postán maradó"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                    "MPL postán maradó";
                                    HA(
                                      SZÁM(SZÖVEG.KERES("GLS Csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                      "GLS - csomagpont";
                                      HA(
                                        SZÁM(SZÖVEG.KERES("Packeta csomagpont és csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                        "Packeta csomagpont és csomagautomata";
                                        HA(
                                          SZÁM(SZÖVEG.KERES("Express One csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                          "Express One csomagpont";
                                          HA(
                                            SZÁM(SZÖVEG.KERES("GLS Csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                            "GLS - csomagautomata";
                                            HA(
                                              SZÁM(SZÖVEG.KERES("Express One házhozszállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                              "Express One házhozszállítás";
                                              HA(
                                                SZÁM(SZÖVEG.KERES("Előfizetés szállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)));
                                                "Előfizetés";
                                                QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (not Col7 contains 'Számlázva, átadva a futárnak' and not Col7 contains 'Személyesen átvéve' and not Col7 contains 'Részben számlázva, átadva a futárnak' and not Col7 contains 'Előfizetés számlázva') and (not Col16 contains 'WELCOMEPACK' or not Col16 contains 'KLUBEVES' or not Col16 contains 'KLUB3HONAPOS' or not Col16 contains 'KLUB6HONAPOS')";1)
                                              )
                                            )
                                          )
                                        )
                                      )
                                    )
                                  )
                                )
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
  )
)
"""

    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": f"{sheet_name}!A1", "values": [[formula_A1]]},
                {"range": f"{sheet_name}!D1", "values": [[formula_D1]]},
                {"range": f"{sheet_name}!E1", "values": [[formula_E1]]},
                {"range": f"{sheet_name}!F1", "values": [[formula_F1]]},
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
            ],
        },
    ).execute()

    logger.info(f"✅ Added sheet '{sheet_name}' with QUERY formulas.")


def set_korrigalt_query_sheet(sheets_service, spreadsheet_id: str) -> None:
    """
    Creates a sheet named '<year>-Korrigalt' and inserts the given QUERY formulas.
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

    # G1 — multiple columns
    formula_G1 = (
        "=QUERY(Sheet1!A:S;"
        "\"select Col6, Col7, Col9, Col10, Col8, Col11 "
        "where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|') "
        "and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') "
        "or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')\";1)"
    )

    formula_M1 = """=QUERY(ARRAYFORMULA(IFERROR(ÉRTÉK(QUERY(Sheet1!A:S;"select Col17 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló' or Col2 contains '|' ) and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' or Col16 contains 'KLUBEVES' or Col16 contains 'KLUB3HONAPOS' or Col16 contains 'KLUB6HONAPOS')";1))));"select Col1 label Col1 'Termék mennyisége'";0)"""

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
    formula_F1 = """=ARRAYFORMULA(
  HA(
    ARRAYFORMULA(
      HA(
        SZÁM(SZÖVEG.KERES(
          "GLS - csomagautomata";
          QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)
        ));
        "GLS - csomagautomata";
        HA(
          SZÁM(SZÖVEG.KERES("GLS - csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
          "GLS - csomagpont";
          HA(
            SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 1."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
            "GLS - Nemzetközi 1.";
            HA(
              SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 2."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
              "GLS - Nemzetközi 2.";
              HA(
                SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 3."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                "GLS - Nemzetközi 3.";
                HA(
                  SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 4."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                  "GLS - Nemzetközi 4.";
                  HA(
                    SZÁM(SZÖVEG.KERES("GLS Futárszolgálat"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                    "GLS Futárszolgálat";
                    HA(
                      SZÁM(SZÖVEG.KERES("MPL csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                      "MPL csomagautomata";
                      HA(
                        SZÁM(SZÖVEG.KERES("MPL házhozszállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                        "MPL házhozszállítás";
                        HA(
                          SZÁM(SZÖVEG.KERES("MPL posta pont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                          "MPL posta pont";
                          HA(
                            SZÁM(SZÖVEG.KERES("MPL postán maradó"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                            "MPL postán maradó";
                            HA(
                              SZÁM(SZÖVEG.KERES("Személyes átvétel - Buda"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                              "Személyes átvétel - Buda";
                              HA(
                                SZÁM(SZÖVEG.KERES("Személyes átvétel - Debrecen"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                "Személyes átvétel - Debrecen";
                                HA(
                                  SZÁM(SZÖVEG.KERES("Személyes átvétel - Pest"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                  "Személyes átvétel - Pest";
                                  HA(
                                    SZÁM(SZÖVEG.KERES("MPL Postán maradó"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                    "MPL postán maradó";
                                    HA(
                                      SZÁM(SZÖVEG.KERES("GLS Csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                      "GLS - csomagpont";
                                      HA(
                                        SZÁM(SZÖVEG.KERES("Express One csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                        "Express One csomagpont";
                                        HA(
                                          SZÁM(SZÖVEG.KERES("Packeta csomagpont és csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                          "Packeta csomagpont és csomagautomata";
                                          HA(
                                            SZÁM(SZÖVEG.KERES("GLS Csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                            "GLS - csomagautomata";
                                            HA(
                                              SZÁM(SZÖVEG.KERES("Express One házhozszállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                              "Express One házhozszállítás";
                                              HA(
                                                SZÁM(SZÖVEG.KERES("Előfizetés szállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                                "Előfizetés";
                                                QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)
                                              )
                                            )
                                          )
                                        )
                                      )
                                    )
                                  )
                                )
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )="";
    HA(N:N="WELCOMEPACK";"GLS Futárszolgálat";"Előfizetés");
    ARRAYFORMULA(
      HA(
        SZÁM(SZÖVEG.KERES("GLS - csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
        "GLS - csomagautomata";
        HA(
          SZÁM(SZÖVEG.KERES("GLS - csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
          "GLS - csomagpont";
          HA(
            SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 1."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
            "GLS - Nemzetközi 1.";
            HA(
              SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 2."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
              "GLS - Nemzetközi 2.";
              HA(
                SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 3."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                "GLS - Nemzetközi 3.";
                HA(
                  SZÁM(SZÖVEG.KERES("GLS - Nemzetközi 4."; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                  "GLS - Nemzetközi 4.";
                  HA(
                    SZÁM(SZÖVEG.KERES("GLS Futárszolgálat"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                    "GLS Futárszolgálat";
                    HA(
                      SZÁM(SZÖVEG.KERES("MPL csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                      "MPL csomagautomata";
                      HA(
                        SZÁM(SZÖVEG.KERES("MPL házhozszállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                        "MPL házhozszállítás";
                        HA(
                          SZÁM(SZÖVEG.KERES("MPL posta pont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                          "MPL posta pont";
                          HA(
                            SZÁM(SZÖVEG.KERES("MPL postán maradó"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                            "MPL postán maradó";
                            HA(
                              SZÁM(SZÖVEG.KERES("Személyes átvétel - Buda"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                              "Személyes átvétel - Buda";
                              HA(
                                SZÁM(SZÖVEG.KERES("Személyes átvétel - Debrecen"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                "Személyes átvétel - Debrecen";
                                HA(
                                  SZÁM(SZÖVEG.KERES("Személyes átvétel - Pest"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                  "Személyes átvétel - Pest";
                                  HA(
                                    SZÁM(SZÖVEG.KERES("MPL Postán maradó"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                    "MPL postán maradó";
                                    HA(
                                      SZÁM(SZÖVEG.KERES("GLS Csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                      "GLS - csomagpont";
                                      HA(
                                        SZÁM(SZÖVEG.KERES("Packeta csomagpont és csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                        "Packeta csomagpont és csomagautomata";
                                        HA(
                                          SZÁM(SZÖVEG.KERES("Express One csomagpont"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                          "Express One csomagpont";
                                          HA(
                                            SZÁM(SZÖVEG.KERES("GLS Csomagautomata"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                            "GLS - csomagautomata";
                                            HA(
                                              SZÁM(SZÖVEG.KERES("Express One házhozszállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                              "Express One házhozszállítás";
                                              HA(
                                                SZÁM(SZÖVEG.KERES("Előfizetés szállítás"; QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)));
                                                "Előfizetés";
                                                QUERY(Sheet1!A:BB;"select Col5 where (Col2 contains 'Alapértelmezett' or Col2 contains 'SAP9-Törzsvásárló'  or Col2 contains '|') and (Col7 contains 'Számlázva, átadva a futárnak' or Col7 contains 'Személyesen átvéve' or Col7 contains 'Részben számlázva, átadva a futárnak' or Col7 contains 'Előfizetés számlázva') or (Col16 contains 'WELCOMEPACK' and Col16 contains 'KLUBEVES' and Col16 contains 'KLUB3HONAPOS' and Col16 contains 'KLUB6HONAPOS')";1)
                                              )
                                            )
                                          )
                                        )
                                      )
                                    )
                                  )
                                )
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
  )
)"""

    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "valueInputOption": "USER_ENTERED",
            "data": [
                {"range": f"{sheet_name}!A1", "values": [[formula_A1]]},
                {"range": f"{sheet_name}!D1", "values": [[formula_D1]]},
                {"range": f"{sheet_name}!E1", "values": [[formula_E1]]},
                {"range": f"{sheet_name}!F1", "values": [[formula_F1]]},
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
            ],
        },
    ).execute()

    logger.info(f"✅ Added sheet '{sheet_name}' with QUERY formulas.")

def make_sheet_first(sheets_service, spreadsheet_id: str, title: str) -> None:
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet = next((s for s in meta.get("sheets", []) if s["properties"]["title"] == title), None)
    if not sheet:
        raise ValueError(f"Sheet not found: {title}")
    sheet_id = sheet["properties"]["sheetId"]
    body = {
        "requests": [{
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "index": 0},
                "fields": "index"
            }
        }]
    }
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    logger.info(f"✅ Sheet '{title}' moved to first position (index=0).")

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

    logger.info(f"✅ Added sheet '{sheet_name}' with IMPORTRANGE formula.")

# ---------- NEW: update JUST Sheet1 for osszefoglalo ----------
def update_sheet1_only_osszefoglalo(
    drive_service,
    user_creds: Credentials,
    excel_path: str,
    osszefoglalo_title: str,
    *,
    parent_folder_id: Optional[str] = PARENT_FOLDER_ID,
    make_link_viewable: bool = MAKE_LINK_VIEWABLE,
    base_sheet_name: str = BASE_SHEET_NAME,
) -> str:
    """
    Updates ONLY Sheet1 in the 'osszefoglalo' spreadsheet.
    - Reuses existing file (static ID) if found by title.
    - If not found, creates it.
    - Does NOT add/alter any other sheets or external tables.
    """
    sheets_service = build("sheets", "v4", credentials=user_creds)

    # reuse existing file by title, else create; then overwrite Sheet1 only
    sheet_id = upsert_sheet_file_and_overwrite_sheet1(
        drive_service=drive_service,
        sheets_service=sheets_service,
        excel_path=excel_path,
        desired_title=osszefoglalo_title,
        parent_folder_id=parent_folder_id,
        make_link_viewable=make_link_viewable,
    )

    logger.info(
        f"✅ Sheet1 updated for '{canonical_title(osszefoglalo_title)}' → "
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    )

    return sheet_id

# =========================
#        WORKFLOW
# =========================

def ascii_bq_safe(name: str) -> str:
    s = unicodedata.normalize('NFKD', name)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r'[^A-Za-z0-9_]', '_', s)
    if re.match(r'^[0-9]', s):
        s = f'col_{s}'
    if not s:
        s = 'col'
    return s

def sanitize_excel_headers_for_bq(in_xlsx: str, sheet_name=0, output_name: str | None = None):
    """
    Keep human/Hungarian headers for the uploaded Sheet copy, but DO NOT use them to
    define an external schema (we keep explicit schema for native loads).
    """
    df = pd.read_excel(in_xlsx, sheet_name=sheet_name)

    mask_named = ~df.columns.to_series().astype(str).str.match(r'^Unnamed')
    df = df.loc[:, mask_named]

    human_cols = [only_space_to_underscore(c) for c in df.columns]
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

    if output_name is None:
        base, ext = os.path.splitext(os.path.basename(in_xlsx))
        output_name = f"{base}_cleaned.xlsx"
    out_path = os.path.join(os.path.dirname(in_xlsx), output_name)
    df.to_excel(out_path, index=False)

    # BQ-safe list (kept in case of native loads or explicit schema)
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

def wrapper_upload_to_google_cloud(
    drive,
    user_creds,
    excel_path: str,
    table: str,
    info: str,
    *,
    sheets_service=None,
    add_klubtagsag: bool = False,
    importrange_source_sheet_id: Optional[str] = None,
    importrange_source_range: Optional[str] = None,
) -> str:
    """
    Overwrite/refresh a fixed spreadsheet (static ID) and (optionally) set up extra tabs,
    then (re)create the external BigQuery table.
    """
    cleaned_xlsx, bq_cols = sanitize_excel_headers_for_bq(excel_path, output_name="napi.xlsx")

    if sheets_service is None:
        sheets_service = build("sheets", "v4", credentials=user_creds)

    desired_title = f"{info}"

    # Reuse existing file if found; otherwise create new, then overwrite the BASE_SHEET_NAME content
    sheet_id = upsert_sheet_file_and_overwrite_sheet1(
        drive_service=drive,
        sheets_service=sheets_service,
        excel_path=cleaned_xlsx,
        desired_title=desired_title,
        parent_folder_id=PARENT_FOLDER_ID,
        make_link_viewable=MAKE_LINK_VIEWABLE
    )

    # Set up optional tabs (keeps them across runs)
    if add_klubtagsag:
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

        # Make '<year>-Korrigalt' first
        year_str = str(datetime.today().year)
        korrigalt_title = f"{year_str}-Korrigalt"
        make_sheet_first(sheets_service, sheet_id, korrigalt_title)

        create_afa_kulcsok_sheet(sheets_service=sheets_service, spreadsheet_id=sheet_id)

    # Create/refresh external table (explicit schema from cleaned headers)
    create_external_table(
        sheet_id=sheet_id,
        table=table,
        user_creds=user_creds,
        info=info,
        provided_bq_cols=bq_cols
    )

    return sheet_id

def delete_all_contents(root_folder: str) -> None:
    if not os.path.isdir(root_folder):
        raise NotADirectoryError(f"Not a folder: {root_folder}")

    for name in os.listdir(root_folder):
        full_path = os.path.join(root_folder, name)
        if os.path.isdir(full_path):
            shutil.rmtree(full_path)
            logger.warning(f"🗑️ Deleted folder: {full_path}")
        else:
            os.remove(full_path)
            logger.warning(f"🗑️ Deleted file:   {full_path}")

# havi adatok
def upload_months_data(drive, user_creds):
    """upload months data. run this function only once a month"""
    base_dir = os.getenv("DOWNLOAD_DIR")

    for folder in os.listdir(os.getenv("DOWNLOAD_DIR")):
        excel_path_overall: str = os.path.join(folder, f"year-{datetime.now().strftime('%Y')}.xlsx")
        file_overall: str = os.path.join(base_dir, excel_path_overall)

        if not os.path.exists(file_overall):
            logger.error("File not found: %s", file_overall)
            raise FileNotFoundError(file_overall)

        # no delete – reuse/overwrite same spreadsheet
        wrapper_upload_to_google_cloud(
            drive=drive,
            user_creds=user_creds,
            excel_path=file_overall,
            table=f"{folder}",
            info=f"{folder} year-{datetime.now().strftime('%Y')}"
        )

# napi adatok
def upload_daily_summary(drive, user_creds) -> None:
    """upload daily stats. run this function daily at 7 am"""
    base_dir = os.getenv("DOWNLOAD_DIR")

    for folder in os.listdir(base_dir):
        excel_path_daily: str = os.path.join(folder, "daily-summary.xlsx")
        file_daily: str = os.path.join(base_dir, excel_path_daily)

        # no delete – reuse/overwrite same spreadsheet
        wrapper_upload_to_google_cloud(
            drive=drive,
            user_creds=user_creds,
            excel_path=file_daily,
            table=f"{folder}-napi",
            info=f"{folder} daily_summary"
        )

    logger.info("Daily summary uploaded")

def create_external_table_for_range(
    sheet_id: str,
    table: str,
    user_creds: Credentials,
    info: str,
    range_a1: str,          # e.g. "'2025-Korrigalt'!A:ZZ"
    skip_rows: int = 1,
    provided_bq_cols: Optional[list[str]] = None
) -> None:
    created_table, source_uri = create_external_table_pointing_to_sheet(
        project_id=PROJECT_ID,
        dataset=DATASET,
        table=table,
        sheet_id=sheet_id,
        credentials=user_creds,
        location=BQ_LOCATION,
        sheet_range=range_a1,
        skip_rows=skip_rows,
        autodetect=AUTO_DETECT_SCHEMA,
        provided_bq_cols=provided_bq_cols  # if None -> autodetect True in helper
    )
    logger.info(f"✅ External table (range only) {info} created: {created_table.full_table_id}")
    logger.info(f"   Source URI {info}: {source_uri}  range={range_a1}")


def upload_year_stats_overall(drive, user_creds) -> None:
    """
    For each webshop folder under DOWNLOAD_DIR:
      - take year-<currentyear>.xlsx
      - find (or create) the '<folder> osszefoglalo' spreadsheet on Drive
      - UPDATE ONLY Sheet1 with the Excel's first sheet
      - ALSO refresh Klubtagsag (IMPORTRANGE), '<year>-Korrigalt' formulas,
        move Korrigalt to first, refresh 'ÁFA kulcsok'
    """
    base_dir = os.getenv("DOWNLOAD_DIR")
    if not base_dir or not os.path.isdir(base_dir):
        raise FileNotFoundError(f"DOWNLOAD_DIR is missing or not a directory: {base_dir}")

    sheets_service = build("sheets", "v4", credentials=user_creds)

    for folder in os.listdir(base_dir):
        excel_path_year = os.path.join(base_dir, folder, f"year-{datetime.today().year}.xlsx")
        if not os.path.exists(excel_path_year):
            logger.error(f"File not found, skipping: {excel_path_year}")
            continue

        osszefoglalo_title = f"{folder} osszefoglalo"

        # 1) Update Sheet1 only (reuse existing spreadsheet)
        sheet_id = update_sheet1_only_osszefoglalo(
            drive_service=drive,
            user_creds=user_creds,
            excel_path=excel_path_year,
            osszefoglalo_title=osszefoglalo_title,
            parent_folder_id=PARENT_FOLDER_ID,
            make_link_viewable=MAKE_LINK_VIEWABLE,
            base_sheet_name=BASE_SHEET_NAME,
        )

        # 2) Klubtagsag import (if env vars provided)
        if GS_SOURCE_SHEET_ID and GS_KLUBTAGSAG_SOURCE_RANGE:
            set_klubtagsag_importrange(
                sheets_service=sheets_service,
                spreadsheet_id=sheet_id,
                source_sheet_id=GS_SOURCE_SHEET_ID,
                source_range=GS_KLUBTAGSAG_SOURCE_RANGE,
            )

        # 3) Refresh Korrigalt sheet formulas (idempotent and non-duplicating)
        set_korrigalt_query_sheet(sheets_service=sheets_service, spreadsheet_id=sheet_id)

        set_mindenmas_query_sheet(sheets_service=sheets_service, spreadsheet_id=sheet_id)

        # 4) Move '<year>-Korrigalt' first
        year_str = str(datetime.today().year)
        make_sheet_first(sheets_service, sheet_id, f"{year_str}-Korrigalt")

        create_afa_kulcsok_sheet(sheets_service=sheets_service, spreadsheet_id=sheet_id)

        logger.info(f"📊 Ready (Sheet1 + Klubtagsag + Korrigalt + ÁFA updated): {folder} → spreadsheet {sheet_id}")

if __name__ == "__main__":
    try:
        user_creds: Credentials = get_oauth_credentials()
        drive = build("drive", "v3", credentials=user_creds)

        logger.info("Google Drive creds/drive created.")
        today = date.today()

        if today.day == 1:
            logger.info("Months data uploaded")
            upload_months_data(drive=drive, user_creds=user_creds)
            upload_year_stats_overall(drive=drive, user_creds=user_creds)

        logger.info("Daily data uploaded")
        upload_daily_summary(drive=drive, user_creds=user_creds)

        logger.info("Done!")
    except HTTPException as http_exception:
        logger.error(http_exception)
    except OSError as os_error:
        logger.error(os_error)
    except Exception as exception:
        logger.error(exception)
