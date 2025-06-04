# main.py
import os
import base64
import pdfplumber
import re
import pandas as pd
import csv
import google.auth
import json
from datetime import datetime
from google.cloud import storage, bigquery
from google.cloud.bigquery import DatasetReference
from googleapiclient.discovery import build
from flask import Flask, request
from google.oauth2.credentials import Credentials
from google.cloud import secretmanager
from google.auth.transport.requests import Request

# --------------------------
# 1. Autenticação automática Gmail e get_secret
# --------------------------
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_token_from_secret(secret_id: str, project_id: str = None) -> str:
    sm_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = sm_client.access_secret_version(name=name)
    return response.payload.data.decode('UTF-8')

def authenticate_gmail():
    token_json = get_token_from_secret("gmail_token_json", project_id=os.environ["GOOGLE_CLOUD_PROJECT"])
    creds = Credentials.from_authorized_user_info(info=json.loads(token_json), scopes=SCOPES)
    # Auto-refresh if expired
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build('gmail', 'v1', credentials=creds)

# -----------------------------------
# 2. Obter PDF do último email CGD
# -----------------------------------
def get_latest_pdf_attachment(service, sender_email, download_path):
    query = f"from:{sender_email} has:attachment"
    results = service.users().messages().list(userId='me', q=query, maxResults=1).execute()
    messages = results.get('messages', [])

    if not messages:
        print("Nenhum e-mail encontrado.")
        return None

    msg_id = messages[0]['id']
    msg = service.users().messages().get(userId='me', id=msg_id).execute()
    payload = msg.get("payload", {})

    def find_pdf_attachment(parts):
        for part in parts:
            if part.get("filename", "").endswith(".pdf") and "attachmentId" in part.get("body", {}):
                return part
            if part.get("parts"):
                found = find_pdf_attachment(part["parts"])
                if found:
                    return found
        return None

    attachment_part = find_pdf_attachment(payload.get("parts", []))

    if attachment_part:
        attachment_id = attachment_part["body"]["attachmentId"]
        attachment = service.users().messages().attachments().get(
            userId='me', messageId=msg_id, id=attachment_id
        ).execute()

        file_data = base64.urlsafe_b64decode(attachment['data'])
        file_path = os.path.join(download_path, attachment_part["filename"])

        with open(file_path, "wb") as f:
            f.write(file_data)

        print(f"[\u2713] PDF guardado: {file_path}")
        return file_path

    print("Nenhum PDF encontrado no e-mail.")
    return None

# -----------------------------
# 3. Parsing do PDF com plumber
# -----------------------------
def parse_pdf(loc):
    start_text = "-------------------" # 3.1. I use this strings to guide pdfplumber to limit parts of the pdf that I want
    stop_text = "--------------------" # 3.1. I use this strings to guide pdfplumber to limit parts of the pdf that I want
    transaction_pattern = re.compile(r"^\d{4}-\d{2}-\d{2} \d{4}-\d{2}-\d{2} .+ [-+]?\d[\d\.]*,\d{2} [-+]?\d[\d\.]*,\d{2}$")

    extract = False
    skip_next_line = False
    extracted_lines = []

    with pdfplumber.open(loc) as pdf:
        for page in pdf.pages:
            lines = page.extract_text().split("\n")
            for line in lines:
                if start_text in line:
                    extract = True
                    skip_next_line = True
                    continue
                if stop_text in line:
                    extract = False
                    break
                if extract:
                    if skip_next_line:
                        skip_next_line = False
                        continue
                    if transaction_pattern.match(line.strip()):
                        extracted_lines.append(line.strip())

    table_data = []
    for line in extracted_lines:
        columns = line.split()
        if len(columns) >= 5:
            data_move = datetime.strptime(columns[0], "%Y-%m-%d").strftime("%Y%m%d")
            data_valor = datetime.strptime(columns[1], "%Y-%m-%d").strftime("%Y%m%d")
            descricao = " ".join(columns[2:-2])
            valor = columns[-2].replace(",", ".")
            saldo = columns[-1].replace(",", ".")
            inserted_date = datetime.today().strftime('%Y%m%d')
            table_data.append([int(data_move), int(data_valor), descricao, float(valor), float(saldo), int(inserted_date)])

    df = pd.DataFrame(table_data, columns=["DATA_MOVE", "DATA_VALOR", "DESCRICAO", "VALOR", "SALDO", "INSERTED_DATE"])
    return df

# -----------------------------
# 4. Upload CSV para GCS
# -----------------------------
def upload_df_to_gcs(df, bucket_name, destination_blob_name):
    temp_csv = "/tmp/temp_output.csv"
    df.to_csv(temp_csv, index=False, encoding='utf-8', sep=';', quoting=csv.QUOTE_MINIMAL)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(temp_csv)
    print(f"[\u2713] Upload para GCS feito: gs://{bucket_name}/{destination_blob_name}")

# -----------------------------
# 5. Load para BigQuery
# -----------------------------
def upload_gcs_to_bigquery(bucket_name, source_blob_name, dataset_id, table_id, project_id):
    client = bigquery.Client()
    dataset_ref = DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        field_delimiter=';',
        autodetect=False,
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("DATA_MOVE", "INTEGER"),
            bigquery.SchemaField("DATA_VALOR", "INTEGER"),
            bigquery.SchemaField("DESCRICAO", "STRING"),
            bigquery.SchemaField("VALOR", "FLOAT"),
            bigquery.SchemaField("SALDO", "FLOAT"),
            bigquery.SchemaField("INSERTED_DATE", "INTEGER"),
        ],
    )

    uri = f"gs://{bucket_name}/{source_blob_name}"
    print(f"[\u2139\ufe0f] A carregar URI: {uri} para {dataset_id}.{table_id}")

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"[\u2713] Dados carregados para BigQuery: {dataset_id}.{table_id}")

# -----------------------------
# 6. Entry point da Cloud Function
# -----------------------------

def hello_pubsub(event, context):
    sender = "your sender here"
    bucket_name = "meu-bucket" # 6.1. You need to create a bucket inside Cloud Storadge
    destination_blob_name = f"extrato_{datetime.today().strftime('%Y%m%d')}.csv"
    dataset_id = "CGD_DATA" # 6.1. Your dataset_id inside Google Bigquery
    table_id = "CGD_BASE_DATA" # 6.1. Your table_id inside Google Bigquery
    project_id = "third-carving-459311-j9" # 6.1. Your project_id

    service = authenticate_gmail()
    pdf_path = get_latest_pdf_attachment(service, sender, "/tmp")

    if pdf_path:
        df = parse_pdf(pdf_path)
        upload_df_to_gcs(df, bucket_name, destination_blob_name)
        upload_gcs_to_bigquery(bucket_name, destination_blob_name, dataset_id, table_id, project_id)
    else:
        print("[!] Nenhum PDF processado.")

# -----------------------------
# 7. Servidor Flask com Debug de Secret
# -----------------------------
from flask import Flask, request
import traceback, json, os

app = Flask(__name__)

@app.route('/', methods=['POST'])
def run_pipeline():
    event = request.get_json(silent=True)
    context = {}
    try:
        # Debug do token
        token_json = get_token_from_secret("gmail_token_json", project_id=os.environ["GOOGLE_CLOUD_PROJECT"])
        print("=== DEBUG: token_json START ===")
        print(token_json)
        print("=== DEBUG: token_json END ===")

        token_info = json.loads(token_json)
        print("=== DEBUG: token_info keys ===", list(token_info.keys()))

        # Autenticação e pipeline
        service = authenticate_gmail()
        hello_pubsub(event, context)
        return 'OK', 200

    except Exception:
        print("=== START TRACEBACK ===")
        traceback.print_exc()
        print("=== END TRACEBACK ===")
        return 'Internal Server Error', 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# -----------------------------
# 8. Lançar servidor (obrigatório no Cloud Run)
# -----------------------------
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
