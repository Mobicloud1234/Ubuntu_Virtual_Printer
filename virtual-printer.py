#!/usr/bin/env python3
import json
import os
import re
import socket
import time
import sys
import shutil
import logging
import platform
import subprocess
from pathlib import Path
from datetime import datetime

from botocore.exceptions import NoCredentialsError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import cups
import boto3
import pandas as pd

# ========== AWS Configuration ==========

AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
AWS_BUCKET_NAME = 'excelmybucket'
AWS_REGION = 'ap-south-1'

s3 = boto3.client('s3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

textract = boto3.client('textract',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)
# ========== Globals ==========
PDF_DIR = os.path.expanduser("~/PDF_Prints")
FAILED_UPLOADS_PATH = os.path.expanduser("~/failed_uploads.json")
SYSTEM_NAME = platform.node()
TODAY = datetime.now().strftime("%Y-%m-%d")
# EXCEL_PATH = os.path.expanduser(f"~/Downloads/{SYSTEM_NAME}_{TODAY}.xlsx")

# ========== Logging ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('virtual_printer.log')
    ]
)

# ========== Internet Check ==========
def is_connected():
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        return True
    except OSError:
        return False

# ========== Textract + Excel ==========
def extract_text_textract(bucket, key):
    return textract.analyze_document(
        Document={'S3Object': {'Bucket': bucket, 'Name': key}},
        FeatureTypes=["FORMS", "TABLES"]
    )

def get_kv_map(response):
    kvs, blocks = {}, response['Blocks']
    block_map = {b['Id']: b for b in blocks}
    for b in blocks:
        if b['BlockType'] == 'KEY_VALUE_SET' and 'KEY' in b.get('EntityTypes', []):
            key, value = "", ""
            for rel in b.get('Relationships', []):
                if rel['Type'] == 'CHILD':
                    key = ' '.join([block_map[i]['Text'] for i in rel['Ids'] if 'Text' in block_map[i]])
                elif rel['Type'] == 'VALUE':
                    for val_id in rel['Ids']:
                        val_block = block_map[val_id]
                        if 'Relationships' in val_block:
                            for val_rel in val_block['Relationships']:
                                if val_rel['Type'] == 'CHILD':
                                    value = ' '.join([block_map[i]['Text'] for i in val_rel['Ids'] if 'Text' in block_map[i]])
            if key:
                kvs[key.strip()] = value.strip()
    return kvs

def match_key(kvs, possibilities):
    for key in kvs:
        for p in possibilities:
            if p.lower() in key.lower():
                return kvs[key]
    return ""


def extract_invoice_fields(kvs):
    return {
        "Invoice Number": match_key(kvs, ["Invoice No", "Invoice Number"]),
        "Invoice Date": match_key(kvs, ["Dated", "Invoice Date",'Date']),
        "GSTIN": match_key(kvs, ["GSTIN"]),
        "Buyer Name": match_key(kvs, ["Buyer","BILLED TO","Bill to","Buyer (Bill to)","Party", "Customer", "Consignee"]),
        "Buyer Contact": match_key(kvs, ["Mobile", "Contact"]),
        "Total Amount" :match_key(kvs, ["Total","Total Amount", "GrandTotal", "Invoice Total", "Amount Payable"]),
        "HSN Code": match_key(kvs, ["HSN", "HSN/SAC"]),
        "CGST": match_key(kvs, ["CGST"]),
        "SGST": match_key(kvs, ["SGST"]),
        "Bank Name": match_key(kvs, ["Bank Name"]),
        "Account Number": match_key(kvs, ["Account No", "A/c No"]),
        "IFSC Code": match_key(kvs, ["IFSC", "IFSC Code"]),
        "QUANTITY": match_key(kvs, ["Quantity", "Qty"]),
        "DESCRIPTION": match_key(kvs, ["Description of Goods", "Description"]),
    }

def save_to_excel(data, path):
    try:

        os.makedirs(os.path.dirname(path), exist_ok=True)
        df_new = pd.DataFrame([data])

        if os.path.exists(path):
            df_existing = pd.read_excel(path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new

        df_combined.to_excel(path, index=False, sheet_name="InvoiceData")
        logging.info(f"✅ Excel updated: {path}")

        # Upload Excel to S3
        try:
            s3_key = f"excel_logs/{TODAY}/{os.path.basename(path)}"
            s3.upload_file(path, "exceldatastore", s3_key)
            logging.info(f"✅ Excel uploaded to S3 as: {s3_key}")
        except Exception as e:
            logging.error(f"❌ Failed to upload Excel to S3: {e}")

    except Exception as e:
        logging.error(f"🚨 Error saving Excel: {e}")



# ========== Failed Uploads ==========
def load_failed_uploads():
    if os.path.exists(FAILED_UPLOADS_PATH):
        with open(FAILED_UPLOADS_PATH, 'r') as f:
            return json.load(f)
    return []

def save_failed_uploads(data):
    with open(FAILED_UPLOADS_PATH, 'w') as f:
        json.dump(data, f, indent=2)

# ========== Upload ==========
def upload_pdf_to_s3(pdf_path, created_time=None):
    try:
        filename = os.path.basename(pdf_path)
        if not created_time:
            created_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        date_folder = created_time.split("_")[0]
        s3_key = f"{date_folder}/{created_time}_{filename}"
        s3.upload_file(pdf_path, AWS_BUCKET_NAME, s3_key)
        logging.info(f"✅ Uploaded to S3: {s3_key}")
        return s3_key
    except NoCredentialsError as e:
        logging.error(f"❌ AWS credentials not found: {e}")
        return None
    except Exception as e:
        logging.error(f"❌ Upload error: {e}")
        return None

# ========== Retry ==========
def retry_failed_uploads():
    if not is_connected():
        return
    failed = load_failed_uploads()

    still_failed = []
    for item in failed:
        pdf_path = item['path']
        created_time = item.get('created_time')
        if os.path.exists(pdf_path):
            s3_key = upload_pdf_to_s3(pdf_path, created_time)
            if s3_key:
                try:
                    response = extract_text_textract(AWS_BUCKET_NAME, s3_key)
                    kvs = get_kv_map(response)
                    data = extract_invoice_fields(kvs)
                    if any(data.values()):
                        excel_dir=r"/home/mobicloud/Excel_Files"
                        os.makedirs(excel_dir,exist_ok=True)
                        excel_file_path=os.path.join(excel_dir,'invoices.xlsx')
                        save_to_excel(data, excel_file_path)
                        logging.info("save extracted data to excel")

                except Exception as e:
                    logging.error(f"❌ Textract failed for retry {pdf_path}: {e}")
                    still_failed.append(item)
            else:
                still_failed.append(item)
        else:
            logging.warning(f"❌ File not found for retry: {pdf_path}")
    save_failed_uploads(still_failed)


class VirtualPrinter:
    def __init__(self, printer_name="zxcv", output_dir=None):
        self.printer_name = printer_name
        if output_dir is None:
            self.output_dir = os.path.expanduser("~/PDF_Prints")
        else:
            self.output_dir = os.path.expanduser(output_dir)


        os.makedirs(self.output_dir, exist_ok=True)
        os.chmod(self.output_dir, 0o777)

        self.conn = cups.Connection()

    def setup_printer(self):
        try:
            self.conn.deletePrinter(self.printer_name)
        except Exception:
            pass

        cups_pdf_conf = """Out ${HOME}/PDF_Prints
        Label 1
        Log /var/log/cups
        Resolution 300
        """
        config_path = os.path.expanduser("~/cups-pdf.conf")
        with open(config_path, 'w') as f:
            f.write(cups_pdf_conf)

        os.system(f'sudo lpadmin -p {self.printer_name} -v cups-pdf:/ -m lsb/usr/cups-pdf/CUPS-PDF_opt.ppd -E')
        # os.system(f'sudo lpadmin -p {self.printer_name} -v cups-pdf:/ -m /usr/share/ppd/cups-pdf/CUPS-PDF_opt.ppd -E')

        self.conn.enablePrinter(self.printer_name)
        self.conn.acceptJobs(self.printer_name)
        os.system('sudo systemctl restart cups')
        time.sleep(2)
        logging.info(f"Printer {self.printer_name} has been set up")

class PDFHandler(FileSystemEventHandler):
    def __init__(self, output_dir, physical_printer=None, retention_days=7):
        self.output_dir = output_dir
        self.physical_printer = physical_printer
        self.retention_days = retention_days
        self.pdf_dir = os.path.expanduser("~/PDF_Prints")

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.pdf'):
            try:
                time.sleep(1)  # Wait for full write

                # Clean the file name
                raw_name = os.path.basename(event.src_path)
                clean_name = raw_name.split('__')[0]
                timestamp = time.strftime("%H-%M-%S")
                filename = f"{clean_name}"  # or just clean_name if no timestamp
                print(filename)

                # Create dated folder
                now = datetime.now()
                date_folder = now.strftime("%Y/%m/%d")
                dated_output_dir = os.path.join(self.output_dir, date_folder)
                os.makedirs(dated_output_dir, exist_ok=True)

                dest_path = os.path.join(dated_output_dir, filename)

                shutil.move(event.src_path, dest_path)
                os.chmod(dest_path, 0o644)
                #
                #
                created_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                # src_path = event.src_path
                # filename = os.path.basename(src_path)
                # dest_path = os.path.join(PDF_DIR, filename)
                # shutil.move(src_path, dest_path)
                # os.chmod(dest_path, 0o644)

                logging.info(f"Moved PDF to: {dest_path}")
                if self.physical_printer:
                    subprocess.run(["lp", "-d", self.physical_printer, dest_path])
                    logging.info(f"Forwarded to printer: {self.physical_printer}")
                else:
                    logging.warning("No physical printer configured. Skipping forwarding.")

                if is_connected():
                    s3_key = upload_pdf_to_s3(dest_path, created_time)
                    if s3_key:
                        response = extract_text_textract(AWS_BUCKET_NAME, s3_key)
                        kvs = get_kv_map(response)
                        data = extract_invoice_fields(kvs)
                        if any(data.values()):
                            excel_dir = r"/home/mobicloud/Excel_Files"
                            os.makedirs(excel_dir, exist_ok=True)
                            excel_file_path = os.path.join(excel_dir, 'invoices.xlsx')
                            save_to_excel(data, excel_file_path)
                        else:

                            logging.warning("⚠️ No valid data extracted.")
                    else:
                        raise Exception("Upload failed")
                else:
                    raise Exception("Offline")
            except Exception as e:
                logging.error(f"❌ Error processing PDF: {e}")
                failed_list = load_failed_uploads()
                failed_list.append({
                    "path": dest_path,
                    "created_time": created_time
                })
                save_failed_uploads(failed_list)

    def delete_old_pdfs(self):
        cutoff = time.time() - (self.retention_days * 86400)
        for root, dirs, files in os.walk(self.output_dir):
            for file in files:
                if file.endswith('.pdf'):
                    file_path = os.path.join(root, file)
                    try:
                        if os.path.getmtime(file_path) < cutoff:
                            os.remove(file_path)
                            logging.info(f"Deleted old PDF: {file_path}")
                    except Exception as e:
                        logging.error(f"Error deleting file {file_path}: {e}")


def detect_physical_printer(exclude_name):
    conn = cups.Connection()
    printers = conn.getPrinters()
    for name, attrs in printers.items():

        if name != exclude_name and "PDF" not in name.upper():
            if attrs.get("printer-state") == 3:  # 4 = idle
                return name
    for name in printers:
        if name != exclude_name and "PDF" not in name.upper():
            return name
    return None

# ----------------- Main Entry ----------------- #

def main():
    printer = VirtualPrinter()

    try:
        printer.setup_printer()

        # Set manually or use auto-detection
        # physical_printer = "HP-LaserJet-1020-2"
        physical_printer = detect_physical_printer(printer.printer_name)

        if physical_printer:
            logging.info(f"Detected physical printer: {physical_printer}")

        else:

            logging.warning("No physical printer detected.")

        observer = Observer()
        pdf_dir = os.path.expanduser("~/PDF_Prints")
        os.makedirs(pdf_dir, exist_ok=True)

        handler = PDFHandler(printer.output_dir, physical_printer)
        observer.schedule(handler, pdf_dir, recursive=False)
        observer.start()


        logging.info(f"Watching for PDFs in: {pdf_dir}")
        logging.info("Virtual printer system is running. Press Ctrl+C to exit.")

        while True:
            time.sleep(1)
            retry_failed_uploads()

    except KeyboardInterrupt:
        observer.stop()
        logging.info("Shutting down virtual printer system.")

    observer.join()

if __name__ == "__main__":
    main()
