import streamlit as st
import pandas as pd
import io
import time
import math
import json
import base64
import re
from reportlab.lib.pagesizes import A4, mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from pypdf import PdfReader, PdfWriter, Transformation, PageObject

# --- SERVER IMPORTS (Google Drive & QZ Tray) ---
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
import streamlit.components.v1 as components

# --- CONFIGURATION ---
st.set_page_config(page_title="Hike Warehouse Manager", layout="wide")

# --- CONSTANTS ---
CACHE_FILE = "master_data.csv"
HISTORY_FILE = "consignment_history.json"
SENDERS_FILE = "senders.xlsx"
RECEIVERS_FILE = "receivers.xlsx"
TEMPLATE_SINGLE_FILE = "active_listing_single.csv"
TEMPLATE_MULTI_FILE = "active_listing_multi.csv"
SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRdLEddTZgmuUSswPp3A_HM7DGH8UCUWEmqd-cIbbJ7nb_Eq4YvZxO0vjWESlxX-9Y6VWRcVLPFlIVp/pub?gid=0&single=true&output=csv"

# --- ZONES ORDER ---
ZONES_ORDER = ['South', 'West', 'East', 'North']

# --- MAPPING DATA ---
STATE_TO_ZONE = {
    'Arunachal Pradesh': ('east', 'ulub_bts'), 'Assam': ('east', 'ulub_bts'),
    'Nagaland': ('east', 'ulub_bts'), 'Meghalaya': ('east', 'ulub_bts'),
    'Bihar': ('east', 'ulub_bts'), 'West Bengal': ('east', 'ulub_bts'),
    'Odisha': ('east', 'ulub_bts'), 'Chhattisgarh': ('east', 'ulub_bts'),
    'Tripura': ('east', 'ulub_bts'), 'Mizoram': ('east', 'ulub_bts'),
    'Jharkhand': ('east', 'ulub_bts'), 'Manipur': ('east', 'ulub_bts'),
    'Andaman & Nicobar Islands': ('east', 'ulub_bts'), 'Sikkim': ('east', 'ulub_bts'),
    'Haryana': ('north', 'gur_san_wh_nl_01nl'), 'Delhi': ('north', 'gur_san_wh_nl_01nl'),
    'Uttar Pradesh': ('north', 'gur_san_wh_nl_01nl'), 'Uttarakhand': ('north', 'gur_san_wh_nl_01nl'),
    'Rajasthan': ('north', 'gur_san_wh_nl_01nl'), 'Punjab': ('north', 'gur_san_wh_nl_01nl'),
    'Himachal Pradesh': ('north', 'gur_san_wh_nl_01nl'), 'Jammu & Kashmir': ('north', 'gur_san_wh_nl_01nl'),
    'Telangana': ('south', 'malur_bts'), 'Andhra Pradesh': ('south', 'malur_bts'),
    'Karnataka': ('south', 'malur_bts'), 'Kerala': ('south', 'malur_bts'),
    'Tamil Nadu': ('south', 'malur_bts'), 'Puducherry': ('south', 'malur_bts'),
    'Gujarat': ('west', 'bhi_vas_wh_nl_01nl'), 'Maharashtra': ('west', 'bhi_vas_wh_nl_01nl'),
    'Madhya Pradesh': ('west', 'bhi_vas_wh_nl_01nl'), 'Goa': ('west', 'bhi_vas_wh_nl_01nl'),
    'Jammu and Kashmir': ('north', 'gur_san_wh_nl_01nl'),
    'Andaman and Nicobar Islands': ('east', 'ulub_bts'),
    'Pondicherry': ('south', 'malur_bts'),
    'Chandigarh': ('north', 'gur_san_wh_nl_01nl'),
    'Dadra & Nagar Haveli & Daman & Diu': ('west', 'bhi_vas_wh_nl_01nl')
}

# --- GOOGLE DRIVE HANDLER (Replaces Local File System) ---
class DriveHandler:
    @staticmethod
    def get_service():
        if "gcp_service_account" not in st.secrets:
            return None
        creds_dict = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=['https://www.googleapis.com/auth/drive']
        )
        return build('drive', 'v3', credentials=creds)

    @staticmethod
    def upload_file(filename, data, mime_type='application/octet-stream'):
        try:
            service = DriveHandler.get_service()
            if not service: return False
            folder_id = st.secrets["drive_folder_id"]
            
            # Check if file exists
            query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
            results = service.files().list(q=query, fields="files(id)").execute()
            items = results.get('files', [])

            # Ensure data is bytes
            if isinstance(data, str): data = data.encode('utf-8')
            media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, resumable=True)

            if not items:
                meta = {'name': filename, 'parents': [folder_id]}
                service.files().create(body=meta, media_body=media).execute()
            else:
                file_id = items[0]['id']
                service.files().update(fileId=file_id, media_body=media).execute()
            return True
        except Exception as e:
            st.error(f"Drive Error: {e}")
            return False

    @staticmethod
    def download_file(filename):
        try:
            service = DriveHandler.get_service()
            if not service: return None
            folder_id = st.secrets["drive_folder_id"]
            query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
            results = service.files().list(q=query, fields="files(id)").execute()
            items = results.get('files', [])
            
            if not items: return None
            
            request = service.files().get_media(fileId=items[0]['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            return fh.getvalue()
        except Exception:
            return None

    @staticmethod
    def file_exists(filename):
        try:
            service = DriveHandler.get_service()
            if not service: return False
            folder_id = st.secrets["drive_folder_id"]
            query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
            results = service.files().list(q=query, fields="files(id)").execute()
            return len(results.get('files', [])) > 0
        except: return False

# --- DATABASE & HISTORY HELPERS ---
def load_history():
    data_bytes = DriveHandler.download_file(HISTORY_FILE)
    if data_bytes:
        try:
            history = json.loads(data_bytes.decode('utf-8'))
            for h in history:
                if 'data' in h:
                    try: h['data'] = pd.DataFrame(h['data'])
                    except: h['data'] = pd.DataFrame()
                if 'original_data' in h:
                    try: h['original_data'] = pd.DataFrame(h['original_data'])
                    except: h['original_data'] = pd.DataFrame()
                if 'backup_data' in h:
                    try: h['backup_data'] = pd.DataFrame(h['backup_data'])
                    except: h['backup_data'] = pd.DataFrame()
                if 'printed_boxes' not in h: h['printed_boxes'] = []
                if 'task_type' not in h: h['task_type'] = h.get('task_type', 'execution')
                if 'is_booked' not in h: h['is_booked'] = True if h.get('task_type') == 'execution' else False
            return history
        except: return []
    return []

def save_history(history_list):
    serializable_list = []
    for h in history_list:
        h_copy = h.copy()
        if 'data' in h_copy and isinstance(h_copy['data'], pd.DataFrame):
            h_copy['data'] = h_copy['data'].to_dict('records')
        if 'original_data' in h_copy and isinstance(h_copy['original_data'], pd.DataFrame):
            h_copy['original_data'] = h_copy['original_data'].to_dict('records')
        if 'backup_data' in h_copy and isinstance(h_copy['backup_data'], pd.DataFrame):
            h_copy['backup_data'] = h_copy['backup_data'].to_dict('records')
        serializable_list.append(h_copy)
    
    json_str = json.dumps(serializable_list)
    DriveHandler.upload_file(HISTORY_FILE, json_str, 'application/json')

def load_template_db(mode_type):
    fname = TEMPLATE_SINGLE_FILE if mode_type == 'single' else TEMPLATE_MULTI_FILE
    data = DriveHandler.download_file(fname)
    if data: return pd.read_csv(io.BytesIO(data), dtype=str)
    return pd.DataFrame()

def save_template_db(df, mode_type):
    fname = TEMPLATE_SINGLE_FILE if mode_type == 'single' else TEMPLATE_MULTI_FILE
    output = io.BytesIO()
    df.to_csv(output, index=False)
    DriveHandler.upload_file(fname, output.getvalue(), 'text/csv')

def load_address_data(file_path, default_cols):
    data = DriveHandler.download_file(file_path)
    if data: return pd.read_excel(io.BytesIO(data), dtype=str)
    return pd.DataFrame(columns=default_cols)

def save_address_data(file_path, df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    DriveHandler.upload_file(file_path, output.getvalue(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# --- MASTER DATA & SYNC ---
def sync_data():
    try:
        df = pd.read_csv(SHEET_URL, dtype={'EAN': str})
        if 'PPCN' not in df.columns: return False, "Column 'PPCN' missing."
        output = io.BytesIO()
        df.to_csv(output, index=False)
        DriveHandler.upload_file(CACHE_FILE, output.getvalue(), 'text/csv')
        return True, "✅ Master Data Synced!"
    except Exception as e: return False, f"❌ Sync Failed: {e}"

def load_master_data():
    data = DriveHandler.download_file(CACHE_FILE)
    if data: return pd.read_csv(io.BytesIO(data), dtype={'EAN': str})
    return pd.DataFrame()

# --- FILE HELPERS (CLOUD) ---
# We flatten the folder structure for Drive: "c_id/type.pdf" becomes "c_id_type.pdf"
def save_uploaded_file(uploaded_file, c_id, file_type):
    filename = f"{c_id}_{file_type}.pdf"
    DriveHandler.upload_file(filename, uploaded_file.getbuffer(), 'application/pdf')
    return filename

def get_stored_file_exists(c_id, file_type):
    filename = f"{c_id}_{file_type}.pdf"
    return DriveHandler.file_exists(filename)

def get_stored_file_bytes(c_id, file_type):
    filename = f"{c_id}_{file_type}.pdf"
    return DriveHandler.download_file(filename)

def get_merged_labels_bytes(c_id):
    filename = f"{c_id}_merged_labels.pdf"
    return DriveHandler.download_file(filename)

# --- QZ TRAY PRINTING (Browser Based) ---
def qz_tray_print_component(pdf_bytes, printer_name):
    """Injects JavaScript to print the PDF using QZ Tray."""
    if not pdf_bytes: return
    b64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')
    js_code = f"""
    <script src="https://cdn.jsdelivr.net/npm/qz-tray@2.2.4/qz-tray.min.js"></script>
    <script>
    qz.websocket.connect().then(function() {{
        return qz.printers.find("{printer_name}");
    }}).then(function(printer) {{
        var config = qz.configs.create(printer);
        var data = [{{ type: 'pdf', format: 'base64', data: '{b64_pdf}' }}];
        return qz.print(config, data);
    }}).then(function() {{
        // Optional success alert
    }}).catch(function(e) {{
        console.error(e);
        alert("Printing Error: " + e);
    }});
    </script>
    """
    components.html(js_code, height=0, width=0)

def extract_label_pdf_bytes(merged_pdf_bytes, box_index):
    try:
        reader = PdfReader(io.BytesIO(merged_pdf_bytes))
        writer = PdfWriter()
        if box_index >= len(reader.pages): return None
        writer.add_page(reader.pages[box_index])
        out = io.BytesIO()
        writer.write(out)
        return out.getvalue()
    except: return None

# --- PDF & EXCEL GENERATION LOGIC ---
def generate_confirm_consignment_csv(df):
    output = io.BytesIO()
    active_df = df[df['Editable Boxes'] > 0].sort_values(by='SKU Id')
    zero_df = df[df['Editable Boxes'] == 0].sort_values(by='SKU Id')
    rows = []
    box_counter = 1
    for _, row in active_df.iterrows():
        try: num_boxes = int(row['Editable Boxes']); ppcn = int(float(row['PPCN'])) if float(row['PPCN']) > 0 else 1; fsn = row.get('FSN', '')
        except: num_boxes=0; ppcn=1; fsn=''
        nominal_val = 350 * ppcn
        for _ in range(num_boxes):
            rows.append({'BOX NUMBER': box_counter, 'BOX NAME': box_counter, 'LENGTH (cm)': 75, 'BREADTH (cm)': 55, 'HEIGHT (cm)': 40, 'WEIGHT (kg)': 10, 'NOMINAL VALUE (INR)': nominal_val, 'FSN': fsn, 'QUANTITY': ppcn})
            box_counter += 1
    if not zero_df.empty:
        chunk_size = 20
        zero_rows = zero_df.to_dict('records')
        chunks = [zero_rows[i:i + chunk_size] for i in range(0, len(zero_rows), chunk_size)]
        for chunk in chunks:
            for row in chunk:
                rows.append({'BOX NUMBER': box_counter, 'BOX NAME': box_counter, 'LENGTH (cm)': 75, 'BREADTH (cm)': 55, 'HEIGHT (cm)': 40, 'WEIGHT (kg)': 10, 'NOMINAL VALUE (INR)': 350, 'FSN': row.get('FSN', ''), 'QUANTITY': 1})
            box_counter += 1
    pd.DataFrame(rows).to_csv(output, index=False)
    return output.getvalue()

def generate_merged_box_labels(df, c_details, sender, receiver, flipkart_pdf_bytes, progress_bar=None):
    if not flipkart_pdf_bytes: return None
    box_data = []
    active_df = df[df['Editable Boxes'] > 0].sort_values(by='SKU Id')
    zero_df = df[df['Editable Boxes'] == 0].sort_values(by='SKU Id')
    real_boxes_count = int(active_df['Editable Boxes'].sum())
    dummy_boxes_count = math.ceil(len(zero_df) / 20) if not zero_df.empty else 0
    total_boxes = real_boxes_count + dummy_boxes_count
    current_box = 1
    for _, row in active_df.iterrows():
        boxes = int(row['Editable Boxes'])
        for _ in range(boxes):
            box_data.append({'num': current_box, 'total': total_boxes, 'sku': str(row['SKU Id']), 'qty': row['PPCN'], 'fsn': str(row.get('FSN', '')), 'type': 'real'})
            current_box += 1
    for _ in range(dummy_boxes_count):
        box_data.append({'num': current_box, 'total': total_boxes, 'sku': "MIX SKU", 'qty': 1, 'fsn': "MIX FSN", 'type': 'dummy'})
        current_box += 1
    
    writer = PdfWriter()
    w_a4, h_a4 = A4; half_h = h_a4 / 2; SHIFT_UP = 25 * mm
    total_items = len(box_data)
    
    for i, box in enumerate(box_data):
        if progress_bar: progress_bar.progress(int((i + 1) / total_items * 100), text=f"Processing Box {i+1}...")
        packet = io.BytesIO()
        c = canvas.Canvas(packet, pagesize=A4)
        def draw_grid_table(y_top):
            row_h = 10*mm; y_header = y_top; y_data = y_top - row_h
            x_start = 10*mm; x_c1 = 30*mm; x_c2 = 85*mm; x_c3 = 175*mm; x_end = w_a4 - 10*mm
            c.setLineWidth(1); c.line(x_start, y_header + row_h, x_end, y_header + row_h); c.line(x_start, y_header, x_end, y_header); c.line(x_start, y_data, x_end, y_data)
            c.line(x_start, y_data, x_start, y_header + row_h); c.line(x_c1, y_data, x_c1, y_header + row_h); c.line(x_c2, y_data, x_c2, y_header + row_h); c.line(x_c3, y_data, x_c3, y_header + row_h); c.line(x_end, y_data, x_end, y_header + row_h)
            c.setFont("Helvetica-Bold", 12); c.drawString(x_start + 2*mm, y_header + 3*mm, "SR NO."); c.drawString(x_c1 + 2*mm, y_header + 3*mm, "FSN"); c.drawString(x_c2 + 2*mm, y_header + 3*mm, "SKU ID"); c.drawString(x_c3 + 2*mm, y_header + 3*mm, "QTY")
            c.setFont("Helvetica", 12)
            c.drawString(x_start + 2*mm, y_data + 3*mm, "1."); c.drawString(x_c1 + 2*mm, y_data + 3*mm, box['fsn']); c.setFont("Helvetica", 12); c.drawString(x_c2 + 2*mm, y_data + 3*mm, box['sku'][:35]); c.setFont("Helvetica-Bold", 14); c.drawString(x_c3 + 2*mm, y_data + 3*mm, str(int(float(box['qty']))))
            return y_data
        def draw_slip(y_base):
            c.setFont("Helvetica-Bold", 30); c.drawCentredString(w_a4/2, y_base + 45*mm, "PACKING SLIP")
            data_bottom_y = draw_grid_table(y_base + 32*mm)
            c.setFont("Helvetica-Bold", 30); c.drawCentredString(w_a4/2, data_bottom_y - 15*mm, f"BOX NO.- {box['num']}            BOX NAME- {box['num']}")
        
        draw_slip(240*mm); c.setLineWidth(2); c.line(0, 210*mm, w_a4, 210*mm); draw_slip(155*mm); c.setLineWidth(1); c.line(0, half_h, w_a4, half_h)
        c.save(); packet.seek(0)
        custom_page = PdfReader(packet).pages[0]
        
        fk_page_idx = i // 2; is_top_label = (i % 2 == 0)
        temp_reader = PdfReader(io.BytesIO(flipkart_pdf_bytes))
        
        if fk_page_idx < len(temp_reader.pages):
            result_page = PageObject.create_blank_page(width=w_a4, height=h_a4)
            result_page.merge_page(custom_page)
            fk_page = temp_reader.pages[fk_page_idx]
            fk_h = fk_page.mediabox.height; fk_w = fk_page.mediabox.width
            shift_amount = -(0.70 * float(fk_h)) + float(SHIFT_UP) if is_top_label else -(0.2 * float(fk_h)) + float(SHIFT_UP)
            op = Transformation().translate(tx=0, ty=shift_amount)
            fk_page.add_transformation(op)
            if not is_top_label: fk_page.mediabox.lower_left = (0, 0); fk_page.mediabox.upper_right = (fk_w, (0.4 * float(fk_h)) + float(SHIFT_UP))
            result_page.merge_page(fk_page)
            writer.add_page(result_page)
        else:
            # Handle excess custom boxes
            result_page = PageObject.create_blank_page(width=w_a4, height=h_a4)
            result_page.merge_page(custom_page)
            writer.add_page(result_page)
            
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()

def generate_consignment_data_pdf(df, c_details):
    active_df = df[df['Editable Boxes'] > 0].copy()
    buffer = io.BytesIO(); doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=10*mm, leftMargin=10*mm, topMargin=10*mm, bottomMargin=10*mm); elements = []
    elements.append(Paragraph(f"<b>Consignment ID:</b> {c_details['id']}", getSampleStyleSheet()['Heading2']))
    elements.append(Paragraph(f"<b>Pickup Date:</b> {c_details['date']}", getSampleStyleSheet()['Normal']))
    elements.append(Spacer(1, 10))
    data = [['SKU', 'QTY', 'No. of Box']]; t_qty, t_box = 0, 0
    for _, row in active_df.sort_values(by='SKU Id').iterrows():
        qty = int(row['Editable Qty']); box = int(row['Editable Boxes']); t_qty += qty; t_box += box
        data.append([str(row['SKU Id']), str(qty), str(box)])
    data.append(['TOTAL', str(t_qty), str(t_box)])
    table = Table(data, colWidths=[110*mm, 30*mm, 30*mm])
    table.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.black), ('BACKGROUND', (0,0), (-1,0), colors.lightgrey), ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold')]))
    elements.append(table); doc.build(elements)
    return buffer.getvalue()

def generate_challan(df, c_details, sender, receiver):
    buffer = io.BytesIO(); c = canvas.Canvas(buffer, pagesize=A4); w, h = A4
    c.setFont("Helvetica-Bold", 14); c.drawString(10*mm, h-15*mm, f"Consignment ID: {c_details['id']}")
    c.setFont("Helvetica-Bold", 20); c.drawCentredString(w/2, h-25*mm, "DELIVERY CHALLAN")
    c.setLineWidth(1); c.rect(10*mm, h-85*mm, w-20*mm, 50*mm)
    def draw_addr(x, y, data, lbl):
        if not isinstance(data, dict): data = {}
        c.setFont("Helvetica-Bold", 10); c.drawString(x, y, lbl); c.drawString(x, y-5*mm, str(data.get('Code','')))
        c.setFont("Helvetica", 10); c.drawString(x, y-10*mm, str(data.get('Address1',''))); c.drawString(x, y-15*mm, f"{data.get('City','')}, {data.get('State','')}"); c.drawString(x, y-20*mm, f"GST: {data.get('GST','')}")
    draw_addr(15*mm, h-40*mm, sender, "FROM:"); draw_addr(110*mm, h-40*mm, receiver, "TO:")
    c.drawString(15*mm, h-95*mm, f"Date: {c_details['date']}")
    active_df = df[df['Editable Boxes'] > 0].copy()
    data = [['S.No', 'SKU', 'Product', 'Qty', 'Boxes']]; 
    for i, row in active_df.iterrows(): data.append([str(i+1), str(row['SKU Id']), str(row.get('Product Name',''))[:25], str(int(row['Editable Qty'])), str(int(row['Editable Boxes']))])
    table = Table(data, colWidths=[15*mm, 60*mm, 70*mm, 20*mm, 20*mm])
    table.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.black), ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'), ('ALIGN', (0,0), (-1,-1), 'CENTER')]))
    table.wrapOn(c, w, h); table.drawOn(c, 10*mm, h-110*mm - (len(data)*7*mm))
    c.save()
    return buffer.getvalue()

def generate_appointment_letter(c_details, sender, receiver):
    buffer = io.BytesIO(); c = canvas.Canvas(buffer, pagesize=A4); w, h = A4
    c.setFont("Helvetica-Bold", 20); c.drawCentredString(w/2, h-30*mm, "APPOINTMENT LETTER")
    c.setFont("Helvetica", 12)
    c.drawString(20*mm, h-60*mm, f"Date: {c_details['date']}")
    c.drawString(20*mm, h-70*mm, f"To: {receiver.get('Code')} ({receiver.get('City')})")
    c.drawString(20*mm, h-80*mm, f"From: {sender.get('Code')} ({sender.get('City')})")
    c.drawString(20*mm, h-100*mm, f"Subject: Delivery Appointment for Consignment {c_details['id']}")
    c.drawString(20*mm, h-120*mm, "Dear Team,"); c.drawString(20*mm, h-130*mm, "Please accept the delivery of the mentioned consignment.")
    c.drawString(20*mm, h-150*mm, "Vehicle No: _________________"); c.drawString(20*mm, h-160*mm, "Driver Name: ________________")
    c.save()
    return buffer.getvalue()

def generate_excel_simple(df, cols, filename):
    output = io.BytesIO(); valid_cols = [c for c in cols if c in df.columns]
    temp_df = df.copy()
    if 'Qty' in cols and 'Qty' not in temp_df.columns: temp_df['Qty'] = temp_df['Editable Qty']
    if 'Boxes' in cols and 'Boxes' not in temp_df.columns: temp_df['Boxes'] = temp_df['Editable Boxes']
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer: temp_df[valid_cols].to_excel(writer, index=False)
    return output.getvalue()

def generate_bartender_full(df):
    active_df = df[df['Editable Boxes'] > 0].copy()
    output = io.BytesIO(); master_df = load_master_data()
    temp_df = active_df[['SKU Id', 'Editable Qty']].copy()
    if 'FSN' in active_df.columns: temp_df['FSN_Temp'] = active_df['FSN']
    elif 'Product Name' in active_df.columns: temp_df['FSN_Temp'] = active_df['Product Name']
    else: temp_df['FSN_Temp'] = ''
    export_df = pd.merge(temp_df, master_df, left_on='SKU Id', right_on='SKU', how='left')
    if 'EAN' in export_df.columns: export_df['EAN'] = export_df['EAN'].astype(str).str.replace(r'\.0$', '', regex=True)
    export_df['EAN'] = export_df.apply(lambda x: x['FSN_Temp'] if pd.isna(x.get('EAN')) else x['EAN'], axis=1)
    export_df['QTY'] = export_df['Editable Qty']
    if 'SKU' in export_df.columns: export_df['SKU'] = export_df['SKU'].fillna(export_df['SKU Id'])
    else: export_df['SKU'] = export_df['SKU Id']
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        export_df.to_excel(writer, index=False)
    return output.getvalue()

# ----------------- Helpers for splitting by quantity -----------------
def split_df_by_quantity_limit(df, qty_col, limit):
    chunks = []
    current_rows = []
    current_sum = 0
    for _, row in df.iterrows():
        q = int(row[qty_col]) if pd.notna(row[qty_col]) else 0
        if q > limit:
            if current_rows:
                chunks.append(pd.DataFrame(current_rows))
                current_rows = []
                current_sum = 0
            chunks.append(pd.DataFrame([row]))
            continue
        if current_sum + q > limit:
            if current_rows:
                chunks.append(pd.DataFrame(current_rows))
            current_rows = [row]
            current_sum = q
        else:
            current_rows.append(row)
            current_sum += q
    if current_rows:
        chunks.append(pd.DataFrame(current_rows))
    final = []
    for c in chunks:
        if isinstance(c, pd.DataFrame): final.append(c.reset_index(drop=True))
        else: final.append(pd.DataFrame(c).reset_index(drop=True))
    return final

# ----------------- Booked calculation helpers -----------------
def clean_sku(val):
    if not isinstance(val, str): return str(val)
    val = val.replace('"', '').replace("'", "")
    if val.upper().startswith("SKU:"): val = val[4:]
    return val.strip()

def compute_booked_details_from_history():
    history = load_history()
    today = pd.Timestamp.now().date()
    details = {}
    dates_set = set()
    for h in history:
        if h.get('task_type') != 'execution': continue
        if h.get('is_booked') is False: continue
        try: d_obj = pd.to_datetime(h.get('date')).date()
        except: continue
        if d_obj >= today:
            dates_set.add(str(d_obj))
            df = h.get('data')
            if isinstance(df, pd.DataFrame) and not df.empty:
                for _, r in df.iterrows():
                    sku_col = None
                    for col in df.columns:
                        if re.search(r'^sku', col, re.IGNORECASE): sku_col = col; break
                    if not sku_col: continue
                    sku = clean_sku(r[sku_col])
                    
                    qty_col = None
                    for col in df.columns:
                        if re.search(r'editable qty|quantity|qty', col, re.IGNORECASE): qty_col = col; break
                    
                    box_col = None
                    for col in df.columns:
                        if re.search(r'editable boxes|boxes|box', col, re.IGNORECASE): box_col = col; break
                    
                    try: q = int(float(r.get(qty_col, 0))) if qty_col else 0
                    except: q = 0
                    try: b = int(float(r.get(box_col, 0))) if box_col else 0
                    except: b = 0
                    
                    if sku not in details: details[sku] = {'total_qty': 0, 'total_boxes': 0, 'dates': {}}
                    details[sku]['total_qty'] += q; details[sku]['total_boxes'] += b
                    ds = details[sku]['dates']; ds.setdefault(str(d_obj), {'qty': 0, 'boxes': 0})
                    ds[str(d_obj)]['qty'] += q; ds[str(d_obj)]['boxes'] += b
    return details, sorted(list(dates_set))

def compute_booked_map_from_details(details):
    m = {}
    for sku, v in details.items(): m[sku] = v.get('total_qty', 0)
    return m

def generate_booked_summary_pdf_bytes(booked_details, selected_dates=None):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    elements = [Paragraph("<b>Booked Summary</b>", styles['Heading2']), Spacer(1, 6)]
    table_data = [['SKU', 'Total Qty', 'Total Boxes', 'Pickup Dates (dd Mon:qty(box))']]
    for sku in sorted(booked_details.keys(), key=lambda s: s.upper()):
        d = booked_details[sku]
        if selected_dates:
            filtered = {dt: info for dt, info in d['dates'].items() if dt in selected_dates}
            total_qty = sum(info['qty'] for info in filtered.values())
            total_boxes = sum(info['boxes'] for info in filtered.values())
            if total_qty == 0: continue
            dates_str = ", ".join([f"{pd.to_datetime(dt).strftime('%d %b')}:{info['qty']}({info['boxes']})" for dt, info in filtered.items()])
        else:
            total_qty = d.get('total_qty', 0); total_boxes = d.get('total_boxes', 0)
            dates_str = ", ".join([f"{pd.to_datetime(dt).strftime('%d %b')}:{info['qty']}({info['boxes']})" for dt, info in d.get('dates', {}).items()])
        table_data.append([sku, str(total_qty), str(total_boxes), dates_str])
    if len(table_data) == 1: elements.append(Paragraph("No booked SKUs for the selected dates.", styles['Normal']))
    else:
        table = Table(table_data, colWidths=[60*mm, 30*mm, 30*mm, 60*mm])
        table.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.black), ('BACKGROUND', (0,0), (-1,0), colors.lightgrey), ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold')]))
        elements.append(table)
    doc.build(elements); buffer.seek(0)
    return buffer.getvalue()

# ----------------- Planning Logic -----------------
def calculate_single_warehouse_plan(sales_df, inv_df, settings, include_duplicates, mode_type):
    tpl_df = load_template_db(mode_type)
    booked_details, _ = compute_booked_details_from_history()
    booked_map = compute_booked_map_from_details(booked_details)

    sales_df.columns = [str(c).strip() for c in sales_df.columns]
    if 'SKU' in sales_df.columns: col_sku = 'SKU'
    elif len(sales_df.columns) > 5: col_sku = sales_df.columns[5]
    else: return pd.DataFrame(), "SKU Column not found", pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if 'Quantity' in sales_df.columns: col_qty = 'Quantity'
    elif len(sales_df.columns) > 13: col_qty = sales_df.columns[13]
    else: return pd.DataFrame(), "Quantity Column not found", pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    possible_state = [c for c in sales_df.columns if 'Delivery State' in str(c)]
    if possible_state: col_state = possible_state[0]
    elif len(sales_df.columns) > 50: col_state = sales_df.columns[50]
    else: return pd.DataFrame(), "State Column not found", pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    sales_df['Clean_SKU'] = sales_df[col_sku].apply(clean_sku)
    pattern = r"KBRV-\d+$" if not include_duplicates else r"^KBRV(?:[A-Z]*?)-\d+$"
    filtered_sales = sales_df[sales_df['Clean_SKU'].str.contains(pattern, case=False, na=False, regex=True)].copy()

    def map_state(s):
        if not isinstance(s, str): return (None, None)
        res = STATE_TO_ZONE.get(s)
        if not res: res = STATE_TO_ZONE.get(s.title())
        if not res: res = STATE_TO_ZONE.get(s.strip().title())
        return res if res else (None, None)

    if not filtered_sales.empty:
        filtered_sales[['Zone', 'WH_Col']] = filtered_sales[col_state].apply(lambda x: pd.Series(map_state(x)))
        filtered_sales[col_qty] = pd.to_numeric(filtered_sales[col_qty], errors='coerce').fillna(0)
        global_sales = filtered_sales.groupby('Clean_SKU')[col_qty].sum().to_dict()
        zone_sales = filtered_sales.groupby(['Clean_SKU', 'Zone'])[col_qty].sum().reset_index()
    else:
        global_sales = {}; zone_sales = pd.DataFrame(columns=['Clean_SKU','Zone',col_qty])

    inv_df.columns = [str(c).strip() for c in inv_df.columns]
    inv_grouped = {}
    if 'SKU' in inv_df.columns and 'Live on Website' in inv_df.columns:
        inv_df['Clean_SKU'] = inv_df['SKU'].apply(clean_sku)
        inv_df['Live on Website'] = pd.to_numeric(inv_df['Live on Website'], errors='coerce').fillna(0)
        inv_grouped = inv_df.groupby('Clean_SKU')['Live on Website'].sum().to_dict()
    else:
        qty_cols = [c for c in inv_df.columns if re.search(r'Live on Website|live on website|Live on website|qty|quantity|Live|Live Qty|LiveQty', c, re.IGNORECASE)]
        if 'SKU' in inv_df.columns and qty_cols:
            inv_df['Clean_SKU'] = inv_df['SKU'].apply(clean_sku)
            inv_df[qty_cols[0]] = pd.to_numeric(inv_df[qty_cols[0]], errors='coerce').fillna(0)
            inv_grouped = inv_df.groupby('Clean_SKU')[qty_cols[0]].sum().to_dict()
        else:
            if 'SKU' in inv_df.columns:
                inv_df['Clean_SKU'] = inv_df['SKU'].apply(clean_sku)
                numeric_cols = inv_df.select_dtypes(include='number').columns.tolist()
                if numeric_cols: inv_grouped = inv_df.groupby('Clean_SKU')[numeric_cols].sum().sum(axis=1).to_dict()
            elif inv_df.shape[1] >= 2:
                inv_df['Clean_SKU'] = inv_df.iloc[:,1].apply(clean_sku)
                numeric_cols = inv_df.select_dtypes(include='number').columns.tolist()
                if numeric_cols: inv_grouped = inv_df.groupby('Clean_SKU')[numeric_cols].sum().sum(axis=1).to_dict()

    final_rows = []; summary_rows = []; zone_summary_rows = []
    sales_skus = set(filtered_sales['Clean_SKU'].unique()) if not filtered_sales.empty else set()
    booked_skus = set(booked_map.keys())
    unique_skus = list(sales_skus.union(booked_skus))

    for sku in unique_skus:
        tot_sales = float(global_sales.get(sku, 0))
        tot_stock_orig = float(inv_grouped.get(sku, 0))
        booked_qty = int(booked_map.get(sku, 0))
        ppcn = 16 
        if not tpl_df.empty and 'SKU' in tpl_df.columns and 'PPCN' in tpl_df.columns:
            m_row = tpl_df[tpl_df['SKU'] == sku]
            if not m_row.empty: 
                try: ppcn = int(float(m_row.iloc[0]['PPCN']))
                except: pass
        master_df = load_master_data()
        if not master_df.empty and 'SKU' in master_df.columns and 'PPCN' in master_df.columns:
             m_row = master_df[master_df['SKU'] == sku]
             if not m_row.empty:
                 try: ppcn = int(float(m_row.iloc[0]['PPCN']))
                 except: pass

        try: req_net = float(tot_sales) - float(tot_stock_orig) - float(booked_qty)
        except: req_net = 0.0

        total_boxes_needed = int(math.floor(req_net / ppcn)) if ppcn > 0 else 0
        boxes_for_summary = total_boxes_needed
        final_qty_for_summary = total_boxes_needed * ppcn

        summary_rows.append({'SKU': sku, 'Sales_30': tot_sales, 'FBF_Qty': int(tot_stock_orig), 'Qty_Booked': int(booked_qty), 'Needed_Qty': req_net, 'Boxes': int(boxes_for_summary), 'Final_Qty': int(final_qty_for_summary), 'PPCN': int(ppcn)})

        sku_z_data = zone_sales[zone_sales['Clean_SKU'] == sku].copy()
        if not sku_z_data.empty: sku_z_data = sku_z_data.rename(columns={col_qty: 'ZoneSales'}) if col_qty in sku_z_data.columns else sku_z_data
        else: sku_z_data = pd.DataFrame(columns=['Clean_SKU','Zone','ZoneSales'])

        allocated_map = {}
        if sku_z_data.empty or total_boxes_needed <= 0: allocated_map = {}
        else:
            sku_z_data['Zone'] = sku_z_data['Zone'].astype(str).str.title()
            sku_z_data['ZoneSales'] = pd.to_numeric(sku_z_data['ZoneSales'], errors='coerce').fillna(0)
            zone_sales_nonzero = sku_z_data[sku_z_data['ZoneSales'] > 0].copy()
            zcount = len(zone_sales_nonzero)
            if zcount == 0:
                top_zone = sku_z_data.sort_values(by='ZoneSales', ascending=False).iloc[0]['Zone']
                allocated_map[top_zone] = int(total_boxes_needed)
            else:
                if total_boxes_needed >= zcount:
                    for z in zone_sales_nonzero['Zone'].tolist(): allocated_map[z] = 1
                    remaining = int(total_boxes_needed - zcount)
                    if remaining > 0:
                        zone_sales_nonzero['Share'] = zone_sales_nonzero['ZoneSales'] / tot_sales if tot_sales > 0 else 0
                        zone_sales_nonzero['Ideal'] = zone_sales_nonzero['Share'] * total_boxes_needed
                        zone_sales_nonzero['ToAdd'] = zone_sales_nonzero['Ideal'].apply(lambda x: int(math.floor(x - 1)) if (x - 1) > 0 else 0)
                        zone_sales_nonzero['Allocated'] = zone_sales_nonzero['ToAdd'] + 1
                        used = int(zone_sales_nonzero['Allocated'].sum())
                        remaining = int(total_boxes_needed - used)
                        if remaining > 0:
                            zone_sales_nonzero['Frac'] = (zone_sales_nonzero['Ideal'] - zone_sales_nonzero['Ideal'].apply(math.floor))
                            zone_sales_nonzero = zone_sales_nonzero.sort_values(by=['Frac','ZoneSales'], ascending=[False,False])
                            for idx, row in zone_sales_nonzero.iterrows():
                                if remaining <= 0: break
                                zone_sales_nonzero.at[idx, 'Allocated'] += 1
                                remaining -= 1
                        for _, r in zone_sales_nonzero.iterrows(): allocated_map[r['Zone']] = int(r['Allocated'])
                else:
                    zone_sales_nonzero = zone_sales_nonzero.sort_values(by='ZoneSales', ascending=False)
                    for z in zone_sales_nonzero['Zone'].tolist(): allocated_map[z] = 0
                    top_zones = zone_sales_nonzero.head(int(total_boxes_needed))
                    for _, r in top_zones.iterrows(): allocated_map[r['Zone']] = allocated_map.get(r['Zone'], 0) + 1

        for zone_name, boxes in allocated_map.items():
            if boxes > 0:
                final_rows.append({'SKU Id': sku, 'Zone': zone_name.title() if zone_name else "Unknown", 'Required Qty': req_net, 'Editable Boxes': int(boxes), 'Editable Qty': int(boxes * ppcn), 'PPCN': ppcn, 'Stock': int(tot_stock_orig), 'Qty_Booked': int(booked_qty)})

        for zone in ZONES_ORDER:
            ztitle = zone
            boxes_zone = allocated_map.get(ztitle, 0)
            boxes_zone = max(0, int(boxes_zone))
            zone_summary_rows.append({'SKU': sku, 'Zone': ztitle, 'Sales_30': tot_sales, 'FBF_Qty': int(tot_stock_orig), 'Qty_Booked': int(booked_qty), 'Needed_Qty': req_net, 'Boxes': int(boxes_zone), 'Final_Qty': int(boxes_zone * ppcn), 'PPCN': int(ppcn)})

    summary_df = pd.DataFrame(summary_rows)
    zone_summary_df = pd.DataFrame(zone_summary_rows)
    if not zone_summary_df.empty:
        zone_pivot = zone_summary_df.pivot_table(index='SKU', columns='Zone', values='Boxes', aggfunc='sum').fillna(0)
        for z in ZONES_ORDER:
            if z not in zone_pivot.columns: zone_pivot[z] = 0
        zone_pivot = zone_pivot[ZONES_ORDER].reset_index()
        combined = pd.merge(summary_df, zone_pivot, left_on='SKU', right_on='SKU', how='left').fillna(0)
    else:
        combined = summary_df.copy()
        for z in ZONES_ORDER: combined[z] = 0
        if 'Qty_Booked' not in combined.columns: combined['Qty_Booked'] = 0

    ordered_cols = ['SKU', 'Sales_30', 'FBF_Qty', 'Qty_Booked', 'Needed_Qty', 'Boxes', 'Final_Qty', 'PPCN'] + ZONES_ORDER
    combined = combined[[c for c in ordered_cols if c in combined.columns]]
    combined = combined.fillna(0)
    for c in ['Sales_30','FBF_Qty','Qty_Booked','Needed_Qty','Boxes','Final_Qty','PPCN'] + ZONES_ORDER:
        if c in combined.columns:
            if c in ZONES_ORDER or c in ['Boxes','Final_Qty','PPCN','Qty_Booked']: combined[c] = combined[c].astype(int)
            else: combined[c] = pd.to_numeric(combined[c], errors='coerce').fillna(0)

    if 'SKU' in combined.columns: combined = combined.sort_values(by='SKU', key=lambda s: s.str.upper()).reset_index(drop=True)
    if 'SKU' in summary_df.columns: summary_df = summary_df.sort_values(by='SKU', key=lambda s: s.str.upper()).reset_index(drop=True)

    if not final_rows: return pd.DataFrame(final_rows), "Calculated rows are empty.", summary_df, zone_summary_df, combined

    final_rows_df = pd.DataFrame(final_rows)
    if 'SKU Id' in final_rows_df.columns:
        final_rows_df['SKU_sort'] = final_rows_df['SKU Id'].astype(str).str.upper()
        zone_order_map = {z: i for i, z in enumerate(ZONES_ORDER)}
        final_rows_df['Zone_order'] = final_rows_df['Zone'].map(lambda x: zone_order_map.get(x.title(), 999))
        final_rows_df = final_rows_df.sort_values(by=['SKU_sort','Zone_order']).drop(columns=['SKU_sort','Zone_order']).reset_index(drop=True)

    return final_rows_df, "Success", summary_df, zone_summary_df, combined

# --- APP NAVIGATION ---
if 'page' not in st.session_state: st.session_state['page'] = 'home'
if 'consignments' not in st.session_state: st.session_state['consignments'] = load_history()
if not DriveHandler.file_exists(SENDERS_FILE):
    save_address_data(SENDERS_FILE, pd.DataFrame([{'Code': 'MAIN', 'Address1': 'Addr', 'City': 'City', 'Channel': 'All'}]))
if not DriveHandler.file_exists(RECEIVERS_FILE):
    save_address_data(RECEIVERS_FILE, pd.DataFrame(columns=addr_cols))

def nav(page):
    st.session_state['page'] = page
    st.rerun()

# --- SIDEBAR ---
with st.sidebar:
    st.title("🚀 Hike Manager")
    if st.button("🏠 Home", use_container_width=True): nav('home')
    if st.button("🕘 History", use_container_width=True): nav('history')

    st.divider()
    st.header("Plan Consignment")
    if st.button("🛒 Flipkart", use_container_width=True):
        st.session_state['plan_channel'] = 'Flipkart'; nav('plan_flipkart')
    if st.button("📦 Amazon", use_container_width=True):
        st.session_state['plan_channel'] = 'Amazon'; nav('plan_generic')
    if st.button("🛍️ Myntra", use_container_width=True):
        st.session_state['plan_channel'] = 'Myntra'; nav('plan_generic')

    st.divider()
    st.subheader("Execution (Labels)")
    if st.button("Flipkart Labels", use_container_width=True):
        st.session_state['current_channel']='Flipkart'; nav('channel')
    if st.button("Amazon Labels", use_container_width=True):
        st.session_state['current_channel']='Amazon'; nav('channel')
    if st.button("Myntra Labels", use_container_width=True):
        st.session_state['current_channel']='Myntra'; nav('channel')

    st.divider()
    st.header("Settings")
    if st.button("🔄 Sync Master Data"):
        s, m = sync_data()
        if s: st.success(m)
        else: st.error(m)
    
    # Check drive connection
    if DriveHandler.get_service() is None:
        st.error("⚠️ Drive Secrets Missing")

# ---------------- History Page ----------------
if st.session_state['page'] == 'history':
    st.title("Task History")
    tasks = st.session_state.get('consignments', [])
    tabs = st.tabs(["New Task", "Planning", "Execution", "Booked Summary"])
    with tabs[0]:
        st.header("Create New Task")
        if st.button("➕ New Plan (Flipkart)"):
            st.session_state['plan_channel'] = 'Flipkart'; nav('plan_flipkart')
    with tabs[1]:
        st.header("Planning Tasks")
        planning = [t for t in tasks if t.get('task_type') == 'planning']
        if not planning: st.info("No planning tasks yet.")
        else:
            for t in planning:
                st.subheader(f"Task: {t['id']} | Date: {t.get('date','-')} | Channel: {t.get('channel','-')}")
                if isinstance(t.get('data', None), pd.DataFrame):
                    df_preview = t['data'].copy()
                    if 'Qty_Booked' not in df_preview.columns: df_preview['Qty_Booked'] = 0
                    st.dataframe(df_preview.head(6), use_container_width=True)
                if st.button(f"Open {t['id']}", key=f"open_plan_{t['id']}"):
                    st.session_state['plan_task_id'] = t['id']
                    st.session_state['plan_results'] = t.get('data', pd.DataFrame()).copy()
                    st.session_state['plan_summary'] = t.get('original_data', pd.DataFrame()).copy() if isinstance(t.get('original_data', None), pd.DataFrame) else pd.DataFrame()
                    ed = st.session_state['plan_results'].copy() if isinstance(st.session_state['plan_results'], pd.DataFrame) else pd.DataFrame()
                    if 'Select' not in ed.columns: ed.insert(0, 'Select', True)
                    if 'Qty_Booked' not in ed.columns: ed['Qty_Booked'] = 0
                    for c in ['Editable Boxes','Editable Qty','PPCN','Stock','Required Qty','Qty_Booked']:
                        if c in ed.columns: ed[c] = pd.to_numeric(ed[c], errors='coerce').fillna(0).astype(int)
                    if 'SKU Id' in ed.columns: ed = ed.sort_values(by='SKU Id', key=lambda s: s.str.upper()).reset_index(drop=True)
                    st.session_state['plan_editor_df'] = ed.reset_index(drop=True)
                    st.session_state['plan_mode_key'] = t.get('mode_key', 'single')
                    st.session_state['plan_channel'] = t.get('channel', 'Flipkart')
                    nav('plan_flipkart')
    with tabs[2]:
        st.header("Execution (Shipments / Manual) Tasks")
        execs = [t for t in tasks if t.get('task_type') == 'execution']
        if not execs: st.info("No execution tasks yet.")
        else:
            for t in execs:
                st.subheader(f"Task: {t['id']} | Date: {t.get('date','-')} | Channel: {t.get('channel','-')}")
                if isinstance(t.get('data', None), pd.DataFrame): st.dataframe(t['data'].head(6), use_container_width=True)
                is_b = t.get('is_booked', True)
                st.write(f"Booked status: **{'BOOKED' if is_b else 'UNBOOKED'}**")
                if st.button(f"Toggle Booked for {t['id']}", key=f"toggle_booked_{t['id']}"):
                    for idx, hh in enumerate(st.session_state['consignments']):
                        if hh['id'] == t['id']:
                            st.session_state['consignments'][idx]['is_booked'] = not st.session_state['consignments'][idx].get('is_booked', True)
                            save_history(st.session_state['consignments'])
                            st.rerun()
                if st.button(f"Open {t['id']}", key=f"open_exec_{t['id']}"):
                    st.session_state['curr_con'] = t; nav('view_saved')
    with tabs[3]:
        st.header("Booked Summary")
        booked_details, available_dates = compute_booked_details_from_history()
        if not booked_details: st.info("No booked quantities found.")
        else:
            st.subheader("Available pickup dates")
            available_dates_sorted = sorted(available_dates)
            selected_dates = st.multiselect("Select pickup dates", options=available_dates_sorted, default=available_dates_sorted)
            rows = []
            for sku, d in booked_details.items():
                if selected_dates:
                    filtered = {dt:info for dt,info in d['dates'].items() if dt in selected_dates}
                    total_qty = sum(info['qty'] for info in filtered.values())
                    total_boxes = sum(info['boxes'] for info in filtered.values())
                    if total_qty == 0: continue
                    dates_str = ", ".join([f"{pd.to_datetime(dt).strftime('%d %b')}:{info['qty']}({info['boxes']})" for dt,info in filtered.items()])
                else:
                    total_qty = d.get('total_qty', 0)
                    total_boxes = d.get('total_boxes', 0)
                    dates_str = ", ".join([f"{pd.to_datetime(dt).strftime('%d %b')}:{info['qty']}({info['boxes']})" for dt,info in d.get('dates', {}).items()])
                rows.append({'SKU': sku, 'Qty': total_qty, 'Boxes': total_boxes, 'Dates': dates_str})
            if not rows: st.info("No booked SKUs for selected dates.")
            else:
                bm_df = pd.DataFrame(rows).sort_values(by='SKU', key=lambda s: s.str.upper()).reset_index(drop=True)
                st.dataframe(bm_df, use_container_width=True)
                pdf_bytes = generate_booked_summary_pdf_bytes(booked_details, selected_dates if selected_dates else None)
                st.download_button("⬇ Download Booked Summary PDF", pdf_bytes, file_name="Booked_Summary.pdf", mime="application/pdf")

# 1. HOME
if st.session_state['page'] == 'home':
    st.title("Warehouse Dashboard")
    if st.session_state['consignments']:
        df_h_rows = []
        for c in st.session_state['consignments']:
            try:
                boxes = c['data']['Editable Boxes'].sum() if isinstance(c.get('data', None), pd.DataFrame) and 'Editable Boxes' in c['data'].columns else 0
                qty = c['data']['Editable Qty'].sum() if isinstance(c.get('data', None), pd.DataFrame) and 'Editable Qty' in c['data'].columns else 0
                datev = pd.to_datetime(c['date'])
            except: boxes = 0; qty = 0; datev = pd.NaT
            df_h_rows.append({'Date': datev, 'Channel': c.get('channel','-'), 'Boxes': boxes, 'Qty': qty})
        df_h = pd.DataFrame(df_h_rows)
        m1, m2, m3 = st.columns(3)
        m1.metric("📦 Total Boxes Sent", int(df_h['Boxes'].sum()) if not df_h.empty else 0)
        m2.metric("👟 Total Pairs/Qty", int(df_h['Qty'].sum()) if not df_h.empty else 0)
        m3.metric("📅 Last Shipment", df_h['Date'].max().strftime('%d-%b-%Y') if not df_h.empty and pd.notna(df_h['Date'].max()) else "N/A")
        st.subheader("Volume by Channel")
        try: st.bar_chart(df_h.groupby('Channel')['Boxes'].sum(), color="#FF4B4B")
        except: pass
        st.subheader("Recent Activity")
        st.dataframe(df_h.sort_values(by='Date', ascending=False).head(5), use_container_width=True)
    else: st.info("No consignments found.")

# 2. PLAN FLIPKART
elif st.session_state['page'] == 'plan_flipkart':
    st.title("Plan Flipkart Consignment")
    with st.expander("⚙️ Settings & Templates", expanded=False):
        c1, c2 = st.columns(2)
        cost_val = c1.number_input("Standard Cost (INR) [Col P]", value=350)
        mult = c2.number_input("Sales Multiplier", value=1.0, step=0.1)
        st.divider()
        t1, t2 = st.columns(2)
        with t1:
            st.markdown("**Single Warehouse Template**")
            curr_s = load_template_db('single')
            if not curr_s.empty: st.caption(f"✅ Loaded: {len(curr_s)} Rows")
            else: st.caption("❌ Not Found")
            up_s = st.file_uploader("Upload Single WH Template", type=['csv'], key='tpl_s')
            if up_s and st.button("Update Single Template"):
                save_template_db(pd.read_csv(up_s, dtype=str), 'single'); st.success("Updated!"); st.rerun()
        with t2:
            st.markdown("**Multi Warehouse Template**")
            curr_m = load_template_db('multi')
            if not curr_m.empty: st.caption(f"✅ Loaded: {len(curr_m)} Rows")
            else: st.caption("❌ Not Found")
            up_m = st.file_uploader("Upload Multi WH Template", type=['csv'], key='tpl_m')
            if up_m and st.button("Update Multi Template"):
                save_template_db(pd.read_csv(up_m, dtype=str), 'multi'); st.success("Updated!"); st.rerun()
        st.divider()
        st.markdown("**Booked Summary (quick view)**")
        bd, dates_av = compute_booked_details_from_history()
        if not bd: st.caption("No booked SKUs.")
        else:
            bm_df = pd.DataFrame([{'SKU': k, 'Qty_Booked': v['total_qty']} for k, v in bd.items()]).sort_values(by='SKU', key=lambda s: s.str.upper()).reset_index(drop=True)
            st.dataframe(bm_df.head(200), use_container_width=True)

    st.subheader("1. Select Mode")
    col_mode, col_exc = st.columns(2)
    with col_mode:
        wh_mode = st.radio("Warehouse Mode", ["Single Warehouse File", "Multi Warehouse File"], horizontal=True)
        mode_key = 'single' if wh_mode == "Single Warehouse File" else 'multi'
    with col_exc:
        exc_mode = st.radio("Exception Mode", ["Exception", "No Exception"], horizontal=True, index=0)
    inc_dupe = st.checkbox("INCLUDE DUPLICATE LISTINGS", value=False)
    st.divider()

    if wh_mode == "Single Warehouse File":
        c1, c2 = st.columns(2)
        sales_file = c1.file_uploader("Last 30 Days Sales (Excel)", type=['xlsx'])
        inv_file = c2.file_uploader("Current FBF Inventory (CSV/Excel)", type=['csv', 'xlsx'])
        prog_cont = st.empty()
        if st.button("🚀 Generate Plan", type="primary"):
            if not sales_file or not inv_file: st.error("Please upload both files.")
            else:
                try:
                    prog_bar = prog_cont.progress(0, text="Reading Sales File...")
                    time.sleep(0.2); sales_file.seek(0)
                    try: sales_df = pd.read_excel(sales_file, sheet_name='Sales Report', engine='openpyxl')
                    except Exception: sales_file.seek(0); sales_df = pd.read_excel(sales_file, sheet_name='Sales Report')
                    prog_bar.progress(30, text="Reading Inventory File...")
                    time.sleep(0.2); inv_file.seek(0)
                    if inv_file.name.endswith('.csv'): inv_df = pd.read_csv(inv_file, dtype=str)
                    else: inv_df = pd.read_excel(inv_file, dtype=str)
                    prog_bar.progress(60, text="Calculating Logic...")
                    settings = {'multiplier': mult}
                    res_df, msg, summary_df, zone_summary_df, combined_zone_df = calculate_single_warehouse_plan(sales_df, inv_df, settings, inc_dupe, mode_key)
                    prog_bar.progress(100, text="Done!"); time.sleep(0.2); prog_cont.empty()
                    st.session_state['plan_results'] = res_df if isinstance(res_df, pd.DataFrame) else pd.DataFrame()
                    st.session_state['plan_summary'] = summary_df if isinstance(summary_df, pd.DataFrame) else pd.DataFrame()
                    st.session_state['plan_zone_summary'] = zone_summary_df if isinstance(zone_summary_df, pd.DataFrame) else pd.DataFrame()
                    st.session_state['plan_combined_zone_working'] = combined_zone_df if isinstance(combined_zone_df, pd.DataFrame) else pd.DataFrame()
                    st.session_state['plan_mode_key'] = mode_key
                    st.session_state['plan_task_id'] = f"TASK_{int(time.time())}"
                    if isinstance(res_df, pd.DataFrame) and res_df.empty: st.warning(f"Calculation completed: {msg}")
                    else: st.success(f"Plan Generated! {len(res_df)} box rows.")
                except Exception as e: st.error(f"Error: {e}")
    else: st.info("Multi Warehouse Logic Coming Soon...")

    if 'plan_results' in st.session_state:
        df_res = st.session_state['plan_results'].copy()
        summary_df = st.session_state.get('plan_summary', pd.DataFrame()).copy()
        zone_summary_df = st.session_state.get('plan_zone_summary', pd.DataFrame()).copy()
        combined_zone_df = st.session_state.get('plan_combined_zone_working', pd.DataFrame()).copy()
        task_id = st.session_state.get('plan_task_id', f"TASK_{int(time.time())}")
        st.divider()
        st.subheader(f"Results & Editor (Task: {task_id})")
        st.markdown("You can **select rows** to save as a consignment.")
        tabs = st.tabs(["None (All)"] + ZONES_ORDER)
        if 'plan_editor_df' not in st.session_state:
            if df_res.empty: st.session_state['plan_editor_df'] = pd.DataFrame(columns=['Select','SKU Id','Zone','Required Qty','Editable Boxes','Editable Qty','PPCN','Stock','Qty_Booked'])
            else:
                ed = df_res.copy(); ed.insert(0, 'Select', True)
                if 'Qty_Booked' not in ed.columns: ed['Qty_Booked'] = 0
                for c in ['Editable Boxes','Editable Qty','PPCN','Stock','Required Qty','Qty_Booked']:
                    if c in ed.columns: ed[c] = pd.to_numeric(ed[c], errors='coerce').fillna(0).astype(int)
                if 'SKU Id' in ed.columns: ed = ed.sort_values(by='SKU Id', key=lambda s: s.str.upper()).reset_index(drop=True)
                st.session_state['plan_editor_df'] = ed.reset_index(drop=True)

        def render_editor_for_df(local_df, key_suffix):
            if local_df.empty:
                st.info("No rows for this selection.")
                return local_df
            edited = st.data_editor(local_df, key=f"editor_{key_suffix}", use_container_width=True, hide_index=True, column_config={"Select": st.column_config.CheckboxColumn("Select", width="small"), "Required Qty": st.column_config.NumberColumn("Required Qty"), "Editable Boxes": st.column_config.NumberColumn("Editable Boxes"), "Editable Qty": st.column_config.NumberColumn("Editable Qty")}, disabled=[])
            return edited

        with tabs[0]:
            pe = st.session_state['plan_editor_df'].copy()
            for col in ['PPCN', 'Stock', 'Qty_Booked', 'Required Qty', 'Editable Boxes', 'Editable Qty']:
                if col not in pe.columns: pe[col] = 0
            all_view = pe.groupby(['SKU Id','PPCN','Stock','Qty_Booked'])[['Required Qty','Editable Boxes','Editable Qty']].sum().reset_index()
            if 'Select' not in all_view.columns: all_view.insert(0, 'Select', True)
            if 'SKU Id' in all_view.columns: all_view = all_view.sort_values(by='SKU Id', key=lambda s: s.str.upper()).reset_index(drop=True)
            edited_all = render_editor_for_df(all_view, "all")
            if st.button("Apply All Edits Now"):
                me = st.session_state['plan_editor_df']
                if not edited_all.empty:
                    for _, r in edited_all.iterrows():
                        sku = r['SKU Id']; ppcn = r.get('PPCN', None)
                        mask = (me['SKU Id'] == sku) & (me['PPCN'] == ppcn)
                        me.loc[mask, 'Select'] = r['Select']
                        total_existing = me.loc[mask, 'Editable Qty'].sum()
                        if total_existing > 0:
                            factor = r['Editable Qty'] / total_existing if total_existing>0 else 0
                            me.loc[mask, 'Editable Qty'] = (me.loc[mask, 'Editable Qty'] * factor).round().astype(int)
                        else:
                            idxs = me[mask].index.tolist()
                            if idxs: me.at[idxs[0], 'Editable Qty'] = int(r['Editable Qty'])
                        me.loc[mask, 'Editable Boxes'] = int(r['Editable Boxes'])
                    if 'SKU Id' in me.columns: me = me.sort_values(by='SKU Id', key=lambda s: s.str.upper()).reset_index(drop=True)
                    st.session_state['plan_editor_df'] = me
                    st.success("Applied edits to master editor.")

        for z in ZONES_ORDER:
            with tabs[ZONES_ORDER.index(z) + 1]:
                z_df = st.session_state['plan_editor_df'][st.session_state['plan_editor_df']['Zone'] == z].copy()
                if z_df.empty: st.info(f"No data for {z}")
                else:
                    if 'SKU Id' in z_df.columns: z_df = z_df.sort_values(by='SKU Id', key=lambda s: s.str.upper()).reset_index(drop=True)
                    edited_zone = render_editor_for_df(z_df, f"zone_{z}")
                    if st.button(f"Apply Edits for {z} to Master", key=f"apply_zone_{z}"):
                        me = st.session_state['plan_editor_df']
                        for _, r in edited_zone.iterrows():
                            mask = (me['SKU Id'] == r['SKU Id']) & (me['Zone'] == r['Zone']) & (me['PPCN'] == r['PPCN'])
                            me.loc[mask, 'Select'] = r['Select']
                            me.loc[mask, 'Editable Boxes'] = int(r['Editable Boxes'])
                            me.loc[mask, 'Editable Qty'] = int(r['Editable Qty'])
                        if 'SKU Id' in me.columns: me = me.sort_values(by='SKU Id', key=lambda s: s.str.upper()).reset_index(drop=True)
                        st.session_state['plan_editor_df'] = me
                        st.success(f"Applied edits for {z}.")

        st.divider()
        col_b1, col_b2, col_b3 = st.columns([1,1,1])
        with col_b1:
            ed_master = st.session_state['plan_editor_df'].copy()
            if not ed_master.empty:
                csvb = io.BytesIO(); ed_master.to_csv(csvb, index=False)
                st.download_button("⬇ Download Editable CSV", csvb.getvalue(), f"Editable_{task_id}.csv", mime="text/csv")
            else: st.button("No editable data to download", disabled=True)
        with col_b2:
            up_ed = st.file_uploader("Upload Edited CSV", type=['csv'], key='upload_edited_csv')
            if up_ed is not None:
                try:
                    edf = pd.read_csv(up_ed, dtype=str)
                    me = st.session_state['plan_editor_df']
                    if 'SKU Id' not in edf.columns: st.error("Uploaded CSV must contain 'SKU Id' column.")
                    else:
                        for _, r in edf.iterrows():
                            sku = r['SKU Id']; mask = me['SKU Id'] == sku
                            if 'Select' in edf.columns:
                                val = str(r['Select']).strip().lower()
                                sel = True if val in ['true','1','yes','y','t'] else False
                                me.loc[mask, 'Select'] = sel
                            if 'Editable Qty' in edf.columns:
                                try:
                                    q = int(float(r['Editable Qty'])); idxs = me[mask].index.tolist()
                                    if len(idxs) == 1: me.loc[idxs, 'Editable Qty'] = q
                                    elif len(idxs) > 1:
                                        per = q // len(idxs); me.loc[idxs, 'Editable Qty'] = per
                                        remainder = q - per*len(idxs)
                                        for j in range(remainder): me.at[idxs[j], 'Editable Qty'] += 1
                                except: pass
                            if 'Editable Boxes' in edf.columns:
                                try:
                                    b = int(float(r['Editable Boxes'])); me.loc[mask, 'Editable Boxes'] = b
                                except: pass
                        if 'SKU Id' in me.columns: me = me.sort_values(by='SKU Id', key=lambda s: s.str.upper()).reset_index(drop=True)
                        st.session_state['plan_editor_df'] = me; st.success("Uploaded and applied edited CSV.")
                except Exception as e: st.error(f"Failed to read uploaded CSV: {e}")
        with col_b3:
            if st.button("Reset Editor to Calculated Results"):
                if not df_res.empty:
                    ed = df_res.copy(); ed.insert(0,'Select', True)
                    if 'Qty_Booked' not in ed.columns: ed['Qty_Booked'] = 0
                    for c in ['Editable Boxes','Editable Qty','PPCN','Stock','Required Qty','Qty_Booked']:
                        if c in ed.columns: ed[c] = pd.to_numeric(ed[c], errors='coerce').fillna(0).astype(int)
                    if 'SKU Id' in ed.columns: ed = ed.sort_values(by='SKU Id', key=lambda s: s.str.upper()).reset_index(drop=True)
                    st.session_state['plan_editor_df'] = ed.reset_index(drop=True); st.success("Editor reset.")
                else: st.info("No calculation results to reset from.")

        st.divider()
        if st.button("💾 SAVE SELECTED ROWS AS TASK (Selected rows only)", type="primary"):
            ed = st.session_state['plan_editor_df']
            if 'Select' not in ed.columns: st.error("No selection column found. Nothing to save.")
            else:
                save_df = ed[ed['Select'] == True].copy()
                if save_df.empty: st.error("No rows selected to save.")
                else:
                    if 'Qty_Booked' not in save_df.columns: save_df['Qty_Booked'] = 0
                    pack = {'id': task_id, 'date': str(pd.Timestamp.now().date()), 'channel': st.session_state.get('plan_channel','Flipkart'), 'data': save_df.reset_index(drop=True), 'original_data': summary_df if not summary_df.empty else pd.DataFrame(), 'backup_data': pd.DataFrame(), 'sender': {}, 'receiver': {}, 'saved': True, 'printed_boxes': [], 'task_type': 'planning', 'mode_key': st.session_state.get('plan_mode_key','single'), 'is_booked': False}
                    st.session_state['consignments'].append(pack)
                    save_history(st.session_state['consignments'])
                    st.success(f"Task saved: {task_id}")

        st.divider()
        tpl_db = load_template_db(mode_key)
        if not combined_zone_df.empty:
            def zone_working_xlsx_bytes(combined_df, zone_summary_df):
                out = io.BytesIO()
                export_combined = combined_df.copy()
                if 'Boxes' in export_combined.columns: export_combined['Boxes'] = export_combined['Boxes'].apply(lambda x: max(0, int(x)))
                if 'Final_Qty' in export_combined.columns: export_combined['Final_Qty'] = export_combined['Final_Qty'].apply(lambda x: max(0, int(x)))
                with pd.ExcelWriter(out, engine='xlsxwriter') as writer:
                    export_combined = export_combined.sort_values(by='SKU', key=lambda s: s.str.upper()).reset_index(drop=True)
                    export_combined.to_excel(writer, index=False, sheet_name='Complete_Working')
                    for zone in ZONES_ORDER:
                        zone_col = zone
                        if zone_col not in export_combined.columns: continue
                        zone_only = export_combined[export_combined[zone_col] > 0].copy()
                        if not zone_only.empty:
                            zone_only = zone_only[['SKU','Sales_30','FBF_Qty','Qty_Booked','Needed_Qty',zone_col,'PPCN']].copy()
                            zone_only = zone_only.rename(columns={zone_col:'Boxes'})
                            zone_only['Boxes'] = zone_only['Boxes'].apply(lambda x: max(0,int(x)))
                            zone_only['Final_Qty'] = zone_only['Boxes'] * zone_only['PPCN']
                            zone_only = zone_only[['SKU','Sales_30','FBF_Qty','Qty_Booked','Needed_Qty','Boxes','Final_Qty','PPCN']]
                            zone_only = zone_only.sort_values(by='SKU', key=lambda s: s.str.upper()).reset_index(drop=True)
                            zone_only.to_excel(writer, index=False, sheet_name=zone)
                return out.getvalue()
            st.download_button("⬇ Download Complete Working (Zone-wise) XLSX", zone_working_xlsx_bytes(combined_zone_df, zone_summary_df), f"Complete_Working_ZoneWise_{task_id}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else: st.info("Complete Working (zone-wise) not available - run plan first.")

        st.markdown("**Active Listings (All Zones)**")
        if tpl_db.empty: st.error("Template empty. Upload a template in settings to generate Active Listings.")
        elif tpl_db.shape[1] < 16: st.error("Template must have at least 16 columns (so Column O and P exist).")
        else:
            sku_qty_map = {}
            if not summary_df.empty:
                for _, r in summary_df.iterrows(): sku_qty_map[r['SKU']] = max(0, int(r['Final_Qty']))
            tpl_filtered = tpl_db[tpl_db['SKU'].isin(sku_qty_map.keys())].copy().reset_index(drop=True)
            if tpl_filtered.empty: st.info("No template rows match SKUs requiring boxes.")
            else:
                tpl_filtered = tpl_filtered.sort_values(by='SKU', key=lambda s: s.str.upper()).reset_index(drop=True)
                tpl_filtered.iloc[:, 14] = tpl_filtered['SKU'].map(sku_qty_map).fillna(0).astype(int)
                tpl_filtered.iloc[:, 15] = cost_val
                tpl_filtered = tpl_filtered[tpl_filtered.iloc[:, 14] > 0].copy().reset_index(drop=True)
                if tpl_filtered.empty: st.info("No template rows with quantity > 0.")
                else:
                    tmp = tpl_filtered.copy()
                    tmp['__Q__'] = tmp.iloc[:,14].astype(int)
                    chunks = split_df_by_quantity_limit(tmp, '__Q__', 4999)
                    for idx, chunk in enumerate(chunks, start=1):
                        chunk = chunk.drop(columns=['__Q__'], errors='ignore')
                        csvb = io.BytesIO(); chunk.to_csv(csvb, index=False)
                        fname = f"Download_All_Zone_{idx}_{task_id}.csv"
                        st.download_button(label=f"⬇ Download All Zone {idx} ({task_id})", data=csvb.getvalue(), file_name=fname, mime="text/csv")

        st.divider()
        st.markdown("**Active Listings (Per Zone)**")
        if tpl_db.empty or tpl_db.shape[1] < 16: st.info("Upload or fix template to generate zone-wise active listings.")
        else:
            master = st.session_state['plan_editor_df'].copy()
            if not master.empty:
                for zone in ZONES_ORDER:
                    zone_sum = master[master['Zone'] == zone].groupby('SKU Id')['Editable Qty'].sum().reset_index().rename(columns={'SKU Id':'SKU','Editable Qty':'CALC_QTY'})
                    if zone_sum.empty: continue
                    zone_sum['CALC_QTY'] = zone_sum['CALC_QTY'].apply(lambda x: max(0,int(x)))
                    merged_zone = tpl_db[tpl_db['SKU'].isin(zone_sum['SKU'])].copy().reset_index(drop=True)
                    if merged_zone.empty: continue
                    merged_zone = merged_zone.sort_values(by='SKU', key=lambda s: s.str.upper()).reset_index(drop=True)
                    merged_zone.iloc[:,14] = merged_zone['SKU'].map(dict(zip(zone_sum['SKU'], zone_sum['CALC_QTY']))).fillna(0).astype(int)
                    merged_zone.iloc[:,15] = cost_val
                    merged_zone = merged_zone[merged_zone.iloc[:,14] > 0].copy().reset_index(drop=True)
                    if merged_zone.empty: continue
                    tmp = merged_zone.copy()
                    tmp['__Q__'] = tmp.iloc[:,14].astype(int)
                    chunks = split_df_by_quantity_limit(tmp, '__Q__', 4999)
                    for idx, chunk in enumerate(chunks, start=1):
                        chunk = chunk.drop(columns=['__Q__'], errors='ignore')
                        csvb = io.BytesIO(); chunk.to_csv(csvb, index=False)
                        fname = f"Download_{zone}_{idx}_{task_id}.csv"
                        st.download_button(label=f"⬇ Download {zone} {idx} ({task_id})", data=csvb.getvalue(), file_name=fname, mime="text/csv")
            else: st.info("No allocation rows to build per-zone listings.")

        st.divider()
        with st.expander("🚫 Danger Zone — Delete Planning Task", expanded=False):
            st.markdown("**Warning:** This will permanently delete the saved planning task from history. This action cannot be undone.")
            confirm_delete = st.checkbox(f"I confirm I want to delete task {task_id}", key=f"confirm_del_{task_id}")
            if st.button(f"🗑️ Delete Task {task_id}", key=f"del_plan_{task_id}"):
                if not confirm_delete: st.error("Please check the confirmation checkbox first.")
                else:
                    before = len(st.session_state['consignments'])
                    st.session_state['consignments'] = [c for c in st.session_state['consignments'] if c.get('id') != task_id]
                    after = len(st.session_state['consignments'])
                    save_history(st.session_state['consignments'])
                    for k in ['plan_results','plan_summary','plan_zone_summary','plan_combined_zone_working','plan_editor_df','plan_task_id','plan_mode_key']:
                        if k in st.session_state: del st.session_state[k]
                    st.success(f"Deleted task {task_id}. Redirecting to History...")
                    time.sleep(0.8); nav('history')

# 3. CHANNEL VIEW
elif st.session_state['page'] == 'channel':
    st.title(f"{st.session_state.get('current_channel','Channel')} Shipments")
    ch = st.session_state.get('current_channel', 'Flipkart')
    cons = [c for c in st.session_state['consignments'] if c['channel'] == ch]
    if not cons: st.info("No shipments found for this channel.")
    for c in reversed(cons[-10:]):
        boxes_sum = int(c['data']['Editable Boxes'].sum()) if isinstance(c.get('data', None), pd.DataFrame) and 'Editable Boxes' in c['data'].columns else 0
        col1, col2 = st.columns([0.8, 0.2])
        with col1:
            if st.button(f"📄 {c['id']} | Date: {c['date']} | Boxes: {boxes_sum}", key=c['id'], use_container_width=True):
                st.session_state['curr_con'] = c; nav('view_saved')
    st.divider()
    if st.button("➕ Create Manual Shipment"): nav('add')

# 4. ADD MANUAL
elif st.session_state['page'] == 'add':
    st.title("New Consignment (Manual)")
    c_id = st.text_input("Consignment ID")
    p_date = st.date_input("Pickup Date")
    df_s = load_address_data(SENDERS_FILE, addr_cols); df_r = load_address_data(RECEIVERS_FILE, addr_cols)
    c1, c2 = st.columns(2)
    with c1:
        s_sel = st.selectbox("Sender", df_s['Code'].tolist() + ["+ Add New"])
        if s_sel == "+ Add New":
            with st.form("ns"):
                ns = {k: st.text_input(k) for k in addr_cols if k!='Channel'}; ns['Channel']='All'
                if st.form_submit_button("Save"):
                    save_address_data(SENDERS_FILE, pd.concat([df_s, pd.DataFrame([ns])], ignore_index=True)); st.rerun()
    with c2:
        r_list = df_r[df_r['Channel']==st.session_state.get('current_channel')]['Code'].tolist()
        r_sel = st.selectbox("Receiver", r_list + ["+ Add New"])
        if r_sel == "+ Add New":
            with st.form("nr"):
                nr = {k: st.text_input(k) for k in addr_cols if k!='Channel'}; nr['Channel']=st.session_state.get('current_channel')
                if st.form_submit_button("Save"):
                    save_address_data(RECEIVERS_FILE, pd.concat([df_r, pd.DataFrame([nr])], ignore_index=True)); st.rerun()
    uploaded = st.file_uploader("Upload CSV", type='csv')
    if uploaded and c_id and s_sel != "+ Add New":
        if st.button("Process"):
            existing_ids = [c['id'] for c in st.session_state['consignments']]
            if c_id in existing_ids: st.error(f"⚠️ Consignment ID '{c_id}' already created!"); st.stop()
            df_m = load_master_data()
            if df_m.empty: st.warning("Master Data not found. Some functionality like EANs may be missing. Sync in sidebar.")
            df_raw = pd.read_csv(uploaded); uploaded.seek(0); df_c = pd.read_csv(uploaded)
            if not df_m.empty: merged = pd.merge(df_c, df_m, left_on='SKU Id', right_on='SKU', how='left')
            else: merged = df_c
            merged['Editable Qty'] = merged['Quantity Sent'].fillna(0)
            if 'PPCN' in merged.columns: merged['PPCN'] = pd.to_numeric(merged['PPCN'], errors='coerce').fillna(16)
            else: merged['PPCN'] = 16
            merged['Editable Boxes'] = (merged['Editable Qty'] / merged['PPCN']).apply(lambda x: float(x)).round(2)
            st.session_state['curr_con'] = {'id': c_id, 'date': str(p_date), 'channel': st.session_state.get('current_channel'), 'data': merged, 'original_data': df_raw, 'backup_data': pd.DataFrame(), 'sender': df_s[df_s['Code']==s_sel].iloc[0].to_dict(), 'receiver': df_r[df_r['Code']==r_sel].iloc[0].to_dict(), 'saved': False, 'printed_boxes': [], 'task_type': 'execution', 'is_booked': True}
            nav('preview')

# 5. PREVIEW
elif st.session_state['page'] == 'preview':
    pkg = st.session_state['curr_con']; st.title(f"Review: {pkg['id']}")
    disp = pkg['data'][['SKU Id', 'Editable Qty', 'Editable Boxes']].copy()
    try: disp['Editable Boxes'] = disp['Editable Boxes'].astype(int)
    except: pass
    st.dataframe(disp, hide_index=True, use_container_width=True)
    if st.button("💾 SAVE CONSIGNMENT", type="primary"):
        pkg['saved'] = True
        pkg['task_type'] = pkg.get('task_type', 'execution')
        if 'is_booked' not in pkg: pkg['is_booked'] = True
        st.session_state['consignments'].append(pkg)
        save_history(st.session_state['consignments']); nav('view_saved')

# 6. VIEW SAVED
elif st.session_state['page'] == 'view_saved':
    pkg = st.session_state['curr_con']; c_id = pkg['id']
    if st.button("🔙 Back to Channel", use_container_width=True): nav('channel')
    st.title(f"Consignment: {c_id}")
    if pkg.get('edit_timestamp'): st.info(f"ℹ️ This consignment has been edited on {pkg['edit_timestamp']}")

    st.subheader("1. Download Files (Generated)")
    r1c1, r1c2, r1c3 = st.columns(3)
    with r1c1:
        orig_csv = io.BytesIO()
        if isinstance(pkg.get('original_data', None), pd.DataFrame) and not pkg['original_data'].empty: pkg['original_data'].to_csv(orig_csv, index=False)
        st.download_button("⬇ Consignment CSV (Raw)", orig_csv.getvalue(), f"{c_id}.csv", "text/csv")
    with r1c2: st.download_button("⬇ Consignment Data PDF", generate_consignment_data_pdf(pkg['data'], pkg), f"Data_{c_id}.pdf")
    with r1c3: st.download_button("⬇ Confirm Consignment Upload (CSV)", generate_confirm_consignment_csv(pkg['data']), f"Confirm_{c_id}.csv", "text/csv")

    r2c1, r2c2, r2c3 = st.columns(3)
    with r2c1: st.download_button("⬇ Product Labels (Bartender)", generate_bartender_full(pkg['data']), f"Bartender_All_{c_id}.xlsx")
    with r2c2: st.download_button("⬇ Ewaybill Data (Excel)", generate_excel_simple(pkg['data'], ['SKU Id', 'Editable Qty', 'Cost Price'], f"Eway_{c_id}.xlsx"), f"Eway_{c_id}.xlsx")

    st.divider()
    st.subheader("2. File Repository & Merged Labels")
    uc1, uc2 = st.columns([1, 1])
    with uc1:
        f_lbl = st.file_uploader("Upload Flipkart Box Labels PDF", type=['pdf'], key='u_lbl')
        if f_lbl:
            if st.button("Process & Merge Labels"):
                save_uploaded_file(f_lbl, c_id, 'box_labels')
                progress_bar = st.progress(0, text="Starting Merge...")
                path_lbl = get_stored_file_bytes(c_id, 'box_labels') # Drive bytes
                if path_lbl:
                    snd = pkg.get('sender', {}); rcv = pkg.get('receiver', {})
                    merged_bytes = generate_merged_box_labels(pkg['data'], pkg, snd, rcv, path_lbl, progress_bar)
                    if merged_bytes:
                        DriveHandler.upload_file(f"{c_id}_merged_labels.pdf", merged_bytes, 'application/pdf')
                        progress_bar.progress(100, text="Completed!"); time.sleep(1)
                        st.success("Uploaded & Merged!"); st.rerun()
                else: st.error("Failed to retrieve uploaded file")

    with uc2:
        path_merged = get_merged_labels_bytes(c_id)
        if path_merged:
            st.download_button("⬇ Download MERGED Box Labels", path_merged, f"Merged_Labels_{c_id}.pdf", "application/pdf")
            st.divider()
            if st.button("🖨️ SCAN & PRINT BOX LABELS", type="primary", use_container_width=True): nav('scan_print')
        elif get_stored_file_exists(c_id, 'box_labels'):
            st.warning("Labels uploaded but not merged yet. Click 'Process & Merge'.")
            st.button("🖨️ SCAN & PRINT BOX LABELS", disabled=True, key='dis_scan')
        else:
            st.button("⬇ Download Box Labels", disabled=True, key='d_lbl_dis', help="Upload PDF first")
            st.button("🖨️ SCAN & PRINT BOX LABELS", disabled=True, key='dis_scan_2')

    st.divider()
    st.subheader("3. Appointment & Challan")
    c_apt, c_chal = st.columns(2)
    with c_apt:
        f_apt = st.file_uploader("Upload Appt PDF", type=['pdf'], key='u_apt')
        if f_apt:
            if st.button("Save Appt"): save_uploaded_file(f_apt, c_id, 'appointment'); st.rerun()
        path_apt = get_stored_file_bytes(c_id, 'appointment')
        if path_apt: st.download_button("⬇ Download Appt", path_apt, f"Appt_{c_id}.pdf")
        else: st.download_button("⬇ Generate Appointment Letter", generate_appointment_letter(pkg, pkg.get('sender',{}), pkg.get('receiver',{})), f"Appt_Gen_{c_id}.pdf")

    with c_chal:
        f_ch = st.file_uploader("Upload Challan PDF", type=['pdf'], key='u_ch')
        if f_ch:
            if st.button("Save Challan"): save_uploaded_file(f_ch, c_id, 'challan'); st.rerun()
        path_ch = get_stored_file_bytes(c_id, 'challan')
        if path_ch: st.download_button("⬇ Download Challan", path_ch, f"Challan_{c_id}.pdf")
        else: st.download_button("⬇ Generate Challan", generate_challan(pkg['data'], pkg, pkg.get('sender',{}), pkg.get('receiver',{})), f"Challan_Gen_{c_id}.pdf")

    st.divider()
    st.subheader("4. Edit Qty in Consignment (Available Boxes)")
    st.info("Download the Excel, modify 'Available Box (Edit)', and upload to update the consignment. Set boxes to 0 if inventory is missing.")
    c_edit_1, c_edit_2, c_edit_3 = st.columns(3)
    with c_edit_1:
        edit_buffer = io.BytesIO()
        edit_export = pkg['data'][['SKU Id', 'Editable Qty', 'Editable Boxes']].copy()
        edit_export = edit_export.rename(columns={'Editable Qty': 'Original Qty', 'Editable Boxes': 'Box Qty'})
        edit_export['Available Box (Edit)'] = edit_export['Box Qty']
        edit_export = edit_export.sort_values(by='SKU Id', key=lambda col: col.str.upper())
        with pd.ExcelWriter(edit_buffer, engine='xlsxwriter') as writer:
            edit_export.to_excel(writer, index=False, sheet_name='Sheet1')
            workbook = writer.book; worksheet = writer.sheets['Sheet1']
            worksheet.set_column(0, 0, 40); worksheet.set_column(1, 3, 20)
        st.download_button("⬇ Download Edit Excel", edit_buffer.getvalue(), f"Edit_Inventory_{c_id}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with c_edit_2:
        up_edit = st.file_uploader("Upload Edited Excel", type=['xlsx'], key='up_edit_inv')
        if up_edit:
            if st.button("✅ Confirm & Update Consignment"):
                try:
                    prog_bar = st.progress(0, text="Reading File..."); time.sleep(0.3)
                    new_df = pd.read_excel(up_edit)
                    required_cols = ['SKU Id', 'Available Box (Edit)']
                    if not all(col in new_df.columns for col in required_cols):
                        st.error("Uploaded file missing required columns: 'SKU Id', 'Available Box (Edit)'"); prog_bar.empty()
                    else:
                        prog_bar.progress(40, text="Applying Logic...")
                        if 'backup_data' not in pkg or not isinstance(pkg['backup_data'], pd.DataFrame) or pkg['backup_data'].empty:
                             pkg['backup_data'] = pkg['data'].copy()
                        curr_data = pkg['data'].copy()
                        box_map = dict(zip(new_df['SKU Id'].astype(str), new_df['Available Box (Edit)']))
                        for idx, row in curr_data.iterrows():
                            sku = str(row['SKU Id'])
                            if sku in box_map:
                                new_boxes = int(float(box_map[sku]))
                                ppcn = int(float(row['PPCN'])) if row['PPCN'] > 0 else 1
                                curr_data.at[idx, 'Editable Boxes'] = new_boxes
                                curr_data.at[idx, 'Editable Qty'] = new_boxes * ppcn
                        pkg['data'] = curr_data
                        pkg['edit_timestamp'] = pd.Timestamp.now().strftime('%d-%b-%Y %I:%M %p')
                        prog_bar.progress(80, text="Saving..."); time.sleep(0.2)
                        for i, h in enumerate(st.session_state['consignments']):
                            if h['id'] == c_id: st.session_state['consignments'][i] = pkg
                        save_history(st.session_state['consignments'])
                        prog_bar.progress(100, text="Done!"); time.sleep(0.5)
                        st.success("Consignment Updated! Generator files (Section 1) are now updated."); st.rerun()
                except Exception as e: st.error(f"Error reading file: {e}")
    with c_edit_3:
        if st.button("🗑️ Delete Uploaded Data (Reset)"):
            if 'backup_data' in pkg and isinstance(pkg['backup_data'], pd.DataFrame) and not pkg['backup_data'].empty:
                pkg['data'] = pkg['backup_data'].copy(); pkg['backup_data'] = pd.DataFrame()
                pkg.pop('edit_timestamp', None)
                for i, h in enumerate(st.session_state['consignments']):
                    if h['id'] == c_id: st.session_state['consignments'][i] = pkg
                save_history(st.session_state['consignments'])
                st.success("Consignment reset to original state."); st.rerun()
            else: st.warning("No backup data found. Cannot reset (or data is already original).")

    st.divider()
    with st.expander("🚫 Danger Zone"):
        if st.button(f"🗑️ Delete Consignment {c_id}", type="primary"):
            st.session_state['consignments'] = [c for c in st.session_state['consignments'] if c['id'] != c_id]
            save_history(st.session_state['consignments']); nav('home')

# 7. SCAN & PRINT PAGE
elif st.session_state['page'] == 'scan_print':
    pkg = st.session_state['curr_con']; c_id = pkg['id']
    merged_pdf_bytes = get_merged_labels_bytes(c_id)

    # 1. Expand Box Data for the Table
    if 'scan_box_data' not in st.session_state or st.session_state.get('scan_c_id') != c_id:
        box_data = []
        active_df = pkg['data'][pkg['data']['Editable Boxes'] > 0].sort_values(by='SKU Id')
        zero_df = pkg['data'][pkg['data']['Editable Boxes'] == 0].sort_values(by='SKU Id')
        current_box = 1
        for _, row in active_df.iterrows():
            try: boxes = int(row['Editable Boxes'])
            except: boxes = 0
            for _ in range(boxes):
                box_data.append({'Box No': current_box, 'SKU': str(row['SKU Id']), 'FSN': str(row.get('FSN', '')), 'EAN': str(row.get('EAN', '')).replace('.0',''), 'Qty': int(row['PPCN'])})
                current_box += 1
        if not zero_df.empty:
            dummy_count = math.ceil(len(zero_df) / 20)
            for _ in range(dummy_count):
                box_data.append({'Box No': current_box, 'SKU': "MIX SKU", 'FSN': "MIX FSN", 'EAN': "", 'Qty': 1})
                current_box += 1
        st.session_state['scan_box_data'] = pd.DataFrame(box_data)
        st.session_state['scan_c_id'] = c_id
        st.session_state['last_printed_box'] = None

    df_boxes = st.session_state['scan_box_data']

    # --- UI LAYOUT ---
    c_back, c_spacer, c_print = st.columns([1, 4, 2])
    with c_back:
        if st.button("🔙 Back", use_container_width=True): nav('view_saved')
    with c_print:
        # Replaced win32 listing with text input for QZ Tray
        printer_name = st.text_input("Printer Name (QZ Tray)", value="ZDesigner GK420t", key='selected_printer_name')

    st.divider()
    
    # 2. Logic for Printing/Scanning (Modified for QZ Tray)
    def process_scan():
        scan_val = st.session_state.scan_input.strip()
        if not scan_val: return
        matches = df_boxes[(df_boxes['SKU'] == scan_val) | (df_boxes['FSN'] == scan_val) | (df_boxes['EAN'] == scan_val)]
        if matches.empty: st.toast(f"❌ Product not found: {scan_val}", icon="⚠️")
        else:
            printed_set = set(pkg.get('printed_boxes', []))
            valid_boxes = matches[~matches['Box No'].isin(printed_set)]
            if valid_boxes.empty: st.toast(f"✅ All boxes for {scan_val} already printed!", icon="ℹ️")
            else:
                target_box = valid_boxes.iloc[0]['Box No']
                pdf_data = extract_label_pdf_bytes(merged_pdf_bytes, int(target_box)-1)
                if pdf_data:
                    qz_tray_print_component(pdf_data, st.session_state.selected_printer_name)
                    st.session_state['last_printed_box'] = int(target_box)
                    if 'printed_boxes' not in pkg: pkg['printed_boxes'] = []
                    pkg['printed_boxes'].append(int(target_box))
                    save_history(st.session_state['consignments'])
                    st.toast(f"🖨️ Sent Box {target_box} to QZ Tray", icon="✅")
                else: st.toast("Error extracting label PDF", icon="❌")
        st.session_state.scan_input = ""

    def trigger_reprint(box_num):
        pdf_data = extract_label_pdf_bytes(merged_pdf_bytes, int(box_num)-1)
        if pdf_data:
            qz_tray_print_component(pdf_data, st.session_state.selected_printer_name)
            st.session_state['last_printed_box'] = int(box_num)
            st.toast(f"🖨️ Re-sent Box {box_num} to QZ Tray", icon="✅")
        else: st.toast("Error extracting label PDF", icon="❌")

    st.text_input("SCAN BARCODE (EAN / SKU / FSN)", key='scan_input', on_change=process_scan, placeholder="Click here and scan...", help="Press Enter after scanning")

    last_p = st.session_state.get('last_printed_box')
    if last_p: st.info(f"🖨️ Last Printed: **BOX {last_p}**", icon="✨")

    printed_set = set(pkg.get('printed_boxes', []))
    display_df = df_boxes.copy()
    display_df['Status'] = display_df['Box No'].apply(lambda x: '✅ PRINTED' if x in printed_set else 'WAITING')

    def highlight_rows(row):
        box_num = row['Box No']
        if box_num == st.session_state.get('last_printed_box'): return ['background-color: #fff3cd'] * len(row)
        elif row['Status'] == '✅ PRINTED': return ['background-color: #d4edda'] * len(row)
        return [''] * len(row)

    st.subheader("Box List")
    st.caption("💡 Click a row to see Reprint options")
    event = st.dataframe(display_df.style.apply(highlight_rows, axis=1), use_container_width=True, hide_index=True, height=500, on_select="rerun", selection_mode="single-row")

    if event.selection.rows:
        selected_idx = event.selection.rows[0]
        selected_box = display_df.iloc[selected_idx]['Box No']
        col_act1, col_act2 = st.columns([3, 1])
        with col_act1: st.warning(f"Selected: **Box {selected_box}**")
        with col_act2:
            if st.button(f"🖨️ Reprint Box {selected_box}", type="primary", use_container_width=True):
                trigger_reprint(selected_box)
