# data_processing.py
"""
Data processing module for Nonny Beer sales data
Handles all distributor data cleaning and merging
"""

import os
import re
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from pathlib import Path
import streamlit as st
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import json

# Google Drive API scope
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Shopify API configuration
SHOP_DOMAIN = "nonny-beer.myshopify.com"  # Fixed domain with hyphen
API_VERSION = "2024-04"  # Match working version from Colab
SHOPIFY_TOKEN = os.environ.get("SHOPIFY_TOKEN")
HEADERS = {"X-Shopify-Access-Token": SHOPIFY_TOKEN}

def authenticate_google_drive():
    """Authenticate and return Google Drive service using a service account"""
    try:
        if 'GOOGLE_CREDENTIALS' in os.environ:
            creds_data = json.loads(os.environ['GOOGLE_CREDENTIALS'])
            creds = service_account.Credentials.from_service_account_info(
                creds_data, scopes=SCOPES
            )
            return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.warning(f"Google Drive authentication failed: {e}")
    
    st.warning("Google Drive upload not available. Set GOOGLE_CREDENTIALS environment variable.")
    return None


def upload_to_drive(service, local_file, gdrive_filename, folder_id=None):
    from googleapiclient.http import MediaFileUpload

    file_metadata = {'name': gdrive_filename}
    if folder_id:
        file_metadata['parents'] = [folder_id]

    media = MediaFileUpload(local_file, mimetype='text/csv')

    try:
        service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        print(f"✅ Uploaded {gdrive_filename} to Drive successfully.")
    except Exception as e:
        print(f"❌ Failed to upload {gdrive_filename} to Drive: {e}")

def shopify_get(endpoint: str, params: dict):
    """Make GET request to Shopify API"""
    url = f'https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/{endpoint}.json'
    
    # Debug information
    print(f"🔍 Shopify API URL: {url}")
    print(f"🔍 Headers: {HEADERS}")
    print(f"🔍 Params: {params}")
    
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        print(f"🔍 Response status: {r.status_code}")
        r.raise_for_status()
        return r
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        print(f"❌ Response text: {r.text}")
        raise

def fetch_shopify_orders():
    """Fetch and process Shopify orders with improved logic"""
    if not SHOPIFY_TOKEN:
        st.warning("Shopify API token not configured")
        return pd.DataFrame()
    
    print('🔄 Fetching Shopify orders...')
    
    orders, page_info = [], None
    FIELDS = (
        'id,created_at,total_price,financial_status,source_name,'
        'shipping_address,customer,line_items'
    )
    
    while True:
        params = {'limit': 250, 'status': 'any', 'fields': FIELDS} if not page_info \
            else {'limit': 250, 'page_info': page_info}
        
        try:
            resp = shopify_get('orders', params)
            batch = resp.json().get('orders', [])
            if not batch:
                break
            orders.extend(batch)
            
            link_hdr = resp.headers.get('Link', '')
            m = re.search(r'<[^>]+page_info=([^&>]+)[^>]*>;\s*rel="next"', link_hdr)
            page_info = m.group(1) if m else None
            if not page_info:
                break
        except Exception as e:
            st.error(f"Error fetching Shopify orders: {e}")
            return pd.DataFrame()  # Return empty DataFrame on error
    
    print(f'✅ Total orders pulled: {len(orders)}')
    
    # Province mapping
    province_map = {
        'AB': 'Alberta', 'BC': 'British Columbia', 'MB': 'Manitoba',
        'NB': 'New Brunswick', 'NL': 'Newfoundland and Labrador',
        'NS': 'Nova Scotia', 'NT': 'Northwest Territories', 'NU': 'Nunavut',
        'ON': 'Ontario', 'PE': 'Prince Edward Island', 'QC': 'Quebec',
        'SK': 'Saskatchewan', 'YT': 'Yukon'
    }
    
    # Channel simplification
    def simplify_channel(source_name):
        if source_name is None:
            return 'Unknown'
        name = str(source_name).lower().strip()
        if name in ['web', 'pos', 'faire', 'airgoods', 'iphone', 'stack', 'shopify_draft_order']:
            return name.title()
        elif name.isnumeric():
            return 'Third Party'
        else:
            return 'Other'
    
    # Product line assignment
    def assign_product_line(text):
        text = str(text).lower()
        if 'pale' in text:
            return 'Pale Ale'
        elif 'pilsner' in text:
            return 'Pilsner'
        elif 'ipa' in text:
            return 'IPA'
        elif 'lager' in text:
            return 'Dark Lager'
        else:
            return 'Other'
    
    # Extract bottles per pack with improved logic
    def extract_bottles_per_pack(text, quantity):
        """Extract bottles per pack using various patterns"""
        text = str(text).lower()
        
        # 1. X-pack pattern (highest priority)
        pack_match = re.search(r'(\d+)-pack', text)
        if pack_match:
            return float(pack_match.group(1)), quantity * float(pack_match.group(1)), 'X-pack'
        
        # 2. X pk pattern
        pk_match = re.search(r'(\d+)\s*pk', text)
        if pk_match:
            return float(pk_match.group(1)), quantity * float(pk_match.group(1)), 'X pk'
        
        # 3. X btls pattern
        btls_match = re.search(r'(\d+)\s*btls', text)
        if btls_match:
            return float(btls_match.group(1)), quantity * float(btls_match.group(1)), 'X btls'
        
        # 4. X*X or X&X pattern
        mult_match = re.findall(r'(\d+)\s*[*&x×]\s*(\d+)', text)
        if mult_match:
            a, b = map(int, mult_match[0])
            rule = '* or &'
            if a == b:
                occur = len(re.findall(rf'{a}\s*[*&x×]\s*{a}', text)) or 1
                total = occur * b * quantity
            else:
                total = a * b * quantity
            return float(b), total, rule
        
        # 5. Single fallback
        if re.search(r'\bsingle\b', text, flags=re.IGNORECASE):
            return 1.0, quantity * 1.0, 'single'
        
        return None, None, 'none'
    
    # Build records
    records = []
    customer_wholesale_flags = {}
    
    for o in orders:
        dt = pd.to_datetime(o['created_at'])
        date_only = pd.to_datetime(dt.date())
        ship = o.get('shipping_address') or {}
        cust = o.get('customer') or {}
        channel = simplify_channel(o.get('source_name'))
        total = float(o.get('total_price', 0.0))
        province_full = province_map.get(ship.get('province_code'), ship.get('province_code'))
        customer_name = f"{cust.get('first_name', '')} {cust.get('last_name', '')}".strip() or 'Guest'
        
        for it in o.get('line_items', []):
            product_name = str(it.get('name'))
            quantity = it.get('quantity')
            sku = it.get('sku')
            
            bottles_per_pack, total_bottles, match_rule = extract_bottles_per_pack(product_name, quantity)
            
            if 'wholesale' in product_name.lower():
                customer_wholesale_flags[customer_name] = True
            
            records.append({
                'Date': date_only,
                'Year': dt.year,
                'Month': dt.month,
                'Sales Channel Name': channel,
                'Sales Channel Category': 'DTC',
                'Account Name': customer_name,
                'Address': ship.get('address1'),
                'City': ship.get('city'),
                'Province': province_full,
                'Postal Code': ship.get('zip'),
                'Sku': sku,
                'Sku Description': product_name,
                'Product Line': assign_product_line(product_name),
                'Quantity': quantity,
                'Bottles Per Pack': bottles_per_pack,
                'Packs Per Case': 6.0,
                'Total Bottles': total_bottles,
                'Sales': total,
            })
    
    df = pd.DataFrame(records)
    
    # Assign account categories based on wholesale flags
    df['Account Category'] = df['Account Name'].map(
        lambda x: 'Commercial' if customer_wholesale_flags.get(x, False) else 'Personal'
    )
    
    # Clean column names
    df.columns = [' '.join(w.capitalize() for w in col.split()) for col in df.columns]
    
    # Drop rows where Sales or Quantity is zero or null
    df = df[(df['Sales'] > 0) & (df['Quantity'] > 0)]
    
    return df

def clean_ollie_data(df):
    """Clean Ollie distributor data"""
    # Drop fully empty columns
    df = df.dropna(axis=1, how='all')
    
    # Keep only selected columns
    columns_to_keep = [
        'Date', 'Buyer', 'Customer Type', 'Address1', 'City', 'State',
        'Zip/postal code', 'Variant Name', 'SKU', 'Quantity', 'Total'
    ]
    df = df[[col for col in columns_to_keep if col in df.columns]]
    
    # Rename columns
    df = df.rename(columns={
        'Date': 'Date',
        'Buyer': 'Account Name',
        'Address1': 'Address',
        'City': 'City',
        'State': 'Province',
        'Zip/postal code': 'Postal Code',
        'Variant Name': 'Sku Description',
        'SKU': 'Sku',
        'Quantity': 'Quantity',
        'Total': 'Sales'
    })
    
    # Add missing columns
    df['Sales Channel Category'] = 'Distributor'
    df['Sales Channel Name'] = 'Ollie'
    df['Year'] = pd.to_datetime(df['Date']).dt.year
    df['Month'] = pd.to_datetime(df['Date']).dt.month
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # Product line assignment
    df['Product Line'] = df['Sku Description'].apply(lambda x: 
        'Pale Ale' if 'pale' in str(x).lower() else
        'Pilsner' if 'pilsner' in str(x).lower() else
        'IPA' if 'ipa' in str(x).lower() else
        'Dark Lager' if 'lager' in str(x).lower() else
        'Other'
    )
    
    # Map Customer Type to Account Category
    customer_type_mapping = {
        'LIC': 'Restaurant/Bar',
        'GRC': 'Grocery',
        'LRS': 'Retail Store',
        'RAS': 'Rural Store',
        'MOS': 'Manufacturer Channel',
        'COU': 'Other'
    }
    df['Account Category'] = df['Customer Type'].map(customer_type_mapping)

    # Extract Bottles per Pack
    pack1 = df['Sku Description'].str.extract(r'(\d+)-pack', expand=False)
    pack2 = df['Sku Description'].str.extract(r'/(\d+)\*', expand=False)
    df['Bottles Per Pack'] = pack1.fillna(pack2).astype('float')

    df['Packs Per Case'] = 6.0
    df['Total Bottles'] = df['Quantity'] * df['Bottles Per Pack']

    # Drop unused and zero rows
    df = df.drop(columns=['Customer Type'])
    df = df[(df["Quantity"] != 0) & (df["Sales"] != 0)]

    # Standardize column names to title case
    df.columns = [col.title() for col in df.columns]
    
    return df

def clean_horizon_data(df):
    """
    Clean combined Horizon sales data from Drive.
    Applies column standardization, metadata extraction, and derived features.
    """
    import re

    # Drop unnecessary columns (only if they exist)
    columns_to_drop = ['CODE','BRAND','STATUS','SALES CHANGE','MCB%','YTD SALES','UPC','YTD SALES CHANGE','YTD MCB%']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    # Rename columns (if applicable)
    rename_dict = {
        'CUSTOMER': 'Account Name',
        'SKU#': 'SKU',
        'POSTAL': 'POSTAL CODE'
    }
    df = df.rename(columns=rename_dict)

    # --- Drop total summary row, if present ---
    df = df.dropna(axis=1, how="all")
    df = drop_total_rows(df)

    # --- Drop blank Account Name rows ---
    df['Account Name'] = df['Account Name'].astype(str).str.strip()
    df = df[df['Account Name'].notna() & (df['Account Name'] != '')]
    df = df[~df['Account Name'].str.lower().isin(['nan'])]

    # --- Standardize column names ---
    df.columns = [col.title() for col in df.columns]

    # --- Extract year and month from filename ---
    df['Year'], df['Month'] = zip(*df['File Name'].map(extract_year_month_from_filename))

    # --- Construct Date ---
    df['Date'] = pd.to_datetime(df[['Year', 'Month']].assign(DAY=1))

    # --- Filter out zero sales/quantity ---
    df = df[(df["Quantity"] != 0) & (df["Sales"] != 0)]

    # --- Add Sales Channel ---
    df['Sales Channel Category'] = "Distributor"
    df['Sales Channel Name'] = 'Horizon'

    # --- Add Customer Category ---
    def get_customer_type(name):
        name = str(name).lower()
        if any(k in name for k in ['restaurant', 'bar', 'cafe']):
            return 'Restaurant/Bar'
        elif any(k in name for k in ['grocery', 'market']):
            return 'Grocery'
        elif any(k in name for k in ['liquor', 'store', 'shop']):
            return 'Retail Store'
        else:
            return 'Other'
    df['Account Category'] = df['Account Name'].apply(get_customer_type)

    # --- Add Product Line ---
    def get_product_line(desc):
        desc = str(desc).lower()
        if 'pale' in desc:
            return 'Pale Ale'
        elif 'pilsner' in desc:
            return 'Pilsner'
        elif 'ipa' in desc:
            return 'IPA'
        elif 'lager' in desc:
            return 'Dark Lager'
        else:
            return 'Other'
    df['Product Line'] = df['Sku Description'].apply(get_product_line)

    # --- Add Bottles Per Pack and Total Bottles ---
    df[['Packs Per Case', 'Bottles Per Pack']] = df['Sku Description'].str.extract(r'(\d+)/(\d+)x', expand=True).astype(float)
    df['Total Bottles'] = df['Quantity'] * df['Packs Per Case'] * df['Bottles Per Pack']

    return df

def merge_psc_sheets(file_obj):
    """
    Merge all PSC sheets from a BytesIO Excel file (cloud-compatible version).
    This version matches Colab logic exactly but is adapted for in-memory stream input.
    """
    import pandas as pd
    import re
    import os
    import glob
    from io import BytesIO

    try:
        # Read all sheets from Excel, no header
        excel_file = pd.ExcelFile(file_obj)
        all_sheets = {name: pd.read_excel(excel_file, sheet_name=name, header=None) for name in excel_file.sheet_names}
    except Exception as e:
        print(f"❌ Error reading PSC file: {e}")
        return pd.DataFrame()

    merged_data = []

    for sheet_name, df in all_sheets.items():
        year, month = extract_year_month_from_sheetname(sheet_name)
        if year is None or month is None:
            print(f"⚠️ Skipping invalid sheet name: {sheet_name}")
            continue

        # Set first row as header, remove header/footer rows
        df.columns = df.iloc[0]
        df = df[1:]
        df = df.iloc[:-2] if df.shape[0] > 2 else df
        df = df.loc[:, ~df.columns.astype(str).str.contains('^Unnamed', na=False)]

        # Heuristic detection of customer column
        col_names = df.columns.tolist()
        customer_col = None
        for col in col_names:
            if 'customer' in str(col).lower():
                customer_col = col
                break
        if customer_col is None and len(col_names) > 1:
            customer_col = col_names[1]
        elif customer_col is None and len(col_names) > 0:
            customer_col = col_names[0]

        if customer_col:
            df.rename(columns={customer_col: "Customer"}, inplace=True)

        if "Customer" in df.columns:
            def clean_customer_value(x):
                if pd.isna(x):
                    return 'Unknown'
                x_str = str(x)
                x_str = re.sub(r'\d{2}/\d{2}/\d{4}\s*-\s*\d{2}/\d{2}/\d{4}', '', x_str)
                x_str = re.sub(r'\d{2}/\d{2}/\d{4}', '', x_str)
                x_str = re.sub(r'\s*\n\s*', ' ', x_str).strip()
                x_str = re.sub(r'\s+', ' ', x_str).strip()
                return x_str if x_str else 'Unknown'

            df["Customer"] = df["Customer"].apply(clean_customer_value)
            print(f"✅ Sheet {sheet_name}: Cleaned Customer column values")

        # Add metadata
        df['Sales Channel Category'] = "Distributor"
        df['Sales Channel Name'] = 'PSC'
        df["Year"] = year
        df["Month"] = month
        df["Date"] = pd.to_datetime(dict(year=df["Year"], month=df["Month"], day=1))

        # Rename known columns
        rename_dict = {
            "SKU#": "Sku", "QTY": "Quantity", "PROV": "Province",
            "SKU DESCRIPTION": "Sku Description", "SALES": "Sales", "Customer": "Account Name"
        }
        df.rename(columns={col: rename_dict.get(col, col) for col in df.columns}, inplace=True)
        df.drop(columns=["BROKER", "CODE", "BRAND", "UPC"], inplace=True, errors='ignore')
        df = df[(df["Quantity"] != 0) & (df["Sales"] != 0)]

        def get_customer_type(name):
            name = str(name).lower()
            if any(k in name for k in ['restaurant', 'bar', 'cafe']):
                return 'Restaurant/Bar'
            elif any(k in name for k in ['grocery', 'market', 'grocer']):
                return 'Grocery'
            elif any(k in name for k in ['liquor', 'store', 'shop']):
                return 'Retail Store'
            else:
                return 'Other'
        df['Account Category'] = df['Account Name'].apply(get_customer_type)

        def get_product_line(desc):
            desc = str(desc).lower()
            if 'pale' in desc:
                return 'Pale Ale'
            elif 'pilsner' in desc:
                return 'Pilsner'
            elif 'ipa' in desc:
                return 'IPA'
            elif 'lager' in desc:
                return 'Dark Lager'
            else:
                return 'Other'
        df['Product Line'] = df['Sku Description'].apply(get_product_line)

        df[['Packs Per Case', 'Bottles Per Pack']] = df['Sku Description'].str.extract(r'(\d+)/(\d+)x', expand=True).astype(float)
        df['Total Bottles'] = df['Quantity'].astype(float) * df['Packs Per Case'] * df['Bottles Per Pack']
        df.columns = [str(col).strip().title() for col in df.columns]

        merged_data.append(df)

    if merged_data:
        result = pd.concat(merged_data, ignore_index=True)
        print(f"✅ Successfully merged {len(merged_data)} sheets with {len(result)} total rows")
        return result
    else:
        print("⚠️ No valid PSC data found.")
        return pd.DataFrame()

def process_single_file(file):
    """Process a single uploaded file"""
    try:
        # Read file based on extension
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)
        
        # Determine file type and clean accordingly
        if 'ollie' in file.name.lower():
            return clean_ollie_data(df)
        elif 'horizon' in file.name.lower():
            return clean_horizon_data(df)
        elif 'psc' in file.name.lower():
            return merge_psc_sheets(file)
        else:
            st.warning(f"Unknown file type: {file.name}")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error processing {file.name}: {e}")
        return pd.DataFrame()

def process_all_data(uploaded_files):
    """Process all uploaded files"""
    all_data = []
    
    for file in uploaded_files:
        df = process_single_file(file)
        if not df.empty:
            all_data.append(df)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()

def generate_account_status(df):
    """Generate account status report"""
    if df.empty:
        return pd.DataFrame()
    
    # Get the latest date
    latest_date = pd.to_datetime(df['Date']).max()
    cutoff_date = latest_date - timedelta(days=90)
    
    # Group by account
    account_summary = df.groupby('Account Name').agg({
        'Date': lambda x: pd.to_datetime(x).max(),
        'Sales': 'sum',
        'Quantity': 'sum',
        'Total Bottles': 'sum'
    }).reset_index()
    
    # Determine status
    account_summary['Account Status'] = account_summary['Date'].apply(
        lambda x: 'Active' if x >= cutoff_date else 
                  'Check-In Needed' if x >= cutoff_date - timedelta(days=90) else 
                  'Non-Active'
    )
    
    # Add days since last order
    account_summary['Days Since Last Order'] = (latest_date - account_summary['Date']).dt.days
    
    # Rename columns
    account_summary = account_summary.rename(columns={
        'Date': 'Last Order Date',
        'Sales': 'Total Sales',
        'Quantity': 'Total Orders',
        'Total Bottles': 'Total Bottles Ordered'
    })
    
    return account_summary

def load_clean_horizon_from_drive(folder_id):
    """
    Download all Horizon distributor sales reports from a Google Drive folder (via service account),
    extract year and month from filenames, and return a cleaned, enriched DataFrame.
    """
    import io
    import os
    import json
    import pandas as pd
    import re
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    # Load Google Drive service account credentials from environment variable
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/drive"])
    service = build("drive", "v3", credentials=creds)

    # Query all .csv or .xlsx files inside the specified folder
    query = f"'{folder_id}' in parents and trashed = false and (mimeType='text/csv' or mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        raise ValueError("❌ No Horizon files found in the folder.")

    df_list = []

    # Loop through each file and download + parse
    for f in files:
        file_id = f["id"]
        file_name = f["name"]

        # Download file content into memory
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)

        # Read as DataFrame
        try:
            if file_name.endswith(".csv"):
                df = pd.read_csv(fh)
            else:
                df = pd.read_excel(fh, header=2)  # Skip first 2 header rows (if applicable)

            df["File Name"] = file_name
            df_list.append(df)

        except Exception as e:
            print(f"❌ Failed to read {file_name}, error: {e}")

    if not df_list:
        raise ValueError("❌ No readable files found.")

    # Combine all monthly files into a single DataFrame
    df_combined = pd.concat(df_list, ignore_index=True)

    # Extract Year and Month from filename using your utility function
    df_combined["Year"], df_combined["Month"] = zip(*df_combined["File Name"].map(extract_year_month_from_filename))

    # Construct a full datetime object as the first day of the month
    df_combined["Date"] = pd.to_datetime(df_combined[["Year", "Month"]].assign(DAY=1))

    # Clean and enrich the merged data using your centralized cleaning logic
    return clean_horizon_data(df_combined)

def load_clean_psc_from_drive(folder_id):
    """
    Download a single Excel file from the PSC folder in Google Drive,
    extract year/month from sheet name, merge all sheets, and return cleaned DataFrame.
    """
    import io
    import json
    import re
    import pandas as pd
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    # Load credentials from environment variable
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build("drive", "v3", credentials=creds)

    # Query PSC Excel files in the folder
    query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        raise ValueError("❌ No PSC Excel files found.")
    if len(files) > 1:
        raise ValueError("⚠️ Multiple PSC Excel files found. Only one expected.")

    # Download the file content into memory
    file_id = files[0]["id"]
    file_name = files[0]["name"]
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)

    # 🔽 Extract year/month from each sheet name
    xl = pd.ExcelFile(fh)
    sheet_info = []
    for sheet_name in xl.sheet_names:
        year, month = extract_year_month_from_sheetname(sheet_name)
        if year is not None and month is not None:
            sheet_info.append((sheet_name, year, month))

    # Optional: use sheet_info to log or tag DataFrame later
    # print(sheet_info)  # or store them for downstream logic

    # ✅ Merge all cleaned sheets and return DataFrame
    fh.seek(0)  # reset pointer again in case merge_psc_sheets needs to re-read
    df_cleaned = merge_psc_sheets(fh)

    return df_cleaned


def load_clean_ollie_from_drive(folder_id):
    """Download all CSVs from Ollie folder and clean"""
    import io
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/drive"])
    service = build("drive", "v3", credentials=creds)

    query = f"'{folder_id}' in parents and mimeType='text/csv' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        raise ValueError("❌ No Ollie CSV files found in the folder.")

    df_list = []
    for f in files:
        request = service.files().get_media(fileId=f["id"])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        df_list.append(df)

    df_combined = pd.concat(df_list, ignore_index=True)
    return clean_ollie_data(df_combined)

def extract_year_month_from_sheetname(sheet_name):
    match = re.search(r'([A-Za-z]+)\s+(20\d{2})', sheet_name)
    if not match:
        return None, None
    month_str = match.group(1).lower()
    year = int(match.group(2))

    month_map = {
        'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
        'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6, 'jul': 7, 'july': 7,
        'aug': 8, 'august': 8, 'sep': 9, 'sept': 9, 'september': 9,
        'oct': 10, 'october': 10, 'nov': 11, 'november': 11, 'dec': 12, 'december': 12
    }

    month = month_map.get(month_str)
    return year, month


def extract_year_month_from_filename(filename):
    """
    Extract year and month from filename. Supports both full and abbreviated month names, with or without a period.
    """
    # Extract 4-digit year
    year_match = re.search(r'(20\d{2})', filename)

    # Extract month name (e.g., Jan, Jan., January, etc.)
    month_match = re.search(
        r'(Jan\.?|Feb\.?|Mar\.?|Apr\.?|May\.?|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\.?|'
        r'January|February|March|April|May|June|July|August|September|October|November|December)',
        filename, re.IGNORECASE
    )

    if not year_match or not month_match:
        return None, None

    year = int(year_match.group(1))
    month_str = month_match.group(1).lower().rstrip('.')  # Normalize month string

    # Map month name to month number
    month_map = {
        'jan': 1, 'january': 1,
        'feb': 2, 'february': 2,
        'mar': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5,
        'jun': 6, 'june': 6,
        'jul': 7, 'july': 7,
        'aug': 8, 'august': 8,
        'sep': 9, 'sept': 9, 'september': 9,
        'oct': 10, 'october': 10,
        'nov': 11, 'november': 11,
        'dec': 12, 'december': 12
    }

    month = month_map.get(month_str, None)
    return year, month

def drop_total_rows(df):
    """
    Drop the final row if it appears to be a 'Total' summary row (e.g., entirely blank or starts with 'total').
    """
    if df.empty:
        return df

    last_row = df.iloc[-1]

    # Drop if last row is fully null
    if last_row.isnull().all():
        return df.iloc[:-1]

    # Drop if first cell contains the word 'total'
    if isinstance(last_row.iloc[0], str) and 'total' in last_row.iloc[0].lower():
        return df.iloc[:-1]

    return df

