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
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import json

# Google Drive API scope
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Shopify API configuration
SHOP_DOMAIN = "nonnybeer.myshopify.com"
API_VERSION = "2024-07"
SHOPIFY_TOKEN = os.environ.get("SHOPIFY_TOKEN")
HEADERS = {"X-Shopify-Access-Token": SHOPIFY_TOKEN}

def authenticate_google_drive():
    """Authenticate and return Google Drive service"""
    creds = None
    
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if 'GOOGLE_CREDENTIALS' in os.environ:
                import json
                creds_data = json.loads(os.environ['GOOGLE_CREDENTIALS'])
                flow = InstalledAppFlow.from_client_config(creds_data, SCOPES)
            elif os.path.exists('credentials.json'):
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            else:
                st.warning("No Google credentials found. Google Drive upload will not be available.")
                return None
            
            creds = flow.run_local_server(port=0)
        
        if creds:
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
    
    if creds:
        return build('drive', 'v3', credentials=creds)
    return None

def upload_to_drive(service, filename, filepath):
    """Upload file to Google Drive"""
    file_metadata = {'name': filename}
    media = MediaFileUpload(filepath, resumable=True)
    
    try:
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')
    except Exception as e:
        st.error(f"Error uploading to Drive: {e}")
        return None

def shopify_get(endpoint: str, params: dict):
    """Make GET request to Shopify API"""
    url = f'https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/{endpoint}.json'
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r

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
            break
    
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
        'Customer Type': 'Account Category',
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
    
    # Product line assignment
    df['Product Line'] = df['Sku Description'].apply(lambda x: 
        'Pale Ale' if 'pale' in str(x).lower() else
        'Pilsner' if 'pilsner' in str(x).lower() else
        'IPA' if 'ipa' in str(x).lower() else
        'Dark Lager' if 'lager' in str(x).lower() else
        'Other'
    )
    
    # Add pack information
    df['Packs Per Case'] = 6
    df['Bottles Per Pack'] = 4
    df['Total Bottles'] = df['Quantity'] * df['Packs Per Case'] * df['Bottles Per Pack']
    
    return df

def clean_horizon_data(df):
    """Clean Horizon distributor data"""
    # Drop fully empty columns
    df = df.dropna(axis=1, how='all')
    
    # Keep only selected columns
    columns_to_keep = [
        'Ship Date', 'Customer', 'Address', 'City', 'State', 'Zip',
        'Product', 'SKU', 'Pack', 'Cases', 'Total'
    ]
    df = df[[col for col in columns_to_keep if col in df.columns]]
    
    # Rename columns
    df = df.rename(columns={
        'Ship Date': 'Date',
        'Customer': 'Account Name',
        'Address': 'Address',
        'City': 'City',
        'State': 'Province',
        'Zip': 'Postal Code',
        'Product': 'Sku Description',
        'SKU': 'Sku',
        'Cases': 'Quantity',
        'Total': 'Sales'
    })
    
    # Add missing columns
    df['Sales Channel Category'] = 'Distributor'
    df['Sales Channel Name'] = 'Horizon'
    df['Year'] = pd.to_datetime(df['Date']).dt.year
    df['Month'] = pd.to_datetime(df['Date']).dt.month
    df['Account Category'] = 'Groceries'
    
    # Extract pack information
    df[['Packs Per Case', 'Bottles Per Pack']] = df['Pack'].str.extract(r'(\d+)/(\d+)', expand=True).fillna(0).astype(float)
    df.loc[df['Packs Per Case'] == 0, 'Packs Per Case'] = 6
    df.loc[df['Bottles Per Pack'] == 0, 'Bottles Per Pack'] = 4
    
    # Product line assignment
    df['Product Line'] = df['Sku Description'].apply(lambda x: 
        'Pale Ale' if 'pale' in str(x).lower() else
        'Pilsner' if 'pilsner' in str(x).lower() else
        'IPA' if 'ipa' in str(x).lower() else
        'Dark Lager' if 'lager' in str(x).lower() else
        'Other'
    )
    
    df['Total Bottles'] = df['Quantity'] * df['Packs Per Case'] * df['Bottles Per Pack']
    
    return df

def merge_psc_sheets(file_path):
    """Merge all PSC sheets with improved cleaning"""
    try:
        excel_file = pd.ExcelFile(file_path)
    except Exception as e:
        st.error(f"Error reading PSC file: {e}")
        return pd.DataFrame()
    
    merged_data = []
    
    for sheet_name in excel_file.sheet_names:
        if sheet_name == 'YTD':
            continue
            
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        # Clean Customer column
        if 'Customer' in df.columns:
            df['Customer'] = df['Customer'].fillna('')
            # Remove pattern like ^A^B^C
            df['Customer'] = df['Customer'].apply(lambda x: re.sub(r'\^[A-Z](\^[A-Z])*', '', str(x)))
            df = df[df['Customer'].str.strip() != '']
            print(f"✅ Sheet {sheet_name}: Cleaned Customer column values")
        
        # Standard column renaming
        df = df.rename(columns={
            'Date Sold': 'Date',
            'Customer': 'Account Name',
            'Address 1': 'Address',
            'City': 'City', 
            'State': 'Province',
            'Zip': 'Postal Code',
            'Item': 'Sku',
            'Item Description': 'Sku Description',
            'Cases': 'Quantity',
            'Amount': 'Sales'
        })
        
        # Add metadata
        df['Sales Channel Category'] = 'Distributor'
        df['Sales Channel Name'] = 'PSC'
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        df['Month'] = pd.to_datetime(df['Date']).dt.month
        df['Account Category'] = 'Groceries'
        
        # Product line assignment
        df['Product Line'] = df['Sku Description'].apply(lambda x: 
            'Pale Ale' if 'pale' in str(x).lower() else
            'Pilsner' if 'pilsner' in str(x).lower() else
            'IPA' if 'ipa' in str(x).lower() else
            'Dark Lager' if 'lager' in str(x).lower() else
            'Other'
        )
        
        # Extract pack information from Item Description
        df[['Packs Per Case', 'Bottles Per Pack']] = df['Sku Description'].str.extract(r'(\d+)/(\d+)x', expand=True).astype(float)
        
        # Calculate Total Bottles
        df['Total Bottles'] = df['Quantity'].astype(float) * df['Packs Per Case'] * df['Bottles Per Pack']
        
        # Capitalize columns
        df.columns = [str(col).strip().title() for col in df.columns]
        
        print(f"Sheet {sheet_name} columns: {list(df.columns)}")
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
