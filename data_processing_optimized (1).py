# data_processing_optimized.py
"""
优化的数据处理模块，支持增量更新和缓存
完整替代 community_project.py 的所有功能
"""

import os
import re
import pandas as pd
import json
import hashlib
import requests
from datetime import datetime, timedelta
from pathlib import Path
import pickle
import streamlit as st
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Google Drive API scope
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate_google_drive():
    """Authenticate and return Google Drive service"""
    creds = None
    
    # Token file stores the user's access and refresh tokens
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # 在Streamlit Cloud上，使用secrets
            if 'GOOGLE_CREDENTIALS' in os.environ:
                # 从环境变量读取凭证
                import json as json_lib
                creds_data = json_lib.loads(os.environ['GOOGLE_CREDENTIALS'])
                flow = InstalledAppFlow.from_client_config(creds_data, SCOPES)
            elif os.path.exists('credentials.json'):
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            else:
                st.warning("No Google credentials found. Google Drive upload will not be available.")
                return None
            
            try:
                creds = flow.run_local_server(port=0)
            except Exception as e:
                st.warning(f"Could not authenticate with Google Drive: {e}")
                return None
        
        # Save the credentials for the next run
        if creds and not os.environ.get('STREAMLIT_CLOUD'):  # 不在云端时才保存
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
    
    try:
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.warning(f"Could not build Google Drive service: {e}")
        return None

def upload_to_drive(service, file_path, file_name, folder_id=None):
    """Upload a file to Google Drive"""
    if not service:
        return None
        
    file_metadata = {'name': file_name}
    if folder_id:
        file_metadata['parents'] = [folder_id]
    
    try:
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        return file.get('id')
    except Exception as e:
        st.error(f"Error uploading to Google Drive: {e}")
        return None

# 用于处理单独上传文件的函数
def process_all_data(uploaded_files=None):
    """
    处理上传的文件（用于单独文件上传，不使用缓存）
    """
    if not uploaded_files:
        return pd.DataFrame()
    
    processor = DataProcessor()
    combined_df = pd.DataFrame()
    
    for uploaded_file in uploaded_files:
        try:
            # 读取文件
            if uploaded_file.name.endswith('.csv'):
                df_raw = pd.read_csv(uploaded_file)
                # 判断文件类型并处理
                if 'ollie' in uploaded_file.name.lower():
                    df = processor.clean_ollie_data(df_raw)
                else:
                    df = df_raw
            else:
                df = pd.read_excel(uploaded_file)
            
            if not df.empty:
                combined_df = pd.concat([combined_df, df], axis=0, ignore_index=True)
                st.success(f"✅ Processed: {uploaded_file.name}")
                    
        except Exception as e:
            st.error(f"Error processing {uploaded_file.name}: {e}")
    
    return combined_df

# 生成账户状态（独立函数版本）
def generate_account_status(df):
    """生成账户状态报告"""
    processor = DataProcessor()
    return processor.generate_account_status(df)

class DataProcessor:
    """优化的数据处理器，支持增量更新和缓存"""
    
    def __init__(self, cache_dir='data_cache'):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.processed_files_log = self.cache_dir / 'processed_files.json'
        self.master_data_cache = self.cache_dir / 'master_data.pkl'
        self.shopify_cache = self.cache_dir / 'shopify_last_sync.json'
        self.load_processed_files_log()
        
        # Shopify configuration
        self.SHOP_DOMAIN = os.environ.get('SHOP_DOMAIN', 'nonny-beer.myshopify.com')
        self.API_VERSION = '2024-04'
        self.TOKEN = os.environ.get('SHOPIFY_TOKEN', '')
        self.HEADERS = {'X-Shopify-Access-Token': self.TOKEN}
    
    def load_processed_files_log(self):
        """加载已处理文件的日志"""
        if self.processed_files_log.exists():
            with open(self.processed_files_log, 'r') as f:
                self.processed_files = json.load(f)
        else:
            self.processed_files = {}
    
    def save_processed_files_log(self):
        """保存已处理文件的日志"""
        with open(self.processed_files_log, 'w') as f:
            json.dump(self.processed_files, f, indent=2)
    
    def get_file_hash(self, file_path):
        """计算文件的MD5哈希值"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def is_file_processed(self, file_path, file_hash=None):
        """检查文件是否已经处理过且未更改"""
        file_str = str(file_path)
        if file_str not in self.processed_files:
            return False
        
        if file_hash is None:
            file_hash = self.get_file_hash(file_path)
        
        return self.processed_files[file_str]['hash'] == file_hash
    
    def load_master_data(self):
        """加载主数据缓存"""
        if self.master_data_cache.exists():
            with open(self.master_data_cache, 'rb') as f:
                return pickle.load(f)
        return pd.DataFrame()
    
    def save_master_data(self, df):
        """保存主数据缓存"""
        with open(self.master_data_cache, 'wb') as f:
            pickle.dump(df, f)
    
    # ===== Shopify 相关函数 =====
    
    def shopify_get(self, endpoint: str, params: dict):
        """Make GET request to Shopify API"""
        url = f'https://{self.SHOP_DOMAIN}/admin/api/{self.API_VERSION}/{endpoint}.json'
        r = requests.get(url, headers=self.HEADERS, params=params, timeout=30)
        r.raise_for_status()
        return r
    
    def get_shopify_last_sync(self):
        """获取上次Shopify同步的时间"""
        if self.shopify_cache.exists():
            with open(self.shopify_cache, 'r') as f:
                data = json.load(f)
                return datetime.fromisoformat(data.get('last_sync', '2024-01-01'))
        return datetime(2024, 1, 1)  # 默认从2024年开始
    
    def save_shopify_last_sync(self, sync_date):
        """保存Shopify同步时间"""
        with open(self.shopify_cache, 'w') as f:
            json.dump({'last_sync': sync_date.isoformat()}, f)
    
    def fetch_shopify_orders(self, start_date=None, end_date=None, incremental=True):
        """
        获取Shopify订单
        - incremental=True: 只获取上次同步后的新订单
        - incremental=False: 获取指定日期范围的所有订单
        """
        if incremental and start_date is None:
            start_date = self.get_shopify_last_sync()
        elif start_date is None:
            start_date = datetime(2024, 1, 1)
        
        if end_date is None:
            end_date = datetime.now()
        
        print(f'🔄 Fetching Shopify orders from {start_date.date()} to {end_date.date()}...')
        
        orders = []
        FIELDS = (
            'id,created_at,total_price,financial_status,source_name,'
            'shipping_address,customer,line_items'
        )
        
        # 添加日期过滤
        params = {
            'limit': 250,
            'status': 'any',
            'fields': FIELDS,
            'created_at_min': start_date.isoformat(),
            'created_at_max': end_date.isoformat()
        }
        
        page_info = None
        while True:
            if page_info:
                params = {'limit': 250, 'page_info': page_info}
            
            try:
                resp = self.shopify_get('orders', params)
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
                print(f"Error fetching Shopify orders: {e}")
                break
        
        print(f'✅ Total orders pulled: {len(orders)}')
        
        # 处理订单数据
        df = self.process_shopify_orders(orders)
        
        # 更新最后同步时间
        if incremental and not df.empty:
            self.save_shopify_last_sync(end_date)
        
        return df
    
    def process_shopify_orders(self, orders):
        """处理Shopify订单数据"""
        # Province mapping
        province_map = {
            'AB': 'Alberta', 'BC': 'British Columbia', 'MB': 'Manitoba',
            'NB': 'New Brunswick', 'NL': 'Newfoundland and Labrador',
            'NS': 'Nova Scotia', 'NT': 'Northwest Territories', 'NU': 'Nunavut',
            'ON': 'Ontario', 'PE': 'Prince Edward Island', 'QC': 'Quebec',
            'SK': 'Saskatchewan', 'YT': 'Yukon'
        }
        
        # Helper functions
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
        
        # Process records
        records = []
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
                product_name = it.get('name')
                quantity = it.get('quantity')
                
                # Extract cans per pack
                pack1 = re.search(r'(\d+)-pack', str(product_name).lower())
                pack2 = re.search(r'/(\d+)\*', str(product_name).lower())
                cans_per_pack = float(pack1.group(1)) if pack1 else float(pack2.group(1)) if pack2 else None
                
                records.append({
                    'Date': date_only,
                    'Year': dt.year,
                    'Month': dt.month,
                    'Sales Channel Name': channel,
                    'Sales Channel Category': 'DTC',
                    'Account Name': customer_name,
                    'Account Category': 'Personal',
                    'Address': ship.get('address1'),
                    'City': ship.get('city'),
                    'Province': province_full,
                    'Postal Code': ship.get('zip'),
                    'Sku': it.get('sku'),
                    'Sku Description': product_name,
                    'Product Line': assign_product_line(product_name),
                    'Quantity': quantity,
                    'Cans Per Pack': cans_per_pack,
                    'Packs Per Case': 6.0,
                    'Total Cans': quantity * cans_per_pack if cans_per_pack else None,
                    'Sales': total,
                })
        
        df = pd.DataFrame(records)
        
        if not df.empty:
            # Clean column names
            df.columns = [' '.join(w.capitalize() for w in col.split()) for col in df.columns]
            
            # Drop rows where Sales or Quantity is zero or null
            df = df[(df['Sales'] > 0) & (df['Quantity'] > 0)]
        
        return df
    
    # ===== Ollie 数据处理 =====
    
    def clean_ollie_data(self, df):
        """Clean Ollie distributor data"""
        # Drop fully empty columns
        df = df.dropna(axis=1, how='all')
        
        # Keep only selected columns
        columns_to_keep = [
            'Date', 'Buyer', 'Customer Type', 'Address1', 'City', 'State',
            'Zip/postal code', 'Variant Name', 'SKU', 'Quantity', 'Total'
        ]
        df = df[[col for col in columns_to_keep if col in df.columns]]
        
        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Add Year and Month columns
        df['Year'] = df['Date'].dt.year
        df['Month'] = df['Date'].dt.month
        
        # Rename columns
        df = df.rename(columns={
            'Total': 'Sales',
            'State': 'Province',
            'Zip/postal code': 'Postal Code',
            'Variant Name': 'Sku Description',
            'Buyer': 'Account Name',
            'Address1': 'Address'
        })
        
        # Add Sales Channel info
        df['Sales Channel Category'] = 'Wholesale'
        df['Sales Channel Name'] = 'Ollie'
        
        # Customer type mapping
        customer_type_mapping = {
            'LIC': 'Restaurant/Bar',
            'GRC': 'Grocery',
            'LRS': 'Retail Store',
            'RAS': 'Rural Store',
            'MOS': 'Manufacturer Channel',
            'COU': 'Other'
        }
        
        df['Account Category'] = df['Customer Type'].map(customer_type_mapping)
        
        # Product Line assignment
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
        
        df['Product Line'] = df['Sku Description'].apply(assign_product_line)
        
        # Extract Bottles per Pack
        pack1 = df['Sku Description'].str.extract(r'(\d+)-pack', expand=False)
        pack2 = df['Sku Description'].str.extract(r'/(\d+)\*', expand=False)
        df['Cans Per Pack'] = pack1.fillna(pack2).astype('float')
        
        df['Packs Per Case'] = 6.0
        
        # Calculate Total Bottles
        df['Total Cans'] = df['Quantity'] * df['Cans Per Pack']
        
        df = df.drop(columns=['Customer Type'])
        df = df[(df["Quantity"] != 0) & (df["Sales"] != 0)]
        
        # Standardize column names
        df.columns = [col.title() for col in df.columns]
        
        return df
    
    # ===== Horizon 数据处理 =====
    
    def extract_year_month_from_filename(self, filename):
        """从文件名提取年月信息"""
        # Extract 4-digit year
        year_match = re.search(r'(20\d{2})', filename)
        
        # Extract month name
        month_match = re.search(
            r'(Jan\.?|Feb\.?|Mar\.?|Apr\.?|May\.?|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\.?|'
            r'January|February|March|April|May|June|July|August|September|October|November|December)',
            filename, re.IGNORECASE
        )
        
        if not year_match or not month_match:
            return None, None
        
        year = int(year_match.group(1))
        month_str = month_match.group(1).lower().rstrip('.')
        
        # Map month name to month number
        month_map = {
            'jan': 1, 'january': 1, 'feb': 2, 'february': 2,
            'mar': 3, 'march': 3, 'apr': 4, 'april': 4,
            'may': 5, 'jun': 6, 'june': 6, 'jul': 7, 'july': 7,
            'aug': 8, 'august': 8, 'sep': 9, 'sept': 9, 'september': 9,
            'oct': 10, 'october': 10, 'nov': 11, 'november': 11,
            'dec': 12, 'december': 12
        }
        
        month = month_map.get(month_str, None)
        return year, month
    
    def drop_total_rows(self, df):
        """删除汇总行"""
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
    
    def process_horizon_file(self, file_path):
        """处理单个Horizon文件"""
        year, month = self.extract_year_month_from_filename(file_path.name)
        if year is None or month is None:
            raise ValueError(f"无法从文件名提取日期: {file_path.name}")
        
        # 读取文件
        df = pd.read_excel(file_path, header=2)
        
        # 清理数据
        columns_to_drop = ['CODE','BRAND','STATUS','SALES CHANGE','MCB%','YTD SALES','UPC','YTD SALES CHANGE','YTD MCB%']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        
        # 重命名列
        rename_dict = {
            'CUSTOMER': 'Account Name',
            'SKU#': 'SKU',
            'POSTAL': 'POSTAL CODE'
        }
        df = df.rename(columns=rename_dict)
        
        # 删除汇总行
        df = self.drop_total_rows(df)
        
        # 添加元数据
        df['Sales Channel Category'] = "Distributor"
        df['Sales Channel Name'] = 'Horizon'
        df['Year'] = year
        df['Month'] = month
        df['Date'] = pd.to_datetime(df[['Year', 'Month']].assign(DAY=1))
        
        # 添加账户类别
        def get_customer_type(name):
            name = str(name).lower()
            if any(k in name for k in ['restaurant', 'bar', 'cafe']):
                return 'Restaurant/Bar'
            elif any(k in name for k in ['grocery', 'market']):
                return 'Grocery'
            elif 'liquor' in name or 'store' in name:
                return 'Retail Store'
            else:
                return 'Other'
        
        df['Account Category'] = df['Account Name'].apply(get_customer_type)
        
        # 添加产品线
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
        
        df['Product Line'] = df['SKU DESCRIPTION'].apply(get_product_line) if 'SKU DESCRIPTION' in df.columns else 'Other'
        
        # 提取包装信息
        if 'SKU DESCRIPTION' in df.columns:
            df[['Packs Per Case', 'Cans Per Pack']] = df['SKU DESCRIPTION'].str.extract(r'(\d+)/(\d+)x', expand=True).astype(float)
            df['Total Cans'] = df['QUANTITY'] * df['Packs Per Case'] * df['Cans Per Pack']
        
        # 标准化列名
        df.columns = [col.title() for col in df.columns]
        
        # 过滤无效数据
        df = df[(df["Quantity"] != 0) & (df["Sales"] != 0)]
        
        return df
    
    # ===== PSC 数据处理 =====
    
    def extract_year_month_from_sheetname(self, sheet_name):
        """从sheet名称提取年月"""
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
    
    def process_psc_file(self, file_path):
        """处理PSC文件（包含多个sheet）"""
        all_sheets = pd.read_excel(file_path, sheet_name=None, header=None)
        sheet_data = []
        
        for sheet_name, df in all_sheets.items():
            year, month = self.extract_year_month_from_sheetname(sheet_name)
            if year is None or month is None:
                print(f"⚠️ 跳过无效sheet: {sheet_name}")
                continue
            
            # 设置列名
            df.columns = df.iloc[0]
            df = df[1:]
            df = df.iloc[:-2] if df.shape[0] > 2 else df
            df = df.loc[:, ~df.columns.astype(str).str.contains('^Unnamed', na=False)]
            
            # 识别并重命名客户列
            col_names = df.columns.tolist()
            customer_col = None
            for i, col in enumerate(col_names):
                col_str = str(col)
                if re.search(r'\d{2}/\d{2}/\d{4}', col_str) and 'customer' in col_str.lower():
                    customer_col = col
                    break
                elif 'customer' in col_str.lower():
                    customer_col = col
                    break
            
            if customer_col is None and len(col_names) > 1:
                customer_col = col_names[1]
            
            if customer_col is not None:
                df.rename(columns={customer_col: "Customer"}, inplace=True)
            
            # 清理Customer列
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
            
            # 添加元数据
            df['Sales Channel Category'] = "Distributor"
            df['Sales Channel Name'] = 'PSC'
            df["Year"] = year
            df["Month"] = month
            df["Date"] = pd.to_datetime(dict(year=df["Year"], month=df["Month"], day=1))
            
            # 重命名标准列
            rename_dict = {
                "SKU#": "Sku", "QTY": "Quantity", "PROV": "Province",
                "SKU DESCRIPTION": "Sku Description", "SALES": "Sales", "Customer": "Account Name"
            }
            df.rename(columns={col: rename_dict.get(col, col) for col in df.columns}, inplace=True)
            
            # 删除不需要的列
            cols_to_drop = ["BROKER", "CODE", "BRAND", "UPC"]
            df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True, errors='ignore')
            
            df = df[(df["Quantity"] != 0) & (df["Sales"] != 0)]
            
            # 添加客户类别
            def get_customer_type(name):
                name = str(name).lower()
                if any(k in name for k in ['restaurant', 'bar', 'cafe']):
                    return 'Restaurant/Bar'
                elif any(k in name for k in ['grocery', 'market', 'grocer']):
                    return 'Grocery'
                elif 'liquor' in name or 'store' in name:
                    return 'Retail Store'
                else:
                    return 'Other'
            
            df['Account Category'] = df['Account Name'].apply(get_customer_type)
            
            # 添加产品线
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
            
            # 提取包装信息
            df[['Packs Per Case', 'Cans Per Pack']] = df['Sku Description'].str.extract(r'(\d+)/(\d+)x', expand=True).astype(float)
            df['Total Cans'] = df['Quantity'].astype(float) * df['Packs Per Case'] * df['Cans Per Pack']
            
            # 标准化列名
            df.columns = [str(col).strip().title() for col in df.columns]
            
            sheet_data.append(df)
        
        if sheet_data:
            return pd.concat(sheet_data, ignore_index=True)
        return pd.DataFrame()
    
    # ===== 主处理函数 =====
    
    def process_distributor_files(self, folder_path, distributor_type, force_reprocess=False):
        """处理特定分销商的文件，支持增量更新"""
        folder_path = Path(folder_path)
        new_data = []
        processed_count = 0
        skipped_count = 0
        
        # 获取文件夹中的所有相关文件
        if distributor_type == 'horizon':
            files = [f for f in folder_path.glob('*.xls*') if 'account list' not in f.name.lower()]
        elif distributor_type == 'psc':
            files = [f for f in folder_path.glob('*.xls*')]
        elif distributor_type == 'ollie':
            files = [f for f in folder_path.glob('*.csv')]
        else:
            files = list(folder_path.glob('*.csv')) + list(folder_path.glob('*.xls*'))
        
        for file_path in files:
            file_hash = self.get_file_hash(file_path)
            
            # 检查是否需要处理这个文件
            if not force_reprocess and self.is_file_processed(file_path, file_hash):
                skipped_count += 1
                print(f"⏭️ 跳过已处理文件: {file_path.name}")
                continue
            
            try:
                # 处理文件
                if distributor_type == 'horizon':
                    df = self.process_horizon_file(file_path)
                elif distributor_type == 'psc':
                    df = self.process_psc_file(file_path)
                elif distributor_type == 'ollie':
                    df_raw = pd.read_csv(file_path)
                    df = self.clean_ollie_data(df_raw)
                else:
                    # 通用CSV处理
                    df = pd.read_csv(file_path) if file_path.suffix == '.csv' else pd.read_excel(file_path)
                
                if not df.empty:
                    new_data.append(df)
                    processed_count += 1
                    
                    # 更新已处理文件日志
                    self.processed_files[str(file_path)] = {
                        'hash': file_hash,
                        'processed_date': datetime.now().isoformat(),
                        'rows': len(df),
                        'distributor': distributor_type
                    }
                    
                    print(f"✅ 处理完成: {file_path.name} ({len(df)} 行)")
                
            except Exception as e:
                print(f"❌ 处理失败: {file_path.name}, 错误: {e}")
        
        # 保存处理日志
        self.save_processed_files_log()
        
        print(f"\n📊 {distributor_type.upper()} 处理完成:")
        print(f"   - 新处理: {processed_count} 个文件")
        print(f"   - 跳过: {skipped_count} 个文件")
        
        # 合并新数据
        if new_data:
            return pd.concat(new_data, ignore_index=True)
        return pd.DataFrame()
    
    def update_master_data(self, new_data_dict, force_full_rebuild=False, include_shopify=True):
        """更新主数据表"""
        if force_full_rebuild:
            # 强制重建：清除缓存，重新处理所有文件
            print("🔄 强制重建主数据表...")
            self.processed_files = {}
            self.save_processed_files_log()
            if self.master_data_cache.exists():
                self.master_data_cache.unlink()
            if self.shopify_cache.exists():
                self.shopify_cache.unlink()
        
        # 加载现有主数据
        master_df = self.load_master_data()
        
        # 收集所有新数据
        all_new_data = []
        
        # 处理分销商数据
        for distributor, folder_path in new_data_dict.items():
            if folder_path and os.path.exists(folder_path):
                new_df = self.process_distributor_files(
                    folder_path, 
                    distributor, 
                    force_reprocess=force_full_rebuild
                )
                if not new_df.empty:
                    all_new_data.append(new_df)
        
        # 处理Shopify数据
        if include_shopify and self.TOKEN:
            try:
                shopify_df = self.fetch_shopify_orders(incremental=not force_full_rebuild)
                if not shopify_df.empty:
                    all_new_data.append(shopify_df)
                    print(f"✅ Shopify数据处理完成: {len(shopify_df)} 条记录")
            except Exception as e:
                print(f"⚠️ Shopify数据处理失败: {e}")
        
        # 如果有新数据，更新主表
        if all_new_data:
            new_combined = pd.concat(all_new_data, ignore_index=True)
            
            if master_df.empty or force_full_rebuild:
                # 第一次处理或强制重建
                master_df = new_combined
            else:
                # 增量更新：添加新数据
                master_df = pd.concat([master_df, new_combined], ignore_index=True)
                
                # 去重（基于关键字段）
                key_columns = ['Date', 'Account Name', 'Sku', 'Sales Channel Name']
                key_columns = [col for col in key_columns if col in master_df.columns]
                if key_columns:
                    master_df = master_df.drop_duplicates(subset=key_columns, keep='last')
            
            # 保存更新后的主数据
            self.save_master_data(master_df)
            print(f"\n✅ 主数据表更新完成，总记录数: {len(master_df)}")
        else:
            print("\n📝 没有新数据需要处理")
        
        return master_df
    
    def get_processing_summary(self):
        """获取处理摘要"""
        summary = {
            'total_files_processed': len(self.processed_files),
            'files_by_distributor': {},
            'last_update': None,
            'shopify_last_sync': None
        }
        
        for file_info in self.processed_files.values():
            dist = file_info.get('distributor', 'unknown')
            summary['files_by_distributor'][dist] = summary['files_by_distributor'].get(dist, 0) + 1
            
            processed_date = file_info.get('processed_date')
            if processed_date and (summary['last_update'] is None or processed_date > summary['last_update']):
                summary['last_update'] = processed_date
        
        # 添加Shopify同步信息
        if self.shopify_cache.exists():
            summary['shopify_last_sync'] = self.get_shopify_last_sync().isoformat()
        
        return summary
    
    def generate_account_status(self, df):
        """生成账户状态报告"""
        df['Account'] = df['Account Name'].str.lower()
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Sort by Date and get latest record for each account
        sorted_df = df.sort_values(by='Date', ascending=False)
        latest_records_df = sorted_df.drop_duplicates(subset='Account', keep='first')
        
        # Calculate days since last order
        today = pd.to_datetime(datetime.today().date())
        latest_records_df['Days Since Last Order'] = (today - latest_records_df['Date']).dt.days
        
        # Classify account status
        def classify_status(days):
            if days <= 90:
                return 'Active'
            elif 91 <= days <= 150:
                return 'Check-In Needed'
            elif days >= 180:
                return 'Non-Active'
            else:
                return 'Unknown'
        
        latest_records_df['Account Status'] = latest_records_df['Days Since Last Order'].apply(classify_status)
        
        # Select output columns
        output_df = latest_records_df[['Account', 'Date', 'Days Since Last Order', 'Account Status']]
        output_df.rename(columns={'Date': 'Last Order Date'}, inplace=True)
        
        return output_df


# 便捷函数
def process_all_distributor_data(data_folders, force_rebuild=False, include_shopify=True):
    """处理所有分销商数据的便捷函数"""
    processor = DataProcessor()
    
    # 定义数据源
    data_sources = {
        'horizon': data_folders.get('horizon'),
        'psc': data_folders.get('psc'),
        'ollie': data_folders.get('ollie')
    }
    
    # 更新主数据
    master_df = processor.update_master_data(
        data_sources, 
        force_full_rebuild=force_rebuild,
        include_shopify=include_shopify
    )
    
    # 获取处理摘要
    summary = processor.get_processing_summary()
    
    # 生成账户状态
    account_status_df = processor.generate_account_status(master_df)
    
    return master_df, account_status_df, summary