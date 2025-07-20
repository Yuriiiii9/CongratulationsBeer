# streamlit_app.py
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from pathlib import Path
import json

# Import data processing functions
from data_processing import (
    process_all_data,
    generate_account_status,
    authenticate_google_drive,
    upload_to_drive,
    fetch_shopify_orders,
    clean_horizon_data,
    clean_ollie_data,
    merge_psc_sheets,
    load_clean_horizon_from_drive,
    load_clean_psc_from_drive,
    load_clean_ollie_from_drive
)

# Only import GROQ if available
try:
    from langchain_groq import ChatGroq
    from langchain_experimental.agents.agent_toolkits.pandas.base import create_pandas_dataframe_agent
    groq_available = True
except ImportError:
    groq_available = False
    st.warning("GROQ not available. AI Assistant will be disabled.")

# Page setup
st.set_page_config(page_title="Nonny Beer Dashboard", layout="wide", initial_sidebar_state="expanded")

# Custom CSS
st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 16px;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .success-message {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .warning-message {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #fff3cd;
        border: 1px solid #ffeeba;
        color: #856404;
    }
</style>
""", unsafe_allow_html=True)

# Password protection
password = st.text_input("Enter access password:", type="password")
PASSWORD = os.environ.get("APP_PASSWORD", "default_password")

if password != PASSWORD:
    st.warning("Incorrect or missing password. Please contact admin for access.")
    st.stop()

# Title and intro
st.title("🍺 Nonny Beer Data Portal")
st.markdown("Welcome to the Nonny Beer data platform. Upload sales data, view analytics, and manage your business insights.")

# Initialize session state
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'account_status' not in st.session_state:
    st.session_state.account_status = None
if 'processing_history' not in st.session_state:
    st.session_state.processing_history = []
if 'last_processed_files' not in st.session_state:
    st.session_state.last_processed_files = set()

# Sidebar for file upload and controls
with st.sidebar:
    st.header("📁 Data Management")
    
    # One-Click Processing
    st.subheader("🚀 Quick Actions")
    if st.button("🔄 One-Click Process All Data", type="primary", use_container_width=True):
        with st.spinner("Processing all data from Google Drive..."):
            try:
                all_data = []
                processed_files = []
                
                # Define folder paths from environment variables
                folders = {
                    'Horizon': os.environ.get('HORIZON_FOLDER_PATH', '/tmp/distributor_data/horizon'),
                    'PSC': os.environ.get('PSC_FOLDER_PATH', '/tmp/distributor_data/psc'),
                    'Ollie': os.environ.get('OLLIE_FOLDER_PATH', '/tmp/distributor_data/ollie')
                }
                
                # Process each distributor folder
                for dist_name, folder_id in folders.items():
                    try:
                        if dist_name == 'Horizon':
                            df = load_clean_horizon_from_drive(folder_id)
                        elif dist_name == 'PSC':
                            df = load_clean_psc_from_drive(folder_id)
                        elif dist_name == 'Ollie':
                            df = load_clean_ollie_from_drive(folder_id)
                        else:
                            continue
                
                        if not df.empty:
                            all_data.append(df)
                            processed_files.append(f"{dist_name}: {df.shape[0]} rows")
                    except Exception as e:
                        st.warning(f"Error processing {dist_name}: {e}")
                        
                
                # Add Shopify data
                shopify_df = fetch_shopify_orders()
                if not shopify_df.empty:
                    all_data.append(shopify_df)
                    processed_files.append("Shopify: Live API Data")
                else:
                    st.warning("Shopify data could not be fetched. Check API token.")
                
                # Combine all data
                if all_data:
                    combined_df = pd.concat(all_data, ignore_index=True)

                    if 'File Name' in combined_df.columns:
                        combined_df.drop(columns=['File Name'], inplace=True)
                        
                    st.session_state.processed_data = combined_df
                    st.session_state.account_status = generate_account_status(combined_df)
                    
                    # Save to Drive
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    service = authenticate_google_drive()

                    UPLOAD_TARGET_FOLDER_ID = os.environ.get("UPLOAD_FOLDER_ID")
                    
                    if service:
                        # Save combined data
                        combined_filename = f'combined_sales_data_{timestamp}.csv'
                        combined_df.to_csv(combined_filename, index=False)
                        upload_to_drive(service, combined_filename, combined_filename, folder_id=UPLOAD_TARGET_FOLDER_ID)
                        os.remove(combined_filename)
                        
                        # Save account status
                        status_filename = f'account_status_{timestamp}.csv'
                        st.session_state.account_status.to_csv(status_filename, index=False)
                        upload_to_drive(service, status_filename, status_filename, folder_id=UPLOAD_TARGET_FOLDER_ID)
                        os.remove(status_filename)
                    
                    # Update processing history
                    st.session_state.processing_history.append({
                        'timestamp': timestamp,
                        'files_processed': processed_files,
                        'total_records': len(combined_df)
                    })
                    
                    st.success(f"✅ Processed {len(processed_files)} files with {len(combined_df):,} total records!")
                    
                    # Show what was processed
                    with st.expander("📋 Processing Details"):
                        for file in processed_files:
                            st.write(f"• {file}")
                else:
                    st.error("No data could be processed. Check your data sources and API tokens.")
                    
            except Exception as e:
                st.error(f"Error during processing: {e}")
                import traceback
                st.code(traceback.format_exc())
    
    # Manual file upload
    st.subheader("📤 Manual Upload")
    uploaded_files = st.file_uploader(
        "Upload CSV/Excel files",
        type=['csv', 'xlsx', 'xls'],
        accept_multiple_files=True,
        help="Upload Ollie, Horizon, PSC, or other distributor reports"
    )
    
    if uploaded_files:
        if st.button("Process Uploaded Files", type="secondary"):
            with st.spinner("Processing uploaded files..."):
                df = process_all_data(uploaded_files)
                if not df.empty:
                    st.session_state.processed_data = df
                    st.session_state.account_status = generate_account_status(df)
                    st.success(f"✅ Processed {len(uploaded_files)} files!")
    
    # Settings
    st.subheader("⚙️ Settings")
    clear_history = st.button("🗑️ Clear Processing History", type="secondary")
    if clear_history:
        st.session_state.last_processed_files = set()
        st.session_state.processing_history = []
        st.success("Processing history cleared!")
    
    # API Keys status
    st.subheader("🔑 API Status")
    shopify_status = "✅ Connected" if os.environ.get("SHOPIFY_TOKEN") else "❌ Not configured"
    drive_status = "✅ Available" if 'GOOGLE_CREDENTIALS' in os.environ else "❌ Not configured"
    st.info(f"Shopify API: {shopify_status}")
    st.info(f"Google Drive: {drive_status}")

# Main content area with tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard", "📈 Analytics", "🤖 AI Assistant", "📋 Data Preview", "🔗 Resources"])

# Tab 1: Dashboard
with tab1:
    st.header("📊 Business Dashboard")

    # Embed Power BI iframe at the top
    powerbi_iframe = """
    <iframe
        title="Nonny Beer Sales Dashboard"
        width="100%" 
        height="900"
        src="https://app.powerbi.com/reportEmbed?reportId=807519b3-5141-474e-bbec-50404312fabf&autoAuth=true&ctid=92315d43-67d5-4613-9f3d-c3fb5114bf50" 
        frameborder="0"
        allowFullScreen="true">
    </iframe>
    """
    st.components.v1.html(powerbi_iframe, height=620)
    
    if st.session_state.processed_data is not None:
        df = st.session_state.processed_data
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_sales = df['Sales'].sum()
            st.metric("Total Sales", f"${total_sales:,.2f}")
        
        with col2:
            total_orders = len(df)
            st.metric("Total Orders", f"{total_orders:,}")
        
        with col3:
            unique_accounts = df['Account Name'].nunique()
            st.metric("Unique Accounts", f"{unique_accounts:,}")
        
        with col4:
            total_bottles = df['Total Bottles'].sum()
            st.metric("Total Bottles", f"{total_bottles:,.0f}")
        
        # Account Status Summary
        if st.session_state.account_status is not None:
            st.subheader("📈 Account Status Overview")
            status_df = st.session_state.account_status
            
            status_counts = status_df['Account Status'].value_counts()
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                for status, count in status_counts.items():
                    if status == 'Active':
                        st.success(f"{status}: {count}")
                    elif status == 'Check-In Needed':
                        st.warning(f"{status}: {count}")
                    elif status == 'Non-Active':
                        st.error(f"{status}: {count}")
            
            with col2:
                fig, ax = plt.subplots(figsize=(8, 6))
                colors = {'Active': '#28a745', 'Check-In Needed': '#ffc107', 'Non-Active': '#dc3545'}
                status_counts.plot(kind='pie', ax=ax, autopct='%1.1f%%', 
                                 colors=[colors.get(x, '#gray') for x in status_counts.index])
                ax.set_ylabel('')
                ax.set_title('Account Status Distribution')
                st.pyplot(fig)
        
        # Processing History
        if st.session_state.processing_history:
            st.subheader("📜 Recent Processing History")
            for entry in st.session_state.processing_history[-5:]:  # Show last 5
                with st.expander(f"Processed on {entry['timestamp']}"):
                    st.write(f"Total records: {entry['total_records']:,}")
                    st.write("Files processed:")
                    for file in entry['files_processed']:
                        st.write(f"• {file}")
        
    else:
        st.info("No data available. Click 'One-Click Process All Data' in the sidebar to get started!")

# Tab 2: Analytics
with tab2:
    st.header("📈 Sales Analytics")
    
    if st.session_state.processed_data is not None:
        df = st.session_state.processed_data
        
        # Time series analysis
        st.subheader("Sales Over Time")
        
        # Monthly sales
        monthly_sales = df.groupby(['Year', 'Month'])['Sales'].sum().reset_index()
        monthly_sales['Date'] = pd.to_datetime(monthly_sales[['Year', 'Month']].assign(day=1))
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(monthly_sales['Date'], monthly_sales['Sales'], marker='o', linewidth=2, markersize=8)
        ax.set_xlabel('Month')
        ax.set_ylabel('Sales ($)')
        ax.set_title('Monthly Sales Trend')
        ax.grid(True, alpha=0.3)
        
        # Format y-axis
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Rotate x-axis labels
        plt.xticks(rotation=45)
        
        st.pyplot(fig)
        
        # Product Performance
        st.subheader("Product Line Performance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            product_sales = df.groupby('Product Line')['Sales'].sum().sort_values(ascending=False)
            
            fig, ax = plt.subplots(figsize=(8, 6))
            product_sales.plot(kind='bar', ax=ax, color='skyblue')
            ax.set_xlabel('Product Line')
            ax.set_ylabel('Total Sales ($)')
            ax.set_title('Sales by Product Line')
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
            plt.xticks(rotation=45)
            st.pyplot(fig)
        
        with col2:
            product_bottles = df.groupby('Product Line')['Total Bottles'].sum().sort_values(ascending=False)
            
            fig, ax = plt.subplots(figsize=(8, 6))
            product_bottles.plot(kind='bar', ax=ax, color='lightcoral')
            ax.set_xlabel('Product Line')
            ax.set_ylabel('Total Bottles')
            ax.set_title('Bottles Sold by Product Line')
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
            plt.xticks(rotation=45)
            st.pyplot(fig)
        
        # Channel Analysis
        st.subheader("Sales Channel Analysis")
        
        channel_metrics = df.groupby('Sales Channel Name').agg({
            'Sales': 'sum',
            'Quantity': 'sum',
            'Account Name': 'nunique'
        }).rename(columns={'Account Name': 'Unique Accounts'})
        
        st.dataframe(channel_metrics.style.format({
            'Sales': '${:,.2f}',
            'Quantity': '{:,.0f}',
            'Unique Accounts': '{:,.0f}'
        }))
        
    else:
        st.info("No data available. Please process files first.")

# Tab 3: AI Assistant
with tab3:
    st.header("🤖 AI Sales Assistant")
    
    if st.session_state.processed_data is not None and groq_available:
        df = st.session_state.processed_data
        
        # Initialize GROQ
        groq_api_key = os.environ.get("GROQ_API_KEY", "")
        
        if groq_api_key:
            st.info("Ask questions about your sales data in natural language!")
            
            # Chat interface
            if 'messages' not in st.session_state:
                st.session_state.messages = []
            
            # Display chat history
            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.write(message["content"])
            
            # Chat input
            if prompt := st.chat_input("Ask about your sales data..."):
                # Add user message
                st.session_state.messages.append({"role": "user", "content": prompt})
                with st.chat_message("user"):
                    st.write(prompt)
                
                # Generate response
                with st.chat_message("assistant"):
                    with st.spinner("Thinking..."):
                        try:
                            # Check if DataFrame is empty
                            if df.empty:
                                st.error("No data available for analysis. Please process some data first.")
                            else:
                                # Initialize GROQ with error handling
                                llm = ChatGroq(
                                    groq_api_key=groq_api_key, 
                                    model="llama3-8b-8192",
                                    temperature=0.1
                                )
                                
                                # Simple approach: format data info and question
                                data_info = f"""
                                Data Overview:
                                - Total rows: {len(df)}
                                - Columns: {', '.join(df.columns)}
                                - Sales Channels: {', '.join(df['Sales Channel Name'].unique() if 'Sales Channel Name' in df else [])}
                                - Total Sales: ${df['Sales'].sum():,.2f} if 'Sales' in df else 'N/A'
                                
                                First 5 rows sample:
                                {df.head().to_string()}
                                """
                                
                                # Create a simpler prompt
                                full_prompt = f"""
                                You are analyzing sales data for Nonny Beer brewery. 
                                {data_info}
                                
                                Question: {prompt}
                                
                                Please provide a clear, concise answer based on the data provided.
                                """
                                
                                # Get response directly from LLM
                                response = llm.invoke(full_prompt)
                                response_text = response.content if hasattr(response, 'content') else str(response)
                                
                                st.write(response_text)
                                st.session_state.messages.append({"role": "assistant", "content": response_text})
                            
                        except Exception as e:
                            st.error(f"AI Assistant Error: {str(e)}")
                            st.info("Please try rephrasing your question or check if data is available.")
        else:
            st.warning("AI Assistant requires GROQ_API_KEY to be configured.")
    else:
        st.info("No data available or GROQ not installed. Please process files first.")

# Tab 4: Data Preview
with tab4:
    st.header("📋 Data Preview")
    
    if st.session_state.processed_data is not None:
        df = st.session_state.processed_data
        
        # Data filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            channels = ['All'] + list(df['Sales Channel Name'].unique())
            selected_channel = st.selectbox("Filter by Channel", channels)
        
        with col2:
            products = ['All'] + list(df['Product Line'].unique())
            selected_product = st.selectbox("Filter by Product", products)
        
        with col3:
            provinces = ['All'] + list(df['Province'].dropna().unique())
            selected_province = st.selectbox("Filter by Province", provinces)
        
        # Apply filters
        filtered_df = df.copy()
        
        if selected_channel != 'All':
            filtered_df = filtered_df[filtered_df['Sales Channel Name'] == selected_channel]
        
        if selected_product != 'All':
            filtered_df = filtered_df[filtered_df['Product Line'] == selected_product]
        
        if selected_province != 'All':
            filtered_df = filtered_df[filtered_df['Province'] == selected_province]
        
        # Display metrics for filtered data
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Filtered Records", f"{len(filtered_df):,}")
        
        with col2:
            st.metric("Total Sales", f"${filtered_df['Sales'].sum():,.2f}")
        
        with col3:
            st.metric("Total Bottles", f"{filtered_df['Total Bottles'].sum():,.0f}")
        
        with col4:
            st.metric("Unique Accounts", f"{filtered_df['Account Name'].nunique():,}")
        
        # Display data
        st.dataframe(filtered_df, use_container_width=True)
        
        # Download button
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download filtered data as CSV",
            data=csv,
            file_name=f"filtered_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    else:
        st.info("No data available. Please process files first.")

# Tab 5: Resources
with tab5:
    st.header("🔗 Project Resources")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📚 Documentation")
        st.markdown("""
        - [GitHub Repository](https://github.com/your_username/nonnybeer-dashboard)
        - [User Guide](https://github.com/your_username/nonnybeer-dashboard/wiki)
        - [Data Processing Guide](https://github.com/your_username/nonnybeer-dashboard/wiki/Data-Processing)
        """)
        
        st.subheader("📊 Data Format Requirements")
        st.markdown("""
        **Horizon Files:**
        - Columns: Ship Date, Customer, Product, Cases, Total
        - Format: Excel (.xlsx) or CSV
        
        **PSC Files:**
        - Columns: Date Sold, Customer, Item Description, Cases, Amount
        - Format: Excel with multiple sheets
        
        **Ollie Files:**
        - Columns: Date, Buyer, Variant Name, Quantity, Total
        - Format: CSV or Excel
        """)
    
    with col2:
        st.subheader("🛠️ Tools & Support")
        st.markdown("""
        - [Report Issues](https://github.com/your_username/nonnybeer-dashboard/issues)
        - [Request Features](https://github.com/your_username/nonnybeer-dashboard/issues/new)
        - Contact: support@nonnybeer.com
        """)
        
        st.subheader("🔧 Environment Variables")
        st.markdown("""
        Required environment variables:
        - `APP_PASSWORD`: Dashboard access password
        - `SHOPIFY_TOKEN`: Shopify API access token
        - `GROQ_API_KEY`: GROQ API key for AI features
        - `GOOGLE_CREDENTIALS`: Google Drive credentials (JSON)
        """)
    
    st.subheader("📈 Data Flow")
    st.info("""
    1. **One-Click Processing**: Automatically scans Google Drive folders for new files
    2. **Data Cleaning**: Each distributor's data is cleaned and standardized
    3. **Shopify Integration**: Live data is pulled from Shopify API
    4. **Data Merging**: All sources are combined into a unified dataset
    5. **Google Drive Save**: Processed data is saved back to Google Drive
    6. **Analytics & Insights**: View dashboards, analytics, and use AI assistant
    """)
