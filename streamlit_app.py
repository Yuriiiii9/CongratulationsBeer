import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from langchain_groq import ChatGroq
from langchain_experimental.agents.agent_toolkits.pandas.base import create_pandas_dataframe_agent
try:
    # 优先使用优化版本
    from data_processing_optimized import (
        DataProcessor,
        process_all_distributor_data
    )
    # 仍然需要一些基础函数
    from data_processing import (
        authenticate_google_drive,
        upload_to_drive,
        process_all_data  # 用于处理单独上传的文件
    )
    optimized_available = True
except ImportError:
    # 回退到原始版本
    from data_processing import (
        process_all_data, 
        generate_account_status,
        authenticate_google_drive,
        upload_to_drive
    )
    optimized_available = False
    print("Warning: Optimized processing not available")

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
</style>
""", unsafe_allow_html=True)

# --- Password protection ---
password = st.text_input("Enter access password:", type="password")
PASSWORD = os.environ.get("APP_PASSWORD", "")

if password != PASSWORD:
    st.warning("Incorrect or missing password. Please contact admin for access.")
    st.stop()

# --- Title and intro ---
st.title("🍺 Nonny Beer Data Portal")
st.markdown("Welcome to the Nonny Beer data platform. Upload sales data, view analytics, and manage your business insights.")

# --- Sidebar for file upload and controls ---
with st.sidebar:
    st.header("📁 Data Management")
    
    # Processing mode selection
    st.subheader("🔧 Processing Mode")
    processing_mode = st.radio(
        "Choose processing mode:",
        ["Incremental Update", "Full Rebuild"],
        help="Incremental: Only process new/changed files\nFull Rebuild: Reprocess all files"
    )
    
    # File upload for one-time files
    st.subheader("📤 Upload Files")
    uploaded_files = st.file_uploader(
        "Upload CSV/Excel files",
        type=['csv', 'xlsx', 'xls'],
        accept_multiple_files=True,
        help="Upload Ollie, Horizon, PSC, or other distributor reports"
    )
    
    # Folder paths for batch processing
    st.subheader("📂 Batch Processing")
    with st.expander("Configure folder paths"):
        horizon_folder = st.text_input("Horizon folder path:", value="/content/drive/MyDrive/Distributor Sales Reports/Horizon")
        psc_folder = st.text_input("PSC folder path:", value="/content/drive/MyDrive/Distributor Sales Reports/PSC Sales Reports")
        ollie_folder = st.text_input("Ollie folder path:", value="/content/drive/MyDrive/Distributor Sales Reports/Ollie")
    
    # Process button
    col1, col2 = st.columns(2)
    with col1:
        process_data = st.button("🔄 Process Data", type="primary", use_container_width=True)
    with col2:
        clear_cache = st.button("🗑️ Clear Cache", type="secondary", use_container_width=True)
    
    # Settings
    st.subheader("⚙️ Settings")
    save_to_drive = st.checkbox("Save results to Google Drive", value=True)
    show_processing_log = st.checkbox("Show processing details", value=False)
    
    # API Keys status
    st.subheader("🔑 API Status")
    shopify_status = "✅ Connected" if os.environ.get("SHOPIFY_TOKEN") else "❌ Not configured"
    st.info(f"Shopify API: {shopify_status}")

# Initialize session state
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'account_status' not in st.session_state:
    st.session_state.account_status = None

# Main content area with tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Dashboard", "📈 Analytics", "🤖 AI Assistant", "📋 Data Preview", "🔗 Resources"])

# Import the optimized processor
try:
    from data_processing_optimized import DataProcessor, process_all_distributor_data
    optimized_processing_available = True
except ImportError:
    optimized_processing_available = False
    st.warning("Optimized processing not available. Using standard processing.")

# Clear cache if requested
if clear_cache and optimized_available:
    processor = DataProcessor()
    if processor.cache_dir.exists():
        import shutil
        shutil.rmtree(processor.cache_dir)
        st.success("✅ Cache cleared successfully!")
        st.rerun()

# Process data when button is clicked
if process_data:
    with st.spinner("Processing data... This may take a few minutes."):
        try:
            combined_df = pd.DataFrame()
            processing_log = []
            
            # Use optimized processing if available and batch folders are configured
            if optimized_available and any([horizon_folder, psc_folder, ollie_folder]):
                # Batch processing from folders
                data_folders = {
                    'horizon': horizon_folder if horizon_folder else None,
                    'psc': psc_folder if psc_folder else None,
                    'ollie': ollie_folder if ollie_folder else None
                }
                
                force_rebuild = (processing_mode == "Full Rebuild")
                
                # Process with optimized method
                combined_df, account_status_df, summary = process_all_distributor_data(
                    data_folders, 
                    force_rebuild=force_rebuild,
                    include_shopify=True
                )
                
                st.session_state.processed_data = combined_df
                st.session_state.account_status = account_status_df
                
                # Show processing summary
                if show_processing_log:
                    st.info(f"📊 Processing Summary:")
                    st.json(summary)
            
            # Process uploaded files separately
            if uploaded_files:
                uploaded_df = process_all_data(uploaded_files)
                if not uploaded_df.empty:
                    if combined_df.empty:
                        combined_df = uploaded_df
                    else:
                        combined_df = pd.concat([combined_df, uploaded_df], ignore_index=True)
            
            # Add Shopify data if available (only if not already included by optimized processing)
            if os.environ.get("SHOPIFY_TOKEN") and not (optimized_available and any([horizon_folder, psc_folder, ollie_folder])):
                try:
                    from data_processing import fetch_shopify_orders
                    shopify_df = fetch_shopify_orders()
                    if not shopify_df.empty:
                        combined_df = pd.concat([combined_df, shopify_df], ignore_index=True)
                except Exception as e:
                    st.warning(f"Could not fetch Shopify data: {e}")
            
            if not combined_df.empty:
                # Only generate account status if not already done by optimized processing
                if 'account_status_df' not in locals():
                    if optimized_available:
                        processor = DataProcessor()
                        account_status_df = processor.generate_account_status(combined_df)
                    else:
                        account_status_df = generate_account_status(combined_df)
                    st.session_state.account_status = account_status_df
                
                st.session_state.processed_data = combined_df
                
                # Save to local and optionally to Google Drive
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                # Save combined data
                combined_filename = f'combined_sales_data_{timestamp}.csv'
                combined_df.to_csv(combined_filename, index=False)
                
                # Save account status
                status_filename = f'account_status_{timestamp}.csv'
                account_status_df.to_csv(status_filename, index=False)
                
                if save_to_drive:
                    try:
                        service = authenticate_google_drive()
                        
                        # Upload files to Google Drive
                        combined_file_id = upload_to_drive(service, combined_filename, combined_filename)
                        status_file_id = upload_to_drive(service, status_filename, status_filename)
                        
                        st.success(f"✅ Data processed and saved to Google Drive!")
                        
                        # Show detailed results
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Total Records", f"{len(combined_df):,}")
                            st.metric("Unique Accounts", f"{combined_df['Account Name'].nunique():,}")
                        with col2:
                            st.metric("Processing Mode", processing_mode)
                            st.metric("Files Saved", "2 files to Google Drive")
                        
                        # Clean up local files
                        os.remove(combined_filename)
                        os.remove(status_filename)
                    except Exception as e:
                        st.warning(f"Could not save to Google Drive: {e}. Files saved locally instead.")
                else:
                    st.success(f"✅ Data processed! Files saved locally.")
                    st.info(f"Combined data: {combined_filename}")
                    st.info(f"Account status: {status_filename}")
            else:
                st.warning("No data was processed. Please check your files and try again.")
                
        except Exception as e:
            st.error(f"Error processing data: {e}")
            if show_processing_log:
                import traceback
                st.code(traceback.format_exc())

# Load existing data if available
if st.session_state.processed_data is None:
    csv_url = os.environ.get("PRIVATE_CSV_URL", "")
    if csv_url:
        try:
            st.session_state.processed_data = pd.read_csv(csv_url)
        except:
            st.info("No existing data found. Please upload and process new files.")

# Tab 1: Dashboard
with tab1:
    st.header("📊 Business Dashboard")
    
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
            avg_order_value = total_sales / total_orders if total_orders > 0 else 0
            st.metric("Avg Order Value", f"${avg_order_value:.2f}")
        
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
                st.pyplot(fig)
        
        # Power BI placeholder
        st.info("📊 Power BI Dashboard will be embedded here once the link is available.")
        
    else:
        st.info("No data available. Please upload and process files using the sidebar.")

# Tab 2: Analytics
with tab2:
    st.header("📈 Sales Analytics")
    
    if st.session_state.processed_data is not None:
        df = st.session_state.processed_data
        
        # Time series analysis
        st.subheader("Sales Over Time")
        
        # Convert Date to datetime if not already
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Group by month
        monthly_sales = df.groupby(df['Date'].dt.to_period('M'))['Sales'].sum()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        monthly_sales.plot(kind='line', ax=ax, marker='o')
        ax.set_xlabel('Month')
        ax.set_ylabel('Sales ($)')
        ax.set_title('Monthly Sales Trend')
        plt.xticks(rotation=45)
        st.pyplot(fig)
        
        # Product analysis
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Top Products by Sales")
            top_products = df.groupby('Product Line')['Sales'].sum().sort_values(ascending=False).head(10)
            
            fig, ax = plt.subplots(figsize=(8, 6))
            top_products.plot(kind='bar', ax=ax)
            ax.set_xlabel('Product Line')
            ax.set_ylabel('Sales ($)')
            plt.xticks(rotation=45)
            st.pyplot(fig)
        
        with col2:
            st.subheader("Sales by Channel")
            channel_sales = df.groupby('Sales Channel Name')['Sales'].sum().sort_values(ascending=False)
            
            fig, ax = plt.subplots(figsize=(8, 6))
            channel_sales.plot(kind='bar', ax=ax)
            ax.set_xlabel('Sales Channel')
            ax.set_ylabel('Sales ($)')
            plt.xticks(rotation=45)
            st.pyplot(fig)
        
        # Geographic analysis
        st.subheader("Sales by Province")
        province_sales = df.groupby('Province')['Sales'].sum().sort_values(ascending=False)
        
        fig, ax = plt.subplots(figsize=(12, 6))
        province_sales.plot(kind='bar', ax=ax)
        ax.set_xlabel('Province')
        ax.set_ylabel('Sales ($)')
        plt.xticks(rotation=45)
        st.pyplot(fig)
        
    else:
        st.info("No data available for analytics. Please upload and process files first.")

# Tab 3: AI Assistant
with tab3:
    st.header("🤖 AI-Powered Data Assistant")
    
    if st.session_state.processed_data is not None:
        df = st.session_state.processed_data
        
        # Initialize AI agent
        AI_API_KEY = os.environ.get("GROQ_API_KEY", "")
        
        if AI_API_KEY:
            @st.cache_resource
            def load_agent(df_hash):
                return create_pandas_dataframe_agent(
                    ChatGroq(
                        temperature=0,
                        model_name="llama3-8b-8192",
                        groq_api_key=AI_API_KEY,
                    ),
                    df,
                    verbose=False,
                    handle_parsing_errors=True,
                    allow_dangerous_code=True
                )
            
            # Create a hash of the dataframe for caching
            df_hash = pd.util.hash_pandas_object(df).sum()
            agent = load_agent(df_hash)
            
            # Example questions
            st.info("💡 Try asking questions like:")
            example_questions = [
                "Which product line has the highest sales?",
                "What's the average order value by sales channel?",
                "Show me the top 10 accounts by total sales",
                "Which province has the most orders?",
                "What's the sales trend over the last 6 months?"
            ]
            
            for q in example_questions:
                st.caption(f"• {q}")
            
            # User input
            user_query = st.text_input("Ask a question about your sales data:")
            
            if user_query:
                with st.spinner("AI is analyzing your data..."):
                    try:
                        column_list_str = ", ".join(df.columns)
                        context = f"The dataset contains the following columns: {column_list_str}.\n"
                        full_prompt = context + user_query
                        
                        response = agent.invoke(full_prompt)
                        st.write(response.get("output", "No output returned."))
                    except Exception as e:
                        st.error(f"Error: {e}")
        else:
            st.warning("AI Assistant requires GROQ_API_KEY to be configured.")
    else:
        st.info("No data available. Please upload and process files first.")

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
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Filtered Records", f"{len(filtered_df):,}")
        
        with col2:
            st.metric("Total Sales", f"${filtered_df['Sales'].sum():,.2f}")
        
        with col3:
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
        st.info("No data available. Please upload and process files first.")

# Tab 5: Resources
with tab5:
    st.header("🔗 Project Resources")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📚 Documentation")
        st.markdown("""
        - [GitHub Repository](https://github.com/your_username/nonnybeer-dashboard)
        - [User Guide](https://github.com/your_username/nonnybeer-dashboard/wiki)
        - [API Documentation](https://github.com/your_username/nonnybeer-dashboard/wiki/API)
        """)
    
    with col2:
        st.subheader("🛠️ Tools & Support")
        st.markdown("""
        - [Report Issues](https://github.com/your_username/nonnybeer-dashboard/issues)
        - [Request Features](https://github.com/your_username/nonnybeer-dashboard/issues/new)
        - Contact: support@nonnybeer.com
        """)
    
    st.subheader("📊 Data Sources")
    st.info("""
    This dashboard integrates data from:
    - Shopify (via API)
    - Ollie distributor reports
    - Horizon distributor reports
    - PSC distributor reports
    - Other CSV/Excel uploads
    """)
