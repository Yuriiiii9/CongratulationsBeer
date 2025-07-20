# 🍺 Nonny Beer Sales Data Platform

This project delivers a fully automated pipeline to clean, merge, and visualize monthly distributor sales data for **Nonny Beer**. It integrates multiple data sources (HORIZON, PSC, OLLIE and Shopify), standardizes file formats, and outputs ready-to-use CSV files for business analysis.

The platform includes:
- A **Streamlit Web App** for file preview, dashboard viewing, and AI-driven Q&A
- A **Power BI Dashboard** for sales trends and customer activity
- **One-click data collection, cleaning, merging, and dashboard visualization plus analysis**

---

## 📋 Table of Contents
- [Description](#project-description)
- [Key Features](#key-features)
- [Repository Structure](#repository-structure)
- [Tech Assets & URLs](#tech-assets--urls)
- [Usage](#how-to-use-the-platform)
- [Data Sources](#data-sources)
- [Output Files](#output-files)
- [Smart Features](#smart-features)
- [Installation](#installation)
- [Prerequisites](#prerequisites)
- [Troubleshooting](#troubleshooting)
- [Costs Involved](#costs-involved)
- [Security & Credentials (Secure Delivery)](#security--credentials-secure-delivery)
- [Contact](#contact)
- [License](#license)

---

## 📄 Project Description

This project provides an end-to-end web-based solution for Nonny Beer to collect, clean, merge, and analyze sales data from multiple distributors.  
It enables real-time dashboard visualization, account activity tracking, and AI-powered querying — all accessible through a no-code, secure interface.


## 🚀 Key Features

- ✅ **Automated File Cleaning & Merging** across three channels
- ☁️ **Google Drive Upload**: versioned `.csv` exports auto-uploaded to shared folders
- 📊 **Embedded Power BI Dashboard** for visual insights
- 📈 **Analytics Transparency**: delivers auto-refreshed, structured insights that highlight account trends, product performance, and sales by channel — all without manual reporting.
- 🤖 **AI Assistant (Groq + LangChain)** to ask natural-language questions like  
  > *“Which accounts were inactive in the last quarter?”*
- 📁 **Download Traceability**: displays the last generated file name for each export, enabling easy version tracking

---

## 🗂️ Repository Structure

```
nonnybeer-handoff/
│
├── streamlit_app.py              # Main Streamlit app (UI + logic)
├── data_processing.py            # File parsing, cleaning, Drive upload
├── requirements.txt              # Python dependencies
├── secret.zip                    # Encrypted credentials (password shared privately with client)
│
├── assets/                       # App screenshots and visuals
│   └── powerbi_dashboard.png
│
├── outputs/                      # Sample output files
│   └── combined_sales_data_sample.csv
│
└── README.md                     # This file
```

## 🔗 Tech Assets & URLs
[https://nonny-beer-web.onrender.com/](https://nonny-beer-web.onrender.com/)

## 🌐 How to Use the Platform

Access the web app here:  
👉 **([https://nonny-beer-web.onrender.com/](https://nonny-beer-web.onrender.com/))**

### Inside the App:

### 🧭 Data Management Console

- One-click process to trigger full pipeline  
- Upload raw CSV or Excel files manually  
- Monitor Shopify & Google Drive API connection  
- View processed file count and record summaries  
- Clear history or reprocess with ease  

---

### 📊 Tab 1: Business Dashboard

- View total sales, orders, bottles, and unique accounts  
- Visualize account status by activity segment  
- Display recent file processing summary  
- Embedded Power BI dashboard for deep drill-down  
- Auto-refresh with each processing run  

---

### 📈 Tab 2: Analytics

- Plot monthly sales trends  
- Compare product lines by revenue and volume  
- Analyze channel-level performance  
- Understand product and customer dynamics  
- Identify patterns, gaps, and growth points  

---

### 🤖 Tab 3: AI Assistant

- Ask natural-language business questions  
- Retrieve insights across sales, products, and accounts  
- Powered by Groq + LangChain  
- No SQL or dashboard filters required  
- Available instantly after processing  

---

### 📁 Tab 4: Data Preview

- Filter data by channel, product, or province  
- Review cleaned and merged outputs  
- Download ready-to-use CSVs  
- Validate source formatting and quantity  
- Great for cross-checking or light analysis  

---

### 📚 Tab 5: Resource Center

- View documentation, user guides, and links  
- Check data format requirements for each source  
- Access required environment variables  
- Contact support or submit issues  
- Central hub for technical and business users  


---

## 📁 Output Files

Each run generates versioned `.csv` files:

| Filename | Description |
|----------|-------------|
| `combined_sales_data_<timestamp>.csv` | Cleaned and merged full dataset |
| `account_status_<timestamp>.csv`     | Customer activity status by last order date |

All files can be previewed, downloaded, and visualized in real time via the web dashboard.

---

## 🧠 Smart Features

- 🧠 One-click data collection, cleaning, merging, and visualization  
- 📂 Real-time preview and download with export traceability  
- ⚠️ Automatic format validation with instant error prompts  
- 📊 Embedded dashboards with interactive multi-dimensional filtering 
- 🔐 Credentials and API keys are managed securely in deployment

---

## 🛠️ Installation

**No installation is required for end users.**
The platform is fully web-based. Simply visit the link below and log in:

👉 [https://nonny-beer-web.onrender.com/](https://nonny-beer-web.onrender.com/)

For developers or technical team members who wish to run the app locally, see below.

### Local Setup (Optional – Developer Use Only)

1. Clone the repository:

   ```bash
   git clone https://github.com/Yuriiiii9/CongratulationsBeer.git
   cd CongratulationsBeer
   ```
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```
3. Run the Streamlit app:

   ```bash
   streamlit run streamlit_app.py
   ```

---

## 📋 Prerequisites

> ⚠️ **This section applies only if you are running the app locally.**
> Most users **do not need to install anything** to access the platform via the website.

* **Python 3.8+** for local development
* Required libraries in `requirements.txt` (e.g., Streamlit, pandas)
* Active **Google Service Account credentials** (included in `secret.zip`)

  * If access to shared folders is removed, data fetching will stop
  * Contact the project team to update service account settings
* API keys for:

  * **Shopify** (order data)
  * **Groq** (AI assistant)
    These are already pre-configured for production use

---

## 🧯 Troubleshooting

* **Format-related errors:**
  Most code-level issues (e.g., column mismatch, sheet name errors) will display a clear error message. These usually result from **new data sources that differ from expected templates**. Please contact the team for format alignment.

* **Render page errors (502 / 504 / 501):**
  These are transient and usually indicate the server is re-deploying. Wait 30–60 seconds and refresh the page.

* **Black screen or spinning Render logo:**
  This means the web app is still booting. Please wait — no action needed.

* **Drive access issues:**
  If the shared Google Drive folders are not visible or files do not load, ensure that the **current service account still has access**.

---

## 💸 Costs Involved

* **Power BI Pro:**
  The embedded dashboard uses a **Power BI Pro trial**. If the trial expires, some visual elements (e.g., Map visuals) may become unavailable.
  However, this **does not affect core data access or platform functionality**.

* We recommend the client obtain a **Power BI Pro license** for long-term access and seamless dashboard performance.

---
### 🔐 Security & Credentials (Secure Delivery)

- This repository includes an encrypted file: `secret.zip`
- It contains necessary credentials such as:
  - Google Service Account JSON
  - API keys (e.g., Groq)
- **The password is NOT shared in this repo.**  
  It will be delivered **securely and privately** to the client (e.g., via email, Zoom, or in-person handoff).
- Do not attempt to open or extract the file without authorization.

⚠️ Reminder: Do not upload or share the contents of `secret.zip` in any public or private GitHub repository.

---

## 🙋 Contact

**Author**: Mina Ai, Yuri Xu, Carol Wang
📧 Email: mina.ai@mail.mcgill.ca, xinting.xu@mail.mcgill.ca, yuran.wang3@mail.mcgill.ca

---

## 📄 License

MIT License — for academic demonstration use. Client-facing license terms to be discussed separately.
```
