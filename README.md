# 🍺 Nonny Beer Sales Data Platform

This project delivers a fully automated pipeline to clean, merge, and visualize monthly distributor sales data for **Nonny Beer**. It integrates multiple data sources (HORIZON, PSC, OLLIE and Shopify), standardizes file formats, and outputs ready-to-use CSV files for business analysis.

The platform includes:
- A **Streamlit Web App** for file preview, dashboard viewing, and AI-driven Q&A
- A **Power BI Dashboard** for sales trends and customer activity
- **Auto-upload to Google Drive** with versioned outputs

---

## 📋 Table of Contents
- [Description](#description)
- [Tech Assets & URLs](#tech-assets--urls)
- [Installation](#installation)
- [Usage](#usage)
- [Features](#features)
- [Data Sources](#data-sources)
- [Output Files](#output-files)
- [Credentials](#credentials)
- [Troubleshooting](#troubleshooting)
- [Costs Involved](#costs-involved)
- [Contact](#contact)
- [License](#license)

---

## 🚀 Key Features

- ✅ **Automated File Cleaning & Merging** across three channels
- ☁️ **Google Drive Upload**: versioned `.csv` exports auto-uploaded to shared folders
- 📊 **Embedded Power BI Dashboard** for visual insights
- 🤖 **AI Assistant (Groq + LangChain)** to ask natural-language questions like  
  > *“Which accounts were inactive in the last quarter?”*
- 📁 **Duplicate & Error Handling**: skips already-processed or failed files with feedback

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

## 🌐 How to Use the Platform

Access the web app here:  
👉 **([https://nonny-beer-web.onrender.com/](https://nonny-beer-web.onrender.com/))**

### Inside the App:

- **Tab 1: Preview**
  - Filter by channel, product, province
  - Download processed CSV files
- **Tab 2: Dashboard**
  - Explore sales trends, customer activity, top products
  - Powered by Power BI (embedded)
- **Tab 3: AI Assistant**
  - Ask questions like:  
    > “Which PSC accounts had no orders in June?”  
    > “Top 5 products by revenue in the last 3 months”

---

## 📁 Output Files (Auto-Uploaded to Google Drive)

Each run generates versioned `.csv` files:

| Filename | Description |
|----------|-------------|
| `combined_sales_data_<timestamp>.csv` | Cleaned and merged full dataset |
| `account_status_<timestamp>.csv`     | Customer activity status by last order date |

All files are automatically uploaded to shared Google Drive folders for recordkeeping and reuse.

---

## 🧠 Smart Features

- 🔁 Skips already-processed files to prevent duplicates
- 🚫 Flags failed uploads or incorrect file formats
- 🧹 Automatically deletes temporary files after upload
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
   git clone https://github.com/your-org/nonnybeer-handoff.git
   cd nonnybeer-handoff
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
### 🔐 Credentials (Secure Delivery)

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

---
