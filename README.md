# 🍺 Nonny Beer Sales Data Platform

This project delivers a fully automated pipeline to clean, merge, and visualize monthly distributor sales data for **Nonny Beer**. It integrates multiple data sources (HORIZON, PSC, OLLIE and Shopify), standardizes file formats, and outputs ready-to-use CSV files for business analysis.

The platform includes:
- A **Streamlit Web App** for file preview, dashboard viewing, and AI-driven Q&A
- A **Power BI Dashboard** for sales trends and customer activity
- **Auto-upload to Google Drive** with versioned outputs

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
