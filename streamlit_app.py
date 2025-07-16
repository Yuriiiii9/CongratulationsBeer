import streamlit as st
import pandas as pd
import os
from langchain.agents import create_csv_agent
from langchain.chat_models import ChatOpenAI

# Page setup
st.set_page_config(page_title="Nonny Beer Dashboard", layout="wide")

# --- Password protection ---
password = st.text_input("Enter access password:", type="password")
PASSWORD = st.secrets["APP_PASSWORD"]
if password != PASSWORD:
    st.warning("Incorrect or missing password. Please contact admin for access.")
    st.stop()

# --- Title and intro ---
st.title("🍺 Nonny Beer Data Portal")
st.markdown("Welcome to the Nonny Beer data platform. View sales dashboards, ask questions, and explore the project repository.")

# --- Power BI section (placeholder) ---
st.header("📊 Power BI Dashboard (Coming Soon)")
st.info("The Power BI dashboard iframe will be embedded here once the link is available.")

# --- Load cleaned data path ---
csv_url = st.secrets["PRIVATE_CSV_URL"]
df = pd.read_csv(csv_url)

# --- Set OpenAI API key from Streamlit secrets ---
os.environ["OPENAI_API_KEY"] = st.secrets["OPENAI_API_KEY"]

# --- Initialize LangChain CSV Agent using in-memory file ---
from langchain.agents import create_csv_agent
from langchain.chat_models import ChatOpenAI

@st.cache_resource
def load_agent():
    # Save df to a temporary file in memory so LangChain can access it
    tmp_path = "/tmp/cleaned_sales.csv"
    df.to_csv(tmp_path, index=False)
    return create_csv_agent(
        ChatOpenAI(temperature=0, model="gpt-3.5-turbo"),
        tmp_path,
        verbose=False
    )

agent = load_agent()

# --- Real AI Q&A section ---
st.header("🤖 Ask Questions About the Data (AI-Powered)")
user_query = st.text_input("Ask a question about the sales data (e.g., Which SKU sold the most?)")

if user_query:
    with st.spinner("AI is processing your question..."):
        try:
            response = agent.run(user_query)
            st.success(response)
        except Exception as e:
            st.error(f"Something went wrong: {e}")

# --- Preview the cleaned data ---
st.subheader("📋 Preview of Cleaned Sales Data")
st.dataframe(df.head(20))

# --- GitHub project link ---
st.header("📂 Project Repository")
st.markdown("[Click to view the GitHub repo](https://github.com/your_username/nonnybeer-dashboard)")
