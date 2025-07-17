import streamlit as st
import pandas as pd
import os
from langchain_groq import ChatGroq
from langchain_experimental.agents import create_csv_agent

# Page setup
st.set_page_config(page_title="Nonny Beer Dashboard", layout="wide")

# --- Password protection ---
password = st.text_input("Enter access password:", type="password")
PASSWORD = os.environ.get("APP_PASSWORD", "")

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
csv_url = os.environ.get("PRIVATE_CSV_URL", "")
df = pd.read_csv(csv_url)

# --- Set OpenAI API key from Streamlit secrets ---
AI_API_KEY = os.environ.get("GROQ_API_KEY", "")

# --- Initialize LangChain CSV Agent using in-memory file ---
from langchain_groq import ChatGroq
from langchain_experimental.agents.agent_toolkits.pandas.base import create_pandas_dataframe_agent

@st.cache_resource
def load_agent():
    return create_pandas_dataframe_agent(
        ChatGroq(
            temperature=0,
            model_name="llama3-8b-8192",  # also can choose "llama3-70b-8192"
            groq_api_key=AI_API_KEY,
        ),
        df,
        verbose=False,
        allow_dangerous_code=True
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
