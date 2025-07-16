import streamlit as st
import pandas as pd
import os
from langchain.agents import create_csv_agent
from langchain.chat_models import ChatOpenAI

# Page setup
st.set_page_config(page_title="Nonny Beer Dashboard", layout="wide")

# --- Password protection ---
password = st.text_input("Enter access password:", type="password")
if password != "nonny123":
    st.warning("Incorrect or missing password. Please contact admin for access.")
    st.stop()

# --- Title and intro ---
st.title("🍺 Nonny Beer Data Portal")
st.markdown("Welcome to the Nonny Beer data platform. View sales dashboards, ask questions, and explore the project repository.")

# --- Power BI section (placeholder) ---
st.header("📊 Power BI Dashboard (Coming Soon)")
st.info("The Power BI dashboard iframe will be embedded here once the link is available.")

# --- Load cleaned data path ---
csv_path = "data/cleaned_sales.csv"  # Replace with your own CSV path if needed

# --- Set OpenAI API key from Streamlit secrets ---
os.environ["OPENAI_API_KEY"] = st.secrets["OPENAI_API_KEY"]

# --- Initialize LangChain CSV Agent ---
@st.cache_resource
def load_agent():
    return create_csv_agent(
        ChatOpenAI(temperature=0, model="gpt-4"),
        csv_path,
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
df = pd.read_csv(csv_path)
st.dataframe(df.head(20))

# --- GitHub project link ---
st.header("📂 Project Repository")
st.markdown("[Click to view the GitHub repo](https://github.com/your_username/nonnybeer-dashboard)")

