
import streamlit as st
import pandas as pd
import openai
import time
import io # Added for df.info() debugging
import pyodbc
from azure.cosmos import CosmosClient, PartitionKey
import os
import requests
import json
# --- Set the page title ---
st.set_page_config(page_title="omniSense Assistant", page_icon="💬")
st.title("💬 omniSense ChatBot")
# --- DATABASE CONFIG ---
secrets = st.secrets["database"]

# Set credentials (use Streamlit secrets or env vars for security)
COSMOSAPI_URI =f"{secrets['API_URI']}" #st.secrets["COSMOS_URI"]
COSMOSAPI_KEY =f"{secrets['API_KEY']}"
# --- FETCH DATA BASED ON USER QUERY API ---
def run_query(user_query):   
    url = COSMOSAPI_URI
    
   # This will go into the POST body, not the URL
    payload = {
                "containerName": "Responses",
                "query": user_query  # Don't wrap in curly braces again
    }

                
    headers = {
                "accept": "text/plain",  # Use "application/json" if API returns JSON
                "X-API-KEY": COSMOSAPI_KEY,
                "Content-Type": "application/json"
    }
   
    try:
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            try:
                data = response.json()  # If API returns JSON
                print(data)
                df = pd.DataFrame(data)    
                print(df)            
                #st.success("✅ Data fetched successfully!")
                return df
            except ValueError:
                return response.text  # If response is plain text
        else:
            return f"API call failed with status code {response.status_code}: {response.text}"
    except Exception as e:
        return f"API request error: {e}"     

# --- API Key Input ---
user_api_key =f"{secrets['keyvalue']}" #st.text_input("🔑 Enter your OpenAI API Key:", type="password") #f"{secrets['keyvalue']}"

if not user_api_key:
    st.warning("⚠️ Please enter your OpenAI API key to continue.")
    st.stop()

# Set the API key
openai.api_key = user_api_key


# --- Load CSV ---
@st.cache_data
def load_data(file):
    return pd.read_csv(file)

# --- Format Data Context for Qualitative Questions ---
def format_data_context(df):
    context = ""
    # Take a smaller sample for context to avoid excessive token usage
    sample = df.head(5).fillna("N/A")
    context += "Here are the first 5 rows of your data:\n"
    context += sample.to_string() + "\n"
    context += f"The columns are: {', '.join(df.columns)}\n"
    return context

# --- Classify Question ---
def classify_question_type(question):
    prompt = f"""
You are a smart assistant that classifies questions as either 'Quantitative' or 'Qualitative'.

You are analyzing survey response data from a Cosmos DB. Each response has:
- Metadata like Date, Time, SurveyName, CustomerName, City, Browser, etc.
- An array of `ResponseAnswers` containing:
    - QuestionText, OptionText (feedback/comment), OptionValue (score), QuestionType (rating/text)
- You can group, summarize, or describe feedback based on OptionText or question content.

Question: "{question}"
Answer with only one word: Quantitative or Qualitative.
"""
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content.strip()

# --- Generate Python Expression ---
def ask_gpt_for_python_expression(user_question):
    prompt = f"""
System Role:
You are an AI assistant that converts natural language questions into **Azure Cosmos DB SQL API** queries for a survey response database. The data is stored in a single container named “Responses”.

---

Schema Overview:

Top-level fields:
- ResponseDetailsID (Integer)
- ResponseDate (ISO 8601 Date in string format, e.g., '2025-06-01')
- ResponseTime (Time)
- BusinessName (Text)
- SurveyName (Text)
- ResponseMonth (Text) — Month as string, e.g., "1", "12"
- ReponseYear (Text) — Year as string, e.g., "2023"
- Country, State, City (Text)
- PopulationSize (Integer or Null)
- Department, Branch, DeviceTypeName, CustomerName (Text)
- CustomerAge (Integer or Null)
- CustomerLocation, CustomerCountry, CustomerState, CustomerCity (Text or Null)
- BrowserName (Text)
- IsCompleted (Integer: 0 or 1)
- NumberOfAttempts (Integer)
- PartsoftheDay (Text)
- ResponseChannel (Text)
- OrderNumber (Text)
- QuarterNo (Integer)
- WeekNumber (Integer)
- UniqueAccountID (Integer)
- AccountName (Text)

Nested arrays:

1. ResponseAnswers (Array of Objects)
   - SurveyQuestionsText (Text)
   - OptionText (Text)
   - OptionValue (Text) — stored as a string
   - SurveyQuestionType (Text)
   - OptionResponseDate (Date), OptionResponseTime (Time)

2. TicketDetails (Array of Objects)
   - TicketNumber, TicketStatus (Text)
   - TicketCreationDate (Date in string format)
   - Other ticket-related fields

3. SentimentDetails (Object)
   - SentimentRating (Integer: -1, 0, 1)

4. OrderDetails (Array of Objects)
   - ItemName, Quantity (Integer), Price (Text or Decimal)

---

Query Rules:
- All queries must be **valid Cosmos DB SQL API syntax**.
- Use `JOIN x IN r.ArrayName` for nested arrays.
- Use `SELECT VALUE COUNT(1)` for count-based queries.
- Use `SELECT VALUE {{}}` to return JSON objects.
- Use `''` for all string comparisons (e.g., `x.OptionText = 'Poor'`)
- Use **integer comparisons** only for numeric fields like `CustomerAge`, `IsCompleted`, `QuarterNo`, etc.
- **DO NOT** use unsupported functions like `DATE_PART`, `FORMAT`, or `TO_CHAR`.
- To filter by month/year, use:  
  `r.ResponseMonth = '5'` and `r.ReponseYear = '2025'`  
  (do not use built-in date functions).
- For date comparisons, use direct string format (e.g., `r.ResponseDate >= '2025-01-01'`)
- Do **not** include SQL markdown like ```sql or any explanation.
- Do **not** return errors, always provide a working query.

Special Ticket Handling:
- If user query involves **tickets**, generate query like:
SELECT VALUE COUNT(1)
FROM Responses r
JOIN t IN r.TicketDetails
WHERE t.TicketCreationDate >= '2025-01-01' AND t.TicketCreationDate < '2026-01-01'
- Ensure ticket queries **join TicketDetails** and filter using `t.TicketCreationDate`.

---

User_Question: {user_question}  
SQL Query: cosmos_sql_query
"""
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content.strip()

# --- Qualitative Answer Generator ---
def ask_openai(question, context):
    prompt = f"""
You are a data analysis assistant. Here is the data context:

{context}

Now, based on this data, answer the following question:
{question}
"""
    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content.strip()

# --- Smart response Generator ---
def ask_SmartResponse(user_question, result):
    # Ensure result is a string, especially if it's a number or a list
    result_str = str(result)

    polish_prompt = f"""
    The user asked: "{user_question}"
    The core answer or result is: {result_str}

    Please respond in a natural, helpful, and intelligent tone, like a helpful data assistant.
    Focus on directly answering the user's question based on the provided result.
    Use complete English sentences.
    If the result is a long list, you can summarize it or mention a few key items naturally.
    If the result is a numerical value, clearly state what it represents.
    If the result is an error message, gracefully explain that the operation could not be completed and suggest a rephrase.

    """

    polished_response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": polish_prompt}]
    )

    return polished_response.choices[0].message.content.strip()

# --- Session state for chat history ---
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# --- Remove CSV upload logic ---
# Assume `run_query()` directly queries your SQL database (e.g., Z_Verizonomnisense)

# Show previous chat history
for entry in st.session_state.chat_history:
    st.markdown(f"**You:** {entry['question']}")
    st.markdown(f"**omniSense:** {entry['answer']}")

# Chat input (at bottom)
user_question = st.chat_input("Ask anything...")
if user_question:
    st.write("You:", user_question)
    with st.spinner("Processing..."):
        time.sleep(1)  # Simulate a short delay

        try:
            question_type = classify_question_type(user_question)
        except Exception as e:
            st.error(f"❌ Error classifying question: {e}")
            st.stop()  # Stop execution if classification fails

        if question_type.lower() == "quantitative":
            try:
                python_expr = ask_gpt_for_python_expression(user_question)

                # --- Clean the LLM's output ---
                if python_expr.startswith("SQL Query:"):
                    python_expr = python_expr.replace("SQL Query:", "").strip()
                else:
                    python_expr = python_expr.strip()
                
                
                # --- Run SQL query from expression ---
                result_df = run_query(python_expr)                
                # Check if the result is a DataFrame
                if isinstance(result_df, pd.DataFrame) and not result_df.empty:

                    if result_df.shape == (1, 1):
                        result_value = result_df.iloc[0, 0]
                        response = ask_SmartResponse(user_question, result_value)
                    else:
                        response = ask_SmartResponse(user_question, result_df)

                else:
                    # Handle if result_df is not a DataFrame or is empty
                    response = (
                        f"I couldn't find any information for your specific question.  "
                        f"Perhaps try rephrasing it or checking for typos.Query: {python_expr}"
                    )

            except Exception as e:
                # Case 2: An error occurred during query generation or execution.
                # This provides error details to the user, including the problematic expression.
                response = f"I'm sorry, I couldn't generate a response for that question right now. " \
                f"Could you please try asking something else? Error: {e} .Query: {python_expr}"
                # response = ask_SmartResponse(
                #     user_question,
                #     f"I couldn't process that request due to an error. "
                #     f"The attempted expression was: `{python_expr}`. "
                #     f"Please check your table or column names, or try a different question."
                # )

        else:  # Qualitative
            try:
                # Minimal context or static schema assumption (e.g., 'unit', 'category', 'date', 'amount')
                context = "The table contains data about transactions with columns like unit, category, date, and amount."
                raw_response = ask_openai(user_question, context)
                response = ask_SmartResponse(user_question, raw_response)
            except Exception as e:
                #response = f"❌ Error generating qualitative response:"
                response = f"I'm sorry, I couldn't generate a response for that question right now. " \
                f"Could you please try asking something else?"

        # Store in chat history
        st.session_state.chat_history.append({
            "question": user_question,
            "answer": response
        })

        st.rerun()
