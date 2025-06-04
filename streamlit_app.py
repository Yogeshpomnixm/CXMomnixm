
import streamlit as st
import pandas as pd
import openai
import time
import io # Added for df.info() debugging
import pyodbc
from azure.cosmos import CosmosClient, PartitionKey
import os
import requests

# --- Set the page title ---
st.set_page_config(page_title="omniSense Assistant", page_icon="💬")
st.title("💬 omniSense ChatBot")
# --- DATABASE CONFIG ---
secrets = st.secrets["database"]
# --- DATABASE CONNECTION FUNCTION ---
def get_connection():
    try:
        #st.write("Attempting to connect to database...")
        secrets = st.secrets["database"]

        # Corrected f-string for printing the driver
        #st.write(f"Using driver: {secrets['driver']}")
        # Corrected f-string for the connection string
        # Each parameter needs to be separated by a semicolon within the string
        conn_str = (
            f"DRIVER={secrets['driver']};"
            f"SERVER={secrets['server']};"
            f"DATABASE={secrets['database']};"
            f"UID={secrets['username']};"
            f"PWD={secrets['password']};"
            "TrustServerCertificate=yes;" # This is specific to SQL Server
        )

        conn = pyodbc.connect(conn_str)
        st.success("Successfully connected to the database!")
        return conn
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        st.info("Please check your database credentials in Streamlit Cloud secrets, "
                "database firewall rules, and ensure the correct ODBC driver "
                "is installed via packages.txt.")
        return None

# --- FETCH DATA BASED ON USER QUERY ---
# def run_query(user_query):
#     conn = get_connection()
#     if conn:
#         st.info("✅ Connected to database")
#         try:
#             df = pd.read_sql(user_query, conn)
#             #st.success("✅ Data fetched successfully!")
#             #st.dataframe(df)  # Show the data
#             return df
#         except Exception as e:
#             #st.error(f"❌ Query error: {e}")
#             return "Query error: {e}"
#         finally:
#             conn.close()
#     else:
#         #st.error("❌ Failed to connect to the database.")
#         return "Failed to connect to the database."

# Set credentials (use Streamlit secrets or env vars for security)
COSMOS_URI =f"{secrets['COSMOS_URI']}" #st.secrets["COSMOS_URI"]
COSMOS_KEY =f"{secrets['COSMOS_KEY']}" #st.secrets["COSMOS_KEY"]
DATABASE_ID = "Collection"
CONTAINER_ID = "Responses"

# Connect to Cosmos DB
client = CosmosClient(COSMOS_URI, COSMOS_KEY)
database = client.get_database_client(DATABASE_ID)
container = database.get_container_client(CONTAINER_ID)

# --- FIX QUERY FOR COSMOS DB ---
def fix_cosmos_query(raw_query):
    # Convert to string explicitly and strip
    query = str(raw_query).strip()

    # Fix COUNT queries
    if "COUNT(" in query.upper() and "SELECT VALUE" not in query.upper():
        query = query.replace("SELECT", "SELECT VALUE", 1)

    # Replace any '==' with '='
    query = query.replace("==", "=")

    return query

# --- RUN QUERY FUNCTION ---
def run_query(user_query):
    if not isinstance(user_query, str):
        user_query = str(user_query)

    st.write("User Query:", user_query)
    fixed_query = fix_cosmos_query(user_query)

    if not isinstance(fixed_query, str):
        fixed_query = str(fixed_query)

    st.write("Fixed Query:", fixed_query)

    try:
        items = list(container.query_items(
            query=fixed_query,
            enable_cross_partition_query=True
        ))
        return pd.DataFrame(items) if items else pd.DataFrame()
    except Exception as e:
        st.error("❌ Query Execution Error")
        return pd.DataFrame([{"error": str(e)}])


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

A quantitative question asks for total, numbers, counts, averages, percentages, sum, group by, unique list of categories, unique list of unit, categories list, unit list, top, max, min, from date, to date, year, month, month names, date etc and all query type questions.
A qualitative question asks for reasons, descriptions, categories, sales, amount, unit, month or opinions.

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
You are an AI assistant that converts natural language questions into Cosmos DB SQL queries for a survey response database. The database has a single container called **“Responses”** in Azure Cosmos DB using the SQL API.

Schema Definition:  
Top-Level Fields:  
- ResponseDetailsID (Integer)  
- ResponseDate (ISO 8601 Date String)  
- ResponseTime (Text)  
- BusinessName, SurveyName (Text)  
- ResponseMonth, ReponseYear (Text)  
- Country, State, City (Text)  
- Department, Branch, DeviceTypeName (Text)  
- CustomerName, CustomerLocation, CustomerCountry, CustomerState, CustomerCity (Text)  
- CustomerAge (Integer or Null)  
- BrowserName (Text)  
- IsCompleted, NumberOfAttempts, QuarterNo, WeekNumber, UniqueAccountID (Integer)  
- PartsoftheDay, ResponseChannel, OrderNumber, AccountName (Text)  

Embedded Arrays and Objects:  

**ResponseAnswers** (Array of objects):  
- ResponseDetailsID, ResponseDate, ResponseTime (Date String or Text)  
- SurveyQuestionsText, OptionText, SurveyQuestionType (Text)  
- OptionValue (Text or Number)  
- OptionResponseDate, OptionResponseTime (Date String or Text)  

**TicketDetails** (Array of objects):  
- ResponseDetailsID, TicketNumber (Text), TicketStatus (Text)  
- TicketCreationDate, TicketResolutionDate, DueDate (Date Strings)  
- TicketResolution, TicketUserNote, Comments (Text)  
- AutoClose (Boolean or Integer)  

**SentimentDetails** (Object, optional):  
- ResponseDetailsID, ResponseDate (Date)  
- AnswerText (Text), SentimentRating (Integer), AnalyticsLabel (Text)  

**OrderDetails** (Array of objects):  
- ResponseDetailsID, OrderNumber, ItemName, Category1, Category2, Category3 (Text)  
- Quantity (Integer), Price (Number or Text)

Survey Question Logic:  
- Q1: “How was your dining experience?” — Rating scale 1–5  
- Q2: “What drove poor experience?” — Checkbox options (shown if Q1 is 1–3)  
- Q3: “What drove great experience?” — Checkbox options (shown if Q1 is 4–5)  
- Q4: “Additional comments” — Text  

Important Querying Logic:
- Cosmos DB SQL is **case-sensitive**. Use correct casing for all aliases and field names.
  - Example: Use `td.TicketCreationDate`, not `TD.TicketCreationDate`
  - Example: Use `ra.OptionValue`, not `RA.OptionValue`
- For satisfaction by driver (e.g., "Taste"), filter `ra.OptionText` and compute average of Q1 (`ra.OptionValue`)  
- Use `IS_DEFINED()` before accessing array values or nested fields  
- `STARTSWITH(r.ResponseDate, "YYYY-MM")` for month filtering  
- Use `JOIN` syntax for arrays: `JOIN ra IN r.ResponseAnswers`, etc.  
- Use `CONTAINS(LOWER(field), "text")` for case-insensitive searches  
- Use `TOP N` to limit result count — do not use `LIMIT`  
- All numerical filters on OptionValue must check `> 0` after using `IS_DEFINED()`  
- `OptionValue` may be stored as a string — apply implicit conversion by ensuring numeric filtering (e.g., `ra.OptionValue > 0`)  

Instructions:
- Always generate **SELECT queries only**
- Use **Cosmos DB SQL syntax**, not T-SQL
- **Field names and aliases must match case exactly — Cosmos DB SQL is case-sensitive**
- Only return fields that are meaningful for the query
- Avoid SELECT * — use specific fields and aliases
- Assume missing embedded objects/arrays must be safely checked using `IS_DEFINED()`  

Case Sensitivity:
- Cosmos DB SQL is **case-sensitive**. Use correct case for aliases and fields (e.g., use `td.TicketCreationDate`, not `TD.TicketCreationDate`).

Aggregate Function Formatting:
- For scalar results like counts, averages, etc., always use `SELECT VALUE COUNT(1)` or `SELECT VALUE AVG(field)` instead of using `AS`.

Defensive Checks:
- Always check for nested array existence using `IS_DEFINED()`, e.g., `IS_DEFINED(td.TicketCreationDate)` before filtering.

Example 1:  
User_Question: What are the top 2 drivers of highest satisfaction for February 2025  
Expected Output:  
SELECT TOP 2  
    ra.OptionText AS Driver,  
    AVG(ra.OptionValue) AS AvgSatisfactionScore  
FROM r  
JOIN ra IN r.ResponseAnswers  
WHERE  
    STARTSWITH(r.ResponseDate, "2025-02")  
    AND IS_DEFINED(ra.OptionValue)  
    AND ra.OptionValue > 0  
GROUP BY ra.OptionText  
ORDER BY AvgSatisfactionScore DESC

Example 2:  
User_Question: What is the average satisfaction for taste in May 2025  
Expected Output:  
SELECT  
    AVG(ra.OptionValue) AS AvgTasteSatisfaction  
FROM r  
JOIN ra IN r.ResponseAnswers  
WHERE  
    STARTSWITH(r.ResponseDate, "2025-05")  
    AND IS_DEFINED(ra.OptionValue)  
    AND ra.OptionValue > 0  
    AND CONTAINS(LOWER(ra.OptionText), "taste")

User_Question: {user_question}  
Expected Output: Cosmos DB SQL Query:
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

    Examples for numerical results:
    - User: "What is the total amount?" Result: "1234.56"
      Response: "The total amount across all records is $1,234.56."
    - User: "What was the travel expense in March?" Result: "150.00"
      Response: "The travel expense in March was $150.00."

    Examples for list results:
    - User: "List unique categories?" Result: "['Food', 'Rent', 'Utilities']"
      Response: "The unique categories found in the dataset are Food, Rent, and Utilities."
    - User: "Show me the units with amounts over 500." Result: "['Unit_A', 'Unit_C']"
      Response: "Units with amounts over $500 include Unit_A and Unit_C."

    Examples for error results:
    - User: "Calculate X divided by zero." Result: "An error occurred: Division by zero"
      Response: "I encountered an issue while processing that request: Division by zero. Please try rephrasing your question."
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
                if result_df is not None and not result_df.empty:
                   
                    if result_df.shape == (1, 1):
                        result_value = result_df.iloc[0, 0]
                        
                        response = ask_SmartResponse(user_question, result_value)+":"+{python_expr}
                    else:
                        response = ask_SmartResponse(user_question, result_df)+":"+{python_expr}
                else:
                    # Case 1: Query ran successfully but returned no rows.
                    # This is where you want your "no data" smart answer.
                    # Prompt for ask_SmartResponse: "No data was found for your specific question.
                    # Please consider rephrasing or checking details."
                    response = f"I couldn't find any information for your specific question.  " \
                    f"Perhaps try rephrasing it or checking for typos."
                    # response = ask_SmartResponse(
                    #     user_question,
                    #     "I couldn't find any information for your specific question. "
                    #     "Perhaps try rephrasing it or checking for typos."
                    # )

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