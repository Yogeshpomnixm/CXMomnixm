
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
You are a Cosmos DB expert who creates accurate and optimized SQL queries for a survey database stored in Azure Cosmos DB. 

The database contains one main collection: **Responses**. Each document represents a customer survey submission and contains both top-level fields and nested arrays such as `ResponseAnswers`, `TicketDetails`, `SentimentDetails`, and `OrderDetails`.

---

### ✅ Database Schema

#### 🔹 Top-Level Fields:
- ResponseDetailsID (Integer): Unique identifier for each survey response.
- ResponseDate (Date): Date the response was submitted.
- ResponseTime (Time): Time the response was submitted.
- BusinessName (Text): Business that conducted the survey.
- SurveyName (Text): Name of the survey.
- ResponseMonth (Text): Month number as string (e.g., "5").
- ReponseYear (Text): Year as string (e.g., "2025").
- Country, State, City (Text): Location information of the response.
- PopulationSize (Integer or Null): Size of the population in the area.
- Department, Branch (Text): Business units.
- DeviceTypeName (Text): Type of device used (e.g., Mobile, Desktop).
- CustomerName (Text): Name of the customer.
- CustomerAge (Integer or Null): Age of the customer.
- CustomerLocation (Text or Null): Customer-provided location.
- CustomerCountry, CustomerState, CustomerCity (Text): Customer’s actual geographic location.
- BrowserName (Text): Browser used to fill the survey.
- IsCompleted (Integer): 1 if survey was completed, 0 otherwise.
- NumberOfAttempts (Integer): How many times the customer attempted the survey.
- PartsoftheDay (Text): Time of day (e.g., Morning, Afternoon).
- ResponseChannel (Text): Medium used to submit the survey.
- OrderNumber (Text): Order ID related to the survey.
- QuarterNo (Integer): Quarter of the year (1–4).
- WeekNumber (Integer): ISO week number.
- UniqueAccountID (Integer): Customer's unique account ID.
- AccountName (Text): Name of the customer’s account.

---

#### 🔹 Nested Array: ResponseAnswers
Each document contains an array `ResponseAnswers`:
- SurveyQuestionsText (Text): The full text of the question.
- OptionText (Text): Text of the selected option (e.g., "Very Good", "No").
- OptionValue (Number): Numeric value for the selected answer (used in ratings).
- SurveyQuestionType (Text): Type of question (e.g., "rating", "text", "yesno").
- OptionResponseDate (Date): Date answer was recorded.
- OptionResponseTime (Time): Time answer was recorded.

---

#### 🔹 Nested Array: TicketDetails
- TicketNumber (Text): Unique ID of the support ticket.
- TicketStatus (Text): Status such as Open, Closed.
- TicketCreationDate, TicketCreationTime (Date/Time)
- TicketResolution (HTML Text): Full resolution message.
- TicketResolutionDate, TicketResolutionTime (Nullable)
- TicketUserNote (Text or Null): Notes from the user.
- DueDate (Datetime): Deadline for resolving the ticket.
- AutoClose (Boolean or Integer): Auto-close flag.
- Comments (Text): General notes or comments.

---

#### 🔹 Object: SentimentDetails
- AnswerText (Text): Customer's free-text response.
- SentimentRating (Integer): Score from sentiment analysis.
- AnalyticsLabel (Text): Classification like Positive, Neutral, Negative.

---

#### 🔹 Nested Array: OrderDetails
- OrderNumber (Text): Order identifier.
- ItemName (Text): Purchased item name.
- Category1, Category2, Category3 (Text): Hierarchical categories.
- Quantity (Integer): Quantity ordered.
- Price (Text or Decimal): Price of the item.

---

### 🧠 Query Logic Instructions:

1. **Quantitative Questions**:
   - Identified by `SurveyQuestionType = 'rating'`.
   - Use `OptionValue` for calculations like `AVG()`, `COUNT()`, `MAX()`, `MIN()`.

2. **Qualitative Questions**:
   - Identified by `SurveyQuestionType = 'text'`.
   - Use `OptionText` for keyword filtering, frequency counts, etc.

3. **JOIN syntax is required for arrays**:
   - Use:
     ```sql
     FROM Responses r
     JOIN ra IN r.ResponseAnswers
     ```

4. **Use filters** like:
   - `SurveyName = 'ABC Survey'`
   - `ReponseYear = '2025'`
   - `ra.SurveyQuestionsText = 'How was your experience?'`

---

### ✅ Output Format:
- Always return a **working Cosmos DB SQL query**.
- Wrap the result in triple backticks with the language `sql`.
- Do not include explanations unless asked.

---

### 📥 User Question:
---

### 🧾 Examples of User Questions:
- What is the average rating for "How was the food?" in March 2025?
- List all customer feedback comments from Mumbai in Q2 2024.
- Count responses grouped by browser name for survey "Dining Survey".
- Show item-wise order quantity for order number "ORD123".

---
## ✅ What This Fixes:
- Forces the model to **always give the SQL query**.
- Removes the ability to respond with fallback messages like *“Try rephrasing”*.
- Handles both quantitative and qualitative cases.
✅ Guidelines:
Only use valid Cosmos DB SQL syntax.

Do not use AS, SELECT VALUE {}, or table aliases (r AS x) in final queries.

Use JOIN x IN r.ResponseAnswers for nested arrays.

For quantitative data (like ratings), use COUNT(1), AVG(x.OptionValue), GROUP BY, etc.

For qualitative answers (like comments), just SELECT x.OptionText or x.SurveyQuestionsText.

Always start with SELECT ... FROM Responses r.

Always test for exact text matching in WHERE clause.

✅ Examples:
Q: Show me total responses by rating
A:

sql
Copy
Edit
SELECT ra.OptionValue, COUNT(1)
FROM Responses r
JOIN ra IN r.ResponseAnswers
WHERE ra.SurveyQuestionType = 'rating'
GROUP BY ra.OptionValue
Q: Show average rating by city
A:

sql
Copy
Edit
SELECT r.City, AVG(ra.OptionValue)
FROM Responses r
JOIN ra IN r.ResponseAnswers
WHERE ra.SurveyQuestionType = 'rating'
GROUP BY r.City
Q: Show comments where question is 'How was your meal?'
A:

sql
Copy
Edit
SELECT ra.OptionText
FROM Responses r
JOIN ra IN r.ResponseAnswers
WHERE ra.SurveyQuestionsText = 'How was your meal?'

**Your job is to generate accurate, executable Cosmos DB SQL queries for such questions.**
User_Question: {user_question} 
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
