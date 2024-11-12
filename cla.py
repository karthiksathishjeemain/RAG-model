import aiohttp
import asyncio
import pandas as pd
import streamlit as st
from groq import Groq
import csv
from google.oauth2 import service_account
from googleapiclient.discovery import build
import re
import os
from dotenv import load_dotenv
load_dotenv()
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_API_URL = os.getenv("GROQ_API_URL")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
SCOPES = os.getenv("SCOPES").strip("[]").replace("'", "").split(", ")


def init_session_state():
    if 'extraction_completed' not in st.session_state:
        st.session_state.extraction_completed = False
    if 'extracted_data' not in st.session_state:
        st.session_state.extracted_data = None
    if 'search_count' not in st.session_state:
        st.session_state.search_count = 0
    if 'current_sheet_id' not in st.session_state:
        st.session_state.current_sheet_id = None
    if 'input_type' not in st.session_state:
        st.session_state.input_type = None
    if 'input_df' not in st.session_state:
        st.session_state.input_df = None



credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build('sheets', 'v4', credentials=credentials)
sheet_service = service.spreadsheets()
client = Groq(api_key=GROQ_API_KEY)

async def search_web(query):
    """Uses SerpAPI to perform a web search based on the user's query."""
    url = f"https://serpapi.com/search.json?engine=google&q={query}&api_key={SERPAPI_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            st.session_state.search_count += 1
            print(f"Search call count: {st.session_state.search_count}")
            return [result["snippet"] for result in data.get("organic_results", [])]

async def extract_info_with_groq(snippets, prompt):
    """Passes search snippets to Groq API and extracts the relevant information."""
    full_text = "\n".join(snippets)
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": f"{prompt}\n\n{full_text}",
            }
        ],
        model="llama3-8b-8192",
    )
    return chat_completion.choices[0].message.content.strip()

async def extract_information(df, selected_column, user_prompt):
    """Extract information from the dataframe with progress tracking."""
    extracted_data = []
    total_rows = len(df[selected_column])
    progress_bar = st.progress(0)
    status_text = st.empty()

    for index, company in enumerate(df[selected_column]):
        status_text.text(f"Processing {company} ({index + 1}/{total_rows})")
        query = user_prompt.format(company=company)
        snippets = await search_web(query)
        email = await extract_info_with_groq(
            snippets, 
            "Extract the email address for " + company + " Note : the response should only contain email addresses seperated with comas and strictly there should be no explaination like `Here is the extracted email address`. The response should be just the relevant email address. If you failed to extract even a single email Id, then the response should be `Email Not Found`"
        )
        print(f"Email for {company}: {email}")
        extracted_data.append({"company": company, "email": email})
        progress_bar.progress((index + 1) / total_rows)

    progress_bar.empty()
    status_text.empty()
    return extracted_data

def extract_sheet_id(sheet_url):
    """Extracts Google Sheets ID from URL."""
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', sheet_url)
    return match.group(1) if match else None

def read_google_sheet(sheet_id):
    """Reads data from Google Sheets and returns a DataFrame."""
    try:
        result = sheet_service.values().get(spreadsheetId=sheet_id, range="Sheet1").execute()
        values = result.get("values", [])
        if not values:
            return pd.DataFrame()
        df = pd.DataFrame(values[1:], columns=values[0])
        return df
    except Exception as e:
        st.error(f"Error reading Google Sheet: {str(e)}")
        return pd.DataFrame()

def update_google_sheet(sheet_id, data):
    """
    Updates Google Sheet with extracted information.
    Adds a new column if 'email' column doesn't exist,
    otherwise updates the existing email column.
    """
    try:
     
        result = sheet_service.values().get(
            spreadsheetId=sheet_id,
            range="Sheet1"
        ).execute()
        current_values = result.get("values", [])
        
        if not current_values:
           
            values = [["Company", "Email"]] + [[row["company"], row["email"]] for row in data]
            range_to_update = "Sheet1!A1"
        else:
            headers = current_values[0]
            email_col_index = -1
            
          
            for idx, header in enumerate(headers):
                if header.lower() == "email":
                    email_col_index = idx
                    break
            
            if email_col_index == -1:
              
                new_col_letter = chr(65 + len(headers))  
                
                
                headers.append("Email")
                new_values = [headers]
                
                
                email_map = {row["company"]: row["email"] for row in data}
                
               
                for row in current_values[1:]:
                    company = row[0] 
                    row.append(email_map.get(company, "Email Not Found"))
                    new_values.append(row)
                
                range_to_update = f"Sheet1!A1:{new_col_letter}{len(new_values)}"
                values = new_values
            else:
              
                email_col_letter = chr(65 + email_col_index)
           
                email_map = {row["company"]: row["email"] for row in data}
                
              
                new_values = []
                for row in current_values[1:]: 
                    company = row[0]  
                    new_values.append([email_map.get(company, row[email_col_index])])
                
                range_to_update = f"Sheet1!{email_col_letter}2:{email_col_letter}{len(current_values)}"
                values = new_values

  
        body = {"values": values}
        result = sheet_service.values().update(
            spreadsheetId=sheet_id,
            range=range_to_update,
            valueInputOption="RAW",
            body=body
        ).execute()
        
        print(f"Sheet updated: {result}")
        return True
        
    except Exception as e:
        st.error(f"Error updating Google Sheet: {str(e)}")
        return False
def process_uploaded_csv(uploaded_file):
    """Process uploaded CSV file and return DataFrame."""
    try:
        df = pd.read_csv(uploaded_file)
        return df
    except Exception as e:
        st.error(f"Error reading CSV file: {str(e)}")
        return None
def main():
    st.title("AI Agent for Information Extraction")
    st.subheader("Choose your data input method")
    
 
    init_session_state()
    
   
    input_method = st.radio(
        "Select input method:",
        ["Upload CSV", "Google Sheets URL"],
        key="input_method"
    )
    
 
    if st.session_state.input_type != input_method:
        st.session_state.input_type = input_method
        st.session_state.extraction_completed = False
        st.session_state.extracted_data = None
        st.session_state.input_df = None
    
    df = None
    
    if input_method == "Upload CSV":
        uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
        if uploaded_file:
            df = process_uploaded_csv(uploaded_file)
            if df is not None:
                st.session_state.input_df = df
                st.write("CSV file uploaded successfully!")
                st.write("Preview of uploaded data:")
                st.write(df.head())
    
    else:  
        sheet_url = st.text_input("Enter Google Sheets URL")
        if sheet_url:
            sheet_id = extract_sheet_id(sheet_url)
            if sheet_id:
                if st.session_state.current_sheet_id != sheet_id:
                    st.session_state.current_sheet_id = sheet_id
                    st.session_state.extraction_completed = False
                    st.session_state.extracted_data = None
                df = read_google_sheet(sheet_id)
                if not df.empty:
                    st.session_state.input_df = df
                    st.write("Preview of sheet data:")
                    st.write(df.head())
            else:
                st.error("Invalid Google Sheets URL. Please check the URL format.")
    

    if st.session_state.input_df is not None:
        df = st.session_state.input_df
        st.write("Available columns:", df.columns.tolist())
        
      
        selected_column = st.selectbox(
            "Select the main column for search entities",
            df.columns,
            key="selected_column"
        )
        user_prompt = st.text_input(
            "Enter your search query with {company}",
            "Get the email of {company}",
            key="user_prompt"
        )

  
        if st.button("Extract Information", key="extract_button"):
            with st.spinner("Extracting information..."):
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    extracted_data = loop.run_until_complete(
                        extract_information(df, selected_column, user_prompt)
                    )
                    loop.close()
                    
                    st.session_state.extracted_data = extracted_data
                    st.session_state.extraction_completed = True
                    
             
                    results_df = pd.DataFrame(extracted_data)
                    st.write("Extracted Results:")
                    st.write(results_df)
                    st.success("Extraction completed successfully!")
                    
                except Exception as e:
                    st.error(f"Error during extraction: {str(e)}")
                    st.session_state.extraction_completed = False

      
        if st.session_state.extraction_completed and st.session_state.extracted_data:
        
            results_df = pd.DataFrame(st.session_state.extracted_data)
            
          
            st.subheader("Download Options")
            
        
            csv_extracted = results_df.to_csv(index=False, quoting=csv.QUOTE_ALL, lineterminator='\n')
            st.download_button(
                label="Download Extracted Results (CSV)",
                data=csv_extracted,
                file_name="extracted_results.csv",
                mime="text/csv",
                key="download_extracted"
            )
            
        
            merged_df = df.copy()
            email_map = {row["company"]: row["email"] for row in st.session_state.extracted_data}
            merged_df['email'] = merged_df[selected_column].map(email_map)
            
            csv_merged = merged_df.to_csv(index=False, quoting=csv.QUOTE_ALL, lineterminator='\n')
            st.download_button(
                label="Download Complete Dataset (CSV)",
                data=csv_merged,
                file_name="complete_dataset.csv",
                mime="text/csv",
                key="download_merged"
            )
            
         
            if input_method == "Google Sheets URL" and st.session_state.current_sheet_id:
                if st.button("Update Google Sheet", key="update_button"):
                    with st.spinner("Updating Google Sheet..."):
                        if update_google_sheet(st.session_state.current_sheet_id, st.session_state.extracted_data):
                            st.success("Google Sheet updated successfully!")

if __name__ == "__main__":
    main()