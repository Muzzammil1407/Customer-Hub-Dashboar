import streamlit as st
import pandas as pd
import altair as alt
import google.generativeai as genai
import time
import pdfplumber
import io
import sys

# --- --- --- --- --- --- --- --- --- --- --- --- --- ---
# --- 1. PASTE YOUR **NEW** GOOGLE AI STUDIO API KEY HERE ---
# --- --- --- --- --- --- --- --- --- --- --- --- --- ---
# Replace this with the NEW key you just generated
GOOGLE_API_KEY = "AIzaSyD1YDMRLYqYA5tQGzL1YBV69tpUvbjuHGo"
# --- --- --- --- --- --- --- --- --- --- --- --- --- ---


# --- Page Configuration ---
st.set_page_config(
    page_title="Customer Data Hub",
    page_icon="📊",
    layout="wide"
)

# --- --- --- --- --- --- --- --- --- ---
# --- CORE DASHBOARD HELPER FUNCTIONS ---
# --- --- --- --- --- --- --- --- --- ---

def parse_file(uploaded_file):
    """Parses a single file based on its extension."""
    if uploaded_file is None:
        return None
        
    try:
        file_name = uploaded_file.name
        if file_name.endswith('.csv'):
            
            # --- --- --- THIS IS THE FIX --- --- ---
            # Try to read as standard utf-8
            try:
                return pd.read_csv(uploaded_file, encoding='utf-8')
            except UnicodeDecodeError:
                # If it fails with the error you saw, reset the file
                # and read with 'utf-8-sig' which handles the BOM
                uploaded_file.seek(0)
                return pd.read_csv(uploaded_file, encoding='utf-8-sig')
            # --- --- --- END OF FIX --- --- ---

        elif file_name.endswith('.json'):
            # Also add encoding handling for JSON, just in case
            try:
                return pd.read_json(uploaded_file, encoding='utf-8')
            except ValueError:
                uploaded_file.seek(0)
                return pd.read_json(uploaded_file, encoding='utf-8-sig')

        elif file_name.endswith(('.xlsx', '.xls')):
            return pd.read_excel(uploaded_file)
        elif file_name.endswith('.xml'):
            return pd.read_xml(uploaded_file)
        else:
            st.error(f"Unsupported file type: {file_name}. Please upload CSV, JSON, Excel, or XML.")
            return None
    except Exception as e:
        st.error(f"Error parsing file {file_name}: {e}")
        # Print the full error to the terminal for debugging
        print(f"Error parsing {file_name}: {e}", file=sys.stderr)
        return None

@st.cache_data
# MODIFICATION: Removed credit_file_bytes and credit_name
def load_and_process_data(master_file_bytes, master_name,
                          retail_file_bytes, retail_name,
                          upi_file_bytes, upi_name):
    """Loads, processes, and merges the 3 core data files from bytes."""
    
    # Re-create file objects from bytes (this is needed for caching)
    master_file = io.BytesIO(master_file_bytes); master_file.name = master_name
    retail_file = io.BytesIO(retail_file_bytes); retail_file.name = retail_name
    upi_file = io.BytesIO(upi_file_bytes); upi_file.name = upi_name

    all_dfs = {}
    
    # --- 1. Load All Files ---
    try:
        st.write("Loading Customer Master...")
        df_master = parse_file(master_file)
        if df_master is None: return None
        all_dfs['master'] = df_master

        st.write("Loading Retail Transactions...")
        df_retail = parse_file(retail_file)
        if df_retail is None: return None
        all_dfs['retail'] = df_retail

        st.write("Loading UPI Transactions...")
        df_upi = parse_file(upi_file)
        if df_upi is None: return None
        all_dfs['upi'] = df_upi
        
    except Exception as e:
        st.error(f"Failed to load one or more files: {e}")
        print(f"File loading error: {e}", file=sys.stderr)
        return None

    # --- 2. Transform and Standardize IDs ---
    try:
        st.write("Standardizing Customer IDs...")
        
        # Master
        if 'Customer_ID' not in df_master.columns:
            st.error("Master file is missing 'Customer_ID' column.")
            return None
        df_master['Customer_ID'] = df_master['Customer_ID'].astype(str)

        # Retail
        if 'Customer_ID' not in df_retail.columns:
            st.error("Retail file is missing 'Customer_ID' column.")
            return None
        df_retail['Customer_ID'] = df_retail['Customer_ID'].astype(str)

        # UPI (Check for 'Customer id' or 'Customer_ID')
        if 'Customer id' in df_upi.columns:
            df_upi.rename(columns={'Customer id': 'Customer_ID'}, inplace=True)
        if 'Customer_ID' not in df_upi.columns:
            st.error("UPI file is missing 'Customer_ID' or 'Customer id' column.")
            return None
        df_upi['Customer_ID'] = df_upi['Customer_ID'].astype(str)
        
    except Exception as e:
        st.error(f"Failed to standardize Customer_ID columns: {e}")
        print(f"ID standardization error: {e}", file=sys.stderr)
        return None

    # --- 3. Merge Data ---
    try:
        st.write("Merging dataframes...")
        merged_df = df_master
        merged_df = pd.merge(merged_df, df_retail, on='Customer_ID', how='outer', suffixes=('_master', '_retail'))
        merged_df = pd.merge(merged_df, df_upi, on='Customer_ID', how='outer', suffixes=('', '_upi'))
        
        st.write(f"Merging complete. Total records found: {len(merged_df)}")

        # --- 4. Clean Data ---
        st.write("Cleaning merged data...")
        cat_cols = ['Product_Category', 'Payment_Method', 'merchant_category', 'transaction_type']
        for col in cat_cols:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].fillna('Unknown')
        
        return merged_df

    except Exception as e:
        st.error(f"Failed to merge data: {e}")
        print(f"Merging error: {e}", file=sys.stderr)
        return None

def convert_df_to_csv(df):
    """Converts a DataFrame to a CSV string for download."""
    return df.to_csv(index=False).encode('utf-8')

# --- --- --- --- --- --- --- --- ---
# --- FILE INSPECTOR HELPER FUNCTION ---
# --- --- --- --- --- --- --- --- ---

@st.cache_data
def parse_pdf_file(uploaded_file_bytes, file_name):
    """Parses text from a PDF file."""
    try:
        text = ""
        with pdfplumber.open(io.BytesIO(uploaded_file_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n--- End of Page ---\n"
        
        if not text:
            return "This PDF contains no extractable text. It might be a scanned image.", "text"
        return text, "text"
    except Exception as e:
        return f"Error parsing PDF {file_name}: {e}", "error"

# --- --- --- --- --- --- ---
# --- SIDEBAR & UI ---
# --- --- --- --- --- --- ---
st.title("📊 Customer Data Processing and Visualization Hub")

# --- 1. Core Dashboard File Upload ---
st.sidebar.header("1. Main Dashboard Upload")
st.sidebar.info("Upload all 3 core files to build the main dashboard.")

st.sidebar.markdown("---")
st.sidebar.subheader("Core Data Files")
master_file = st.sidebar.file_uploader("Upload customer_master_data (JSON)")
retail_file = st.sidebar.file_uploader("Upload retail_transaction file")
upi_file = st.sidebar.file_uploader("Upload upi_transactions file")
st.sidebar.markdown("---")


if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

if st.sidebar.button("Process Core Files", type="primary"):
    if master_file and retail_file and upi_file:
        with st.spinner("Processing files... See console for details."):
            status_placeholder = st.empty()
            
            def log_status(message):
                status_placeholder.write(message)
            
            old_write = st.write
            st.write = log_status

            # Read files into bytes to pass to cached function
            master_bytes = master_file.getvalue()
            retail_bytes = retail_file.getvalue()
            upi_bytes = upi_file.getvalue()

            merged_data = load_and_process_data(
                master_bytes, master_file.name,
                retail_bytes, retail_file.name,
                upi_bytes, upi_file.name
            )
            
            st.write = old_write 
            status_placeholder.empty()

            if merged_data is not None and not merged_data.empty:
                st.session_state.processed_data = merged_data
                st.sidebar.success("Core files processed successfully!")
                st.sidebar.metric("Total Customers", merged_data['Customer_ID'].nunique())
                st.sidebar.metric("Total Merged Records", len(merged_data))
            else:
                st.sidebar.error("Failed to process data. Check error messages above.")
                print("Data processing failed. Check sidebar errors.", file=sys.stderr)
    else:
        st.sidebar.warning("Please upload all three core files.")

st.sidebar.divider()

# --- 2. PDF File Inspector Upload ---
st.sidebar.header("2. PDF Inspector")
st.sidebar.info("Upload a PDF file to extract its text.")
pdf_file = st.sidebar.file_uploader("Upload a PDF")


# --- --- --- --- --- --- ---
# --- MAIN PAGE WITH TABS ---
# --- --- --- --- --- --- ---

tab_list = ["🚀 Main Dashboard"]
if st.session_state.processed_data is not None:
    tab_list.extend(["📄 View Merged Data", "📈 Dashboard", "📥 Download", "🤖 Chat About Data"])

if pdf_file is not None:
    tab_list.append("🔍 PDF Inspector")

tabs = st.tabs(tab_list)

# --- Tab 1: Welcome / Main Dashboard Home ---
with tabs[0]:
    st.header("Welcome to the Data Hub")
    if st.session_state.processed_data is None:
        st.info("Please upload and process the three core data files using the sidebar to activate the dashboard.")
    else:
        st.success("Core data processed. Select other tabs to see the results.")
    
    st.subheader("High Level Summary")
    if st.session_state.processed_data is not None:
        merged_df = st.session_state.processed_data
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Merged Records", f"{len(merged_df):,}")
        col2.metric("Total Customers", merged_df['Customer_ID'].nunique())
        if 'State' in merged_df.columns:
            col3.metric("Total States", merged_df['State'].nunique())
    else:
        st.write("Upload core files to see summary.")

# --- Dashboard & Data Tabs ---
if st.session_state.processed_data is not None:
    merged_df = st.session_state.processed_data
    
    # --- Tab 2: View Merged Data ---
    with tabs[1]:
        st.header("Merged Customer Data")
        
        # --- --- --- THIS IS THE DATAFRAME FIX --- --- ---
        st.write(f"Displaying first 10,000 of {len(merged_df)} combined records. Full data is used for charts and downloads.")
        st.dataframe(merged_df.head(10000), use_container_width=True)
        # --- --- --- END OF DATAFRAME FIX --- --- ---

    # --- Tab 3: Dashboard ---
    with tabs[2]:
        st.header("Data Visualization Dashboard")
        col1, col2 = st.columns(2)
        
        # --- --- --- THIS IS THE CHART FIX --- --- ---
        # We pre-aggregate the data *before* sending it to Altair.
        # This sends a tiny dataframe (e.g., 50 states) instead of 508MB.
        # --- --- --- --- --- --- --- --- --- --- --- ---
        
        with col1:
            st.subheader("Customers by State")
            if 'State' in merged_df.columns:
                # 1. Aggregate data first
                state_counts = merged_df['State'].value_counts().reset_index()
                state_counts.columns = ['State', 'count']
                
                # 2. Plot the small, aggregated data
                chart_state = alt.Chart(state_counts).mark_bar().encode(
                    x=alt.X('State', sort='-y'),
                    y=alt.Y('count', title='Number of Customers'),
                    tooltip=['State', 'count']
                ).interactive()
                st.altair_chart(chart_state, use_container_width=True)
            else:
                st.info("No 'State' column found in merged data.")
            
            st.subheader("Customers by Income Level")
            if 'Income' in merged_df.columns:
                # 1. Aggregate data first
                income_counts = merged_df['Income'].value_counts().reset_index()
                income_counts.columns = ['Income', 'count']

                # 2. Plot the small, aggregated data
                chart_income = alt.Chart(income_counts).mark_arc(innerRadius=50).encode(
                    theta=alt.Theta("count", stack=True),
                    color=alt.Color("Income"),
                    tooltip=['Income', 'count']
                ).interactive()
                st.altair_chart(chart_income, use_container_width=True)
            else:
                st.info("No 'Income' column found in merged data.")

        with col2:
            st.subheader("Retail Transactions by Product Category")
            if 'Product_Category' in merged_df.columns:
                # 1. Aggregate data first
                product_counts = merged_df['Product_Category'].value_counts().reset_index()
                product_counts.columns = ['Product_Category', 'count']
                
                # 2. Plot the small, aggregated data
                chart_product = alt.Chart(product_counts).mark_bar().encode(
                    x=alt.X('Product_Category', sort='-y'),
                    y=alt.Y('count', title='Number of Transactions'),
                    tooltip=['Product_Category', 'count']
                ).interactive()
                st.altair_chart(chart_product, use_container_width=True)
            else:
                st.info("No 'Product_Category' column found in merged data.")

            st.subheader("UPI Transactions by Merchant Category")
            if 'merchant_category' in merged_df.columns:
                # 1. Aggregate data first
                merchant_counts = merged_df['merchant_category'].value_counts().reset_index()
                merchant_counts.columns = ['merchant_category', 'count']

                # 2. Plot the small, aggregated data
                chart_upi_cat = alt.Chart(merchant_counts).mark_bar().encode(
                    x=alt.X('merchant_category', title='Merchant Category', sort='-y'),
                    y=alt.Y('count', title='Number of Transactions'),
                    tooltip=['merchant_category', 'count']
                ).interactive()
                st.altair_chart(chart_upi_cat, use_container_width=True)
            else:
                st.info("No 'merchant_category' column found in merged data.")

    # --- Tab 4: Download & Reports ---
    with tabs[3]:
        st.header("Download Processed Data")
        st.write("Click the button below to download the complete merged dataset as a CSV file.")
        csv_data = convert_df_to_csv(merged_df)
        st.download_button(
            label="📥 Download Merged Data as CSV",
            data=csv_data,
            file_name="merged_customer_data.csv",
            mime="text/csv",
        )
        
        st.subheader("Data Quality Report (Missing Values)")
        missing_data = merged_df.isnull().sum().reset_index()
        missing_data.columns = ['Column', 'Missing Values']
        missing_data = missing_data[missing_data['Missing Values'] > 0].sort_values(by='Missing Values', ascending=False)
        st.dataframe(missing_data, use_container_width=True)

    # --- Tab 5: Chat About Data (Intermediate Task) ---
    with tabs[4]:
        st.header("Chat About Your Data")
        
        if GOOGLE_API_KEY == "PASTE_YOUR_NEW_API_KEY_HERE":
            st.error("API Key not provided. Please paste your new Google AI Studio API key at the top of the app.py file.")

        else:
            try:
                genai.configure(api_key=GOOGLE_API_KEY)
                
                # --- --- --- THIS IS THE CHATBOT FIX (Plan B) --- --- ---
                # Changed to 'gemini-1.5-pro-latest'
                # This is the most current, powerful model.
                model = genai.GenerativeModel('gemini-1.5-pro-latest')
                # --- --- --- END OF CHATBOT FIX --- --- ---
            
                if 'chat_context' not in st.session_state:
                    st.session_state.chat_context = f"""
                    You are an expert data analyst. I have a dataset with {merged_df.shape[0]} rows and {merged_df.shape[1]} columns.
                    The columns are: {', '.join(merged_df.columns)}.
                    Please answer questions based ONLY on this data context.
                    """

                question = st.text_input("Ask a question (e.g., 'How many customers are from Germany?' or 'What is the most common product category?')")
                
                if st.button("Ask"):
                    if question:
                        with st.spinner("Analyzing data and generating response..."):
                            prompt = st.session_state.chat_context + "\n\nUser Question: " + question
                            response = model.generate_content(prompt)
                            st.markdown(response.text)
                    else:
                        st.warning("Please enter a question.")
            
            except Exception as e:
                st.error(f"Error initializing AI model. Is your API key correct or enabled for this model? Error: {e}")
                print(f"Gemini API Error: {e}", file=sys.stderr)


# --- PDF Inspector Tab (for other files) ---
if pdf_file is not None:
    with tabs[-1]: # This will always be the PDF Inspector tab
        st.header(f"PDF Inspector: `{pdf_file.name}`")
        
        with st.spinner(f"Parsing {pdf_file.name}..."):
            pdf_bytes = pdf_file.getvalue()
            result, data_type = parse_pdf_file(pdf_bytes, pdf_file.name)
            
            if data_type == "text":
                st.info("Extracted text from PDF:")
                st.text_area("PDF Content", result, height=500)
            else:
                st.error(result)