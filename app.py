import streamlit as st
import pandas as pd
import io
import re

# --- CONFIGURATION ---
KNOWN_RPL_VARS = ['DV_BRAND_NAME', 'SITE_BRAND', 'SITE_COUNTRY']

# --- HELPER FUNCTIONS ---
def validate_and_clean_rpl(text):
    if pd.isna(text) or text == "":
        return "", None
    text = str(text).strip()
    # Typography
    replacements = {'“': '"', '”': '"', '’': "'", '‘': "'", '…': '...', '–': '-', '—': '-'}
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    # Validation
    open_tags = text.count('${')
    close_tags = text.count('}')
    if open_tags != close_tags:
        return text, f"CRITICAL: Mismatched braces. Found {open_tags} '${{' but {close_tags} '}}'."
    if re.search(r'\$\{\s+', text) or re.search(r'\s+\}', text):
        return text, "WARNING: Spaces detected inside RPL tag."
    return text, None

def clean_headers(df):
    df.columns = df.columns.str.strip()
    required_map = {
        'Priority': ['priority', 'id', 'prio'],
        'Description': ['desc', 'description', 'context'],
        'Module_Type': ['module', 'module_type', 'modul'],
        'DB_Field_Name': ['field', 'db_field_name', 'db_field']
    }
    new_columns = {}
    for col in df.columns:
        col_lower = col.lower()
        for standard_name, alternatives in required_map.items():
            if col_lower == standard_name.lower() or col_lower in alternatives:
                new_columns[col] = standard_name
    if new_columns:
        df.rename(columns=new_columns, inplace=True)
    return df

def process_file(uploaded_file, campaign_name):
    try:
        df = pd.read_excel(uploaded_file)
        df = clean_headers(df)
        
        meta_cols = ['Priority', 'Module_Type', 'DB_Field_Name', 'Description']
        missing = [c for c in meta_cols if c not in df.columns]
        if missing:
            return None, f"Error: Missing columns {missing}"

        lang_cols = [c for c in df.columns if c not in meta_cols]
        melted = df.melt(id_vars=meta_cols, value_vars=lang_cols, var_name='SITE_LANGUAGE', value_name='Content')
        
        errors = []
        for index, row in melted.iterrows():
            cleaned, error_msg = validate_and_clean_rpl(row['Content'])
            melted.at[index, 'Content'] = cleaned
            if error_msg:
                errors.append({'Lang': row['SITE_LANGUAGE'], 'Field': row['DB_Field_Name'], 'Error': error_msg, 'Content': row['Content']})
        
        if errors:
            return None, pd.DataFrame(errors)

        final_df = melted.pivot_table(
            index=['SITE_LANGUAGE', 'Priority', 'Module_Type'], 
            columns='DB_Field_Name', 
            values='Content', 
            aggfunc='first'
        ).reset_index()

        final_df['CAMPAIGN_NAME'] = campaign_name
        final_df['SITE_BRAND'] = 'ALL'
        final_df['SITE_COUNTRY'] = 'ALL'
        final_df.rename(columns={'Priority': 'PRIORITY', 'Module_Type': 'MODULE'}, inplace=True)
        
        return final_df, None
    except Exception as e:
        return None, str(e)

# --- APP LAYOUT ---
st.title("✉️ Responsys Content Generator")
st.write("Upload your copywriter Excel file to convert it for the database.")

# Inputs
campaign_name = st.text_input("Campaign Name", value="NF_Campaign_Name_Here")
uploaded_file = st.file_uploader("Choose Excel File", type=['xlsx'])

if uploaded_file and campaign_name:
    if st.button("Process File"):
        with st.spinner("Processing..."):
            result_df, error_result = process_file(uploaded_file, campaign_name)
            
            if isinstance(error_result, pd.DataFrame):
                st.error("⛔ Found Syntax Errors! Please fix these in Excel.")
                st.dataframe(error_result)
            elif isinstance(error_result, str):
                st.error(error_result)
            else:
                st.success("✅ Success! File generated.")
                
                # Convert to CSV for download
                # NEW LINE (Fixes Swedish characters)
                csv = result_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="Download CSV for Responsys",
                    data=csv,
                    file_name="upload_to_responsys.csv",
                    mime="text/csv"

                )
