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
    """
    Normalizes headers to standard names, even if there are typos.
    """
    df.columns = df.columns.str.strip()
    
    required_map = {
        'Priority': ['priority', 'id', 'prio'],
        'Description': ['desc', 'description', 'context'],
        'Module_Type': ['module', 'module_type', 'modul'],
        'DB_Field_Name': ['field', 'db_field_name', 'db_field'],
        # NEW: Mappings for your new columns
        'SITE_BRAND': ['site_brand', 'brand', 'brand_name'],
        'CAMPAIGN_NAME': ['campaign_name', 'campaign', 'camapign_name', 'camp_name'],
        'SITE_COUNTRY': ['site_country', 'country']
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

def process_file(uploaded_file, default_campaign_name):
    try:
        # Detect file type (CSV or Excel)
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
            
        df = clean_headers(df)
        
        # 1. DEFINE METADATA COLUMNS (Columns that are NOT languages)
        # We explicitly look for your new columns here
        possible_meta = ['Priority', 'Module_Type', 'DB_Field_Name', 'Description', 'SITE_BRAND', 'CAMPAIGN_NAME', 'SITE_COUNTRY']
        
        # Find which metadata columns actually exist in this file
        existing_meta = [c for c in possible_meta if c in df.columns]
        
        # Validation: Must have at least the basics
        critical_cols = ['Priority', 'Module_Type', 'DB_Field_Name']
        missing = [c for c in critical_cols if c not in df.columns]
        if missing:
            return None, f"Error: Missing critical columns {missing}"

        # 2. IDENTIFY LANGUAGES (Anything that isn't in the metadata list)
        lang_cols = [c for c in df.columns if c not in existing_meta]
        
        # 3. MELT
        melted = df.melt(id_vars=existing_meta, value_vars=lang_cols, var_name='SITE_LANGUAGE', value_name='Content')
        
        # 4. CLEAN RPL
        errors = []
        for index, row in melted.iterrows():
            cleaned, error_msg = validate_and_clean_rpl(row['Content'])
            melted.at[index, 'Content'] = cleaned
            if error_msg:
                errors.append({'Lang': row['SITE_LANGUAGE'], 'Field': row['DB_Field_Name'], 'Error': error_msg, 'Content': row['Content']})
        
        if errors:
            return None, pd.DataFrame(errors)

        # 5. PIVOT (Include new columns in the index so they stay with the row)
        # We assume Brand/Campaign are constant for the Priority group
        pivot_index = ['SITE_LANGUAGE', 'Priority', 'Module_Type']
        
        # If Brand/Campaign exist in file, add them to grouping index
        if 'SITE_BRAND' in df.columns: pivot_index.append('SITE_BRAND')
        if 'CAMPAIGN_NAME' in df.columns: pivot_index.append('CAMPAIGN_NAME')
        if 'SITE_COUNTRY' in df.columns: pivot_index.append('SITE_COUNTRY')

        final_df = melted.pivot_table(
            index=pivot_index, 
            columns='DB_Field_Name', 
            values='Content', 
            aggfunc='first'
        ).reset_index()

        # 6. FILL MISSING DATA (Fallback)
        # If the file didn't have these columns, use defaults
        if 'CAMPAIGN_NAME' not in final_df.columns:
            final_df['CAMPAIGN_NAME'] = default_campaign_name
        if 'SITE_BRAND' not in final_df.columns:
            final_df['SITE_BRAND'] = 'ALL'
        if 'SITE_COUNTRY' not in final_df.columns:
            final_df['SITE_COUNTRY'] = 'ALL'
            
        # Rename for Responsys Schema
        final_df.rename(columns={'Priority': 'PRIORITY', 'Module_Type': 'MODULE'}, inplace=True)
        
        # 7. EXPORT WITH UTF-8-SIG (Fixes Swedish characters)
        csv_bytes = final_df.to_csv(index=False).encode('utf-8-sig')
        
        return csv_bytes, None
    except Exception as e:
        return None, str(e)

# --- APP LAYOUT ---
st.title("✉️ Responsys Content Generator")
st.write("Upload your file. The tool now supports SITE_BRAND and CAMPAIGN_NAME columns automatically.")

# Inputs
campaign_name_input = st.text_input("Default Campaign Name (Used if not in Excel)", value="NF_Campaign_Name")
uploaded_file = st.file_uploader("Choose File (Excel or CSV)", type=['xlsx', 'csv'])

if uploaded_file:
    if st.button("Process File"):
        with st.spinner("Processing..."):
            csv_data, error_result = process_file(uploaded_file, campaign_name_input)
            
            if isinstance(error_result, pd.DataFrame):
                st.error("⛔ Found Syntax Errors! Please fix these in the file.")
                st.dataframe(error_result)
            elif isinstance(error_result, str):
                st.error(error_result)
            else:
                st.success("✅ Success! File generated.")
                st.download_button(
                    label="Download CSV for Responsys",
                    data=csv_data,
                    file_name="upload_to_responsys.csv",
                    mime="text/csv"
                )
