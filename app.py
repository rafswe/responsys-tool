import streamlit as st
import pandas as pd
import io
import re
import json
import csv 

# ==========================================
# 1. HELPER FUNCTIONS (Shared by both modes)
# ==========================================

def clean_headers(df):
    """ Normalizes headers, removing spaces, typos, and ghost columns. """
    # FIX 1: Force all headers to string to prevent "float is not iterable" crash
    df.columns = df.columns.astype(str).str.strip()
    
    # Drop purely empty/nan ghost columns
    df = df.loc[:, (df.columns != 'nan') & (df.columns != '')]
    
    required_map = {
        'Priority': ['priority', 'prio'], 
        'Description': ['desc', 'description', 'context'],
        'Module_Type': ['module', 'module_type', 'modul'],
        'DB_Field_Name': ['field', 'db_field_name', 'db_field'],
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

def validate_and_clean_rpl(text):
    """ Checks for RPL syntax errors and smart quotes. """
    if pd.isna(text) or str(text).strip() == "":
        return "", None
    text = str(text).strip()
    
    if text.upper() == '[EMPTY]':
        return "[EMPTY]", None
    
    replacements = {'“': '"', '”': '"', '’': "'", '‘': "'", '…': '...', '–': '-', '—': '-'}
    for bad, good in replacements.items():
        text = text.replace(bad, good)
        
    open_tags = text.count('${')
    close_tags = text.count('}')
    if open_tags != close_tags:
        return text, f"CRITICAL: Mismatched braces. Found {open_tags} '${{' but {close_tags} '}}'."
    if re.search(r'\$\{\s+', text) or re.search(r'\s+\}', text):
        return text, "WARNING: Spaces detected inside RPL tag."
        
    return text, None

def load_and_prep_data(uploaded_file):
    """ Smart loads Excel/CSV and identifies columns. """
    try:
        # A. Smart Load (Find Header Row)
        if uploaded_file.name.endswith('.xlsx') or uploaded_file.name.endswith('.xls'):
            xl_file = pd.ExcelFile(uploaded_file)
            preview = xl_file.parse(header=None, nrows=15)
            header_row_idx = 0
            for idx, row in preview.iterrows():
                row_str = [str(x).lower() for x in row.tolist()]
                if any('priority' in s for s in row_str) or any('db_field_name' in s for s in row_str):
                    header_row_idx = idx
                    break
            df = xl_file.parse(header=header_row_idx)
        else:
            # FIX 3: Smart loading for messy CSVs with metadata at the top
            try:
                preview = pd.read_csv(uploaded_file, nrows=15, sep=None, engine='python', header=None)
                header_row_idx = 0
                for idx, row in preview.iterrows():
                    row_str = [str(x).lower() for x in row.tolist()]
                    if any('priority' in s for s in row_str) or any('db_field_name' in s for s in row_str):
                        header_row_idx = idx
                        break
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, header=header_row_idx, sep=None, engine='python')
            except:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, sep=None, engine='python')

        # B. Clean Headers
        df = clean_headers(df)
        df = df.loc[:, ~df.columns.duplicated()]
        
        # C. Identify Metadata
        possible_meta = ['Priority', 'Module_Type', 'DB_Field_Name', 'Description', 'SITE_BRAND', 'CAMPAIGN_NAME', 'SITE_COUNTRY']
        existing_meta = [c for c in possible_meta if c in df.columns]
        
        critical_cols = ['Priority', 'Module_Type']
        missing = [c for c in critical_cols if c not in df.columns]
        if missing:
            return None, None, None, f"Error: Missing columns {missing}"

        # D. Identify Languages
        lang_cols = [c for c in df.columns if c not in existing_meta]
        lang_cols = [c for c in lang_cols if "unnamed" not in str(c).lower()]
        
        return df, existing_meta, lang_cols, None

    except Exception as e:
        return None, None, None, str(e)

# ==========================================
# 2. CORE LOGIC: CSV GENERATOR
# ==========================================
def generate_csv_logic(df, existing_meta, lang_cols, default_campaign, use_english_fallback):
    if use_english_fallback and 'EN' in df.columns:
        for lang in lang_cols:
            if lang != 'EN':
                df[lang] = df[lang].fillna(df['EN'])

    melted = df.melt(id_vars=existing_meta, value_vars=lang_cols, var_name='SITE_LANGUAGE', value_name='Content')
    
    # FIX 2: Smart Drop Logic for Structural Modules
    melted['Content'] = melted['Content'].fillna("")
    if 'DB_Field_Name' not in melted.columns: melted['DB_Field_Name'] = ""
    melted['DB_Field_Name'] = melted['DB_Field_Name'].fillna("")
    
    # Keep rows with text, OR rows that are structural (no DB field defined)
    melted = melted[(melted['Content'] != "") | (melted['DB_Field_Name'] == "")]

    errors = []
    for index, row in melted.iterrows():
        cleaned, error_msg = validate_and_clean_rpl(row['Content'])
        melted.at[index, 'Content'] = cleaned
        if error_msg:
            errors.append({'Lang': row['SITE_LANGUAGE'], 'Field': row['DB_Field_Name'], 'Error': error_msg, 'Content': row['Content']})
    
    if errors:
        return None, pd.DataFrame(errors)

    pivot_index = ['SITE_LANGUAGE', 'Priority', 'Module_Type']
    if 'SITE_BRAND' in df.columns: pivot_index.append('SITE_BRAND')
    if 'CAMPAIGN_NAME' in df.columns: pivot_index.append('CAMPAIGN_NAME')
    if 'SITE_COUNTRY' in df.columns: pivot_index.append('SITE_COUNTRY')

    final_df = melted.pivot_table(
        index=pivot_index, 
        columns='DB_Field_Name', 
        values='Content', 
        aggfunc='first'
    ).reset_index()

    # Clean up empty column generated by structural modules
    if "" in final_df.columns:
        final_df = final_df.drop(columns=[""])

    if 'CAMPAIGN_NAME' not in final_df.columns: final_df['CAMPAIGN_NAME'] = default_campaign
    if 'SITE_BRAND' not in final_df.columns: final_df['SITE_BRAND'] = 'ALL'
        
    final_df.rename(columns={'Priority': 'PRIORITY', 'Module_Type': 'MODULE'}, inplace=True)
    final_df = final_df.replace('[EMPTY]', '')
    final_df.fillna("", inplace=True)
    
    buffer = io.StringIO()
    final_df.to_csv(buffer, index=False, sep=',', quoting=csv.QUOTE_ALL)
    csv_bytes = buffer.getvalue().encode('utf-8-sig')
    
    return csv_bytes, None

# ==========================================
# 3. CORE LOGIC: JSON GENERATOR
# ==========================================
def generate_json_logic(df, existing_meta, lang_cols, default_campaign):
    errors = []
    for col in lang_cols:
        for idx, val in df[col].items():
            cleaned, error_msg = validate_and_clean_rpl(val)
            df.at[idx, col] = cleaned
            if error_msg:
                 errors.append({'Row': idx, 'Lang': col, 'Error': error_msg, 'Content': val})
    if errors:
        return None, pd.DataFrame(errors)

    melted = df.melt(id_vars=existing_meta, value_vars=lang_cols, var_name='SITE_LANGUAGE', value_name='Content')
    
    # FIX 2: Smart Drop Logic for Structural Modules
    melted['Content'] = melted['Content'].fillna("")
    if 'DB_Field_Name' not in melted.columns: melted['DB_Field_Name'] = ""
    melted['DB_Field_Name'] = melted['DB_Field_Name'].fillna("")
    
    # Keep rows with text, OR rows that are structural (no DB field defined)
    melted = melted[(melted['Content'] != "") | (melted['DB_Field_Name'] == "")]

    if 'CAMPAIGN_NAME' not in melted.columns: melted['CAMPAIGN_NAME'] = default_campaign
    if 'SITE_BRAND' not in melted.columns: melted['SITE_BRAND'] = 'ALL'
    
    grouped_payloads = {} 
    unique_keys = melted[['CAMPAIGN_NAME', 'Priority', 'Module_Type', 'SITE_LANGUAGE', 'SITE_BRAND']].drop_duplicates()
    
    for _, key_row in unique_keys.iterrows():
        camp = key_row['CAMPAIGN_NAME']
        prio = key_row['Priority']
        mod  = key_row['Module_Type']  
        lang = key_row['SITE_LANGUAGE']
        brand = key_row['SITE_BRAND']
        
        subset = melted[
            (melted['CAMPAIGN_NAME'] == camp) & 
            (melted['Priority'] == prio) & 
            (melted['Module_Type'] == mod) &
            (melted['SITE_LANGUAGE'] == lang)
        ]
        
        if subset.empty: continue

        active_fields = ['CAMPAIGN_NAME', 'PRIORITY', 'MODULE', 'SITE_LANGUAGE', 'SITE_BRAND']
        record_values = [str(camp), str(prio), str(mod), str(lang), str(brand)]
        
        for _, row in subset.iterrows():
            field = str(row['DB_Field_Name']).strip()
            content = row['Content']
            
            # Skip mapping if it's a structural module with no field
            if field == "":
                continue
                
            if content == '[EMPTY]':
                content = ""
                
            active_fields.append(field)
            record_values.append(str(content))
            
        shape_signature = tuple(active_fields)
        if shape_signature not in grouped_payloads:
            grouped_payloads[shape_signature] = []
        grouped_payloads[shape_signature].append(record_values)

    json_outputs = []
    for shape, records in grouped_payloads.items():
        payload = {
            "recordData": {
                "fieldNames": list(shape),
                "records": records
            },
            "insertOnNoMatch": True,
            "updateOnMatch": "REPLACE_ALL"
        }
        json_outputs.append(payload)
        
    return json_outputs, None

# ==========================================
# 4. APP LAYOUT
# ==========================================
st.set_page_config(page_title="Responsys Tools", page_icon="✉️")
st.title("✉️ Responsys Content Tools")

tab1, tab2 = st.tabs(["📂 1. Create CSV (Connect Job)", "🚀 2. Create JSON (Postman API)"])

with st.sidebar:
    st.header("Input Data")
    uploaded_file = st.file_uploader("Upload Excel / CSV", type=['xlsx', 'xls', 'csv'])
    default_campaign = st.text_input("Default Campaign Name", "NF_Campaign_Name")
    st.info("Ensure your file has: Priority, Module_Type, DB_Field_Name (Leave DB_Field_Name blank for structural modules like dividers)")

with tab1:
    st.header("Generate CSV for Connect Job")
    st.write("Best for **New Campaigns** or massive updates.")
    use_fallback = st.checkbox("New Campaign Mode: Fill empty translations with English?", value=True)

    if uploaded_file:
        if st.button("Generate CSV", key="btn_csv"):
            with st.spinner("Processing CSV..."):
                df, meta, langs, err = load_and_prep_data(uploaded_file)
                if err:
                    st.error(err)
                else:
                    csv_data, error_df = generate_csv_logic(df, meta, langs, default_campaign, use_fallback)
                    if error_df is not None:
                        st.error("⛔ Syntax Errors Found!")
                        st.dataframe(error_df)
                    else:
                        st.success("✅ CSV Ready!")
                        st.download_button("Download upload_to_responsys.csv", data=csv_data, file_name="upload_to_responsys.csv", mime="text/csv")
    else:
        st.warning("👈 Please upload an Excel or CSV file in the sidebar to get started.")

with tab2:
    st.header("Generate JSON for API")
    st.write("Best for **Partial Updates** (fixing typos without overwriting other fields).")
    if uploaded_file:
        if st.button("Generate JSONs", key="btn_json"):
            with st.spinner("Calculating payloads..."):
                df, meta, langs, err = load_and_prep_data(uploaded_file)
                if err:
                    st.error(err)
                else:
                    json_list, error_df = generate_json_logic(df, meta, langs, default_campaign)
                    if error_df is not None:
                        st.error("⛔ Syntax Errors Found!")
                        st.dataframe(error_df)
                    else:
                        total_payloads = len(json_list)
                        st.success(f"✅ Generated {total_payloads} unique payloads.")
                        for i, payload in enumerate(json_list):
                            fields = ", ".join(payload['recordData']['fieldNames'][4:]) 
                            st.markdown("---")
                            st.subheader(f"🚀 Payload {i+1} of {total_payloads}")
                            st.caption(f"**Fields updating:** {fields if fields else 'STRUCTURAL MODULE ONLY (No Content)'}")
                            st.code(json.dumps(payload, indent=2), language='json')
    else:
        st.warning("👈 Please upload an Excel or CSV file in the sidebar to get started.")
