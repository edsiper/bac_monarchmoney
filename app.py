import streamlit as st
import pandas as pd
import io
import time
import re
from datetime import datetime
from database import (
    db_init, db_add_account_mapping, db_get_account_mappings, db_delete_account_mapping,
    db_add_sinpe_account_mapping, db_get_sinpe_account_mappings, db_delete_sinpe_account_mapping
)

def detect_tef_accounts(df):
    """Detect all 'TEF A: NUMBER' patterns in transaction descriptions"""
    tef_accounts = set()
    # More flexible pattern to handle various spacing: "TEF A:", "TEF  A :", "TEF A :", etc.
    tef_pattern = r'TEF\s+A\s*:\s*(\d+)'

    # Look in the merchant/description column
    merchant_col = None
    for col in ['Merchant', 'Descripción de Transacción', 'Description']:
        if col in df.columns:
            merchant_col = col
            break

    if merchant_col:
        for desc in df[merchant_col].astype(str):
            matches = re.findall(tef_pattern, desc)
            for match in matches:
                tef_accounts.add(match.strip())

    return tef_accounts

def detect_sinpe_accounts(df):
    """Detect all 'SINPE A' patterns in transaction descriptions"""
    sinpe_accounts = set()

    # Look in the merchant/description column
    merchant_col = None
    for col in ['Merchant', 'Descripción de Transacción', 'Description']:
        if col in df.columns:
            merchant_col = col
            break

    if merchant_col:
        for desc in df[merchant_col].astype(str):
            # Look for both SINPE patterns
            if desc.startswith('CD SINPE A ') or desc.startswith('PIN-SINPE A:'):
                # Find the position of either pattern
                start_idx = desc.find('CD SINPE A ')
                if start_idx == -1:
                    start_idx = desc.find('PIN-SINPE A:')

                if start_idx != -1:
                    # Extract everything after the pattern
                    if desc.startswith('CD SINPE A '):
                        sinpe_text = desc[start_idx + 11:].strip()  # "CD SINPE A " is 11 chars
                    else:  # PIN-SINPE A:
                        sinpe_text = desc[start_idx + 12:].strip()  # "PIN-SINPE A:" is 12 chars

                    # Find the end of the SINPE account (before any additional text)
                    end_idx = sinpe_text.find(' ')
                    if end_idx != -1:
                        sinpe_account = sinpe_text[:end_idx]
                    else:
                        sinpe_account = sinpe_text
                    sinpe_accounts.add(sinpe_account)

    return sinpe_accounts

def apply_account_mappings(df, bac_mappings, sinpe_mappings):
    """Replace 'TEF A: NUMBER' and 'SINPE A' with friendly names in merchant column"""
    def replace_transfers(description):
        if pd.isna(description):
            return description

        desc_str = str(description)

        # Replace TEF A: patterns
        tef_pattern = r'TEF\s+A\s*:\s*(\d+)'
        def replace_tef(match):
            account_num = match.group(1).strip()
            if account_num in bac_mappings:
                return f"{bac_mappings[account_num]} - BAC:{account_num}"
            else:
                return match.group(0)  # Return original if no mapping

        desc_str = re.sub(tef_pattern, replace_tef, desc_str)

        # Replace SINPE A patterns - simple string replacement
        if 'CD SINPE A ' in desc_str:
            for sinpe_account, friendly_name in sinpe_mappings.items():
                if sinpe_account in desc_str:
                    desc_str = desc_str.replace(f'CD SINPE A {sinpe_account}', f'{friendly_name} - SINPE:{sinpe_account}')

        return desc_str

    # Apply to the merchant column
    merchant_col = None
    for col in ['Merchant', 'Descripción de Transacción', 'Description']:
        if col in df.columns:
            merchant_col = col
            break

    if merchant_col:
        df[merchant_col] = df[merchant_col].apply(replace_transfers)

    return df

def parse_bac_csv(file_content):
    """
    Parse BAC CSV with its specific multi-section format
    """
    lines = file_content.split('\n')

    # Find the transaction headers line
    transaction_headers_line = -1
    transaction_data_start = -1

    for i, line in enumerate(lines):
        if 'Fecha de Transacción' in line:
            transaction_headers_line = i
            transaction_data_start = i + 1
            break

    if transaction_headers_line == -1:
        # Fallback: look for other possible header patterns
        for i, line in enumerate(lines):
            if 'Fecha' in line and ('Descripción' in line or 'Descripcion' in line):
                transaction_headers_line = i
                transaction_data_start = i + 1
                break

    if transaction_headers_line == -1:
        raise ValueError("Could not find transaction headers in the CSV file")

    # Find where transaction data ends (before summary section)
    transaction_data_end = len(lines)
    for i, line in enumerate(lines[transaction_data_start:], transaction_data_start):
        if ('Resumen de Estado Bancario' in line or
            'Código Transacción Totales' in line or
            line.strip() == '' and i > transaction_data_start + 10):  # Empty line after significant data
            transaction_data_end = i
            break

    # Extract headers and clean them
    headers_line = lines[transaction_headers_line].strip()
    headers = [h.strip() for h in headers_line.split(',')]

    # Extract transaction data
    transaction_lines = []
    for i in range(transaction_data_start, transaction_data_end):
        line = lines[i].strip()
        if line and not line.startswith('Resumen') and not line.startswith('Código'):
            transaction_lines.append(line)

    # Create CSV content for pandas
    csv_content = headers_line + '\n' + '\n'.join(transaction_lines)

    # Parse with pandas
    df = pd.read_csv(io.StringIO(csv_content), sep=',', skipinitialspace=True)

    return df, len(transaction_lines)

def convert_bac_to_monarch_format(df, import_id, bac_mappings, sinpe_mappings):
    """
    Convert BAC transaction data to Monarch Money format
    Monarch Money format: Date, Merchant, Category, Account, Original Statement, Notes, Amount, Tags
    """
    if df.empty:
        return pd.DataFrame()

    # Apply account mappings to replace TEF A: NUMBER with friendly names
    df_mapped = apply_account_mappings(df.copy(), bac_mappings, sinpe_mappings)

    # Create a copy to work with
    result_df = df_mapped.copy()

    # Map column names to standard format
    column_mapping = {
        'Fecha de Transacción': 'Date',
        'Fecha de Transaccion': 'Date',
        'Fecha': 'Date',
        'Descripción de Transacción': 'Merchant',
        'Descripcion de Transaccion': 'Merchant',
        'Descripción': 'Merchant',
        'Descripcion': 'Merchant',
        'Débito de Transacción': 'Debit',
        'Debito de Transaccion': 'Debit',
        'Débito': 'Debit',
        'Debito': 'Debit',
        'Crédito de Transacción': 'Credit',
        'Credito de Transaccion': 'Credit',
        'Crédito': 'Credit',
        'Credito': 'Credit'
    }

    # Rename columns
    for old_name, new_name in column_mapping.items():
        if old_name in result_df.columns:
            result_df = result_df.rename(columns={old_name: new_name})

    # Ensure we have the required columns
    required_cols = ['Date', 'Merchant']
    missing_cols = [col for col in required_cols if col not in result_df.columns]
    if missing_cols:
        st.error(f"Missing columns after mapping: {missing_cols}")
        st.info(f"Available columns: {list(result_df.columns)}")
        return pd.DataFrame()

    # Clean and convert date format
    if 'Date' in result_df.columns:
        # Convert DD/MM/YYYY to YYYY-MM-DD
        result_df['Date'] = pd.to_datetime(
            result_df['Date'],
            format='%d/%m/%Y',
            errors='coerce'
        ).dt.strftime('%Y-%m-%d')

        # Remove rows with invalid dates
        result_df = result_df.dropna(subset=['Date'])

    # Create Amount column from Debit/Credit
    if 'Debit' in result_df.columns and 'Credit' in result_df.columns:
        # Clean the debit and credit columns
        result_df['Debit'] = result_df['Debit'].astype(str).str.replace(',', '').str.strip()
        result_df['Credit'] = result_df['Credit'].astype(str).str.replace(',', '').str.strip()

        # Convert to numeric
        result_df['Debit'] = pd.to_numeric(result_df['Debit'], errors='coerce').fillna(0)
        result_df['Credit'] = pd.to_numeric(result_df['Credit'], errors='coerce').fillna(0)

        # Create Amount column (negative for debits, positive for credits)
        result_df['Amount'] = result_df['Credit'] - result_df['Debit']
    else:
        st.error("Could not find Debit/Credit columns to create Amount")
        return pd.DataFrame()

    # Clean merchant name
    if 'Merchant' in result_df.columns:
        result_df['Merchant'] = result_df['Merchant'].astype(str).str.strip()

    # Create Monarch Money format columns
    monarch_df = pd.DataFrame({
        'Date': result_df['Date'],
        'Merchant': result_df['Merchant'],
        'Category': '',  # Always empty
        'Account': 'BAC',  # Always BAC
        'Original Statement': '',  # Always empty
        'Notes': f'id={import_id}',  # import=unix_timestamp
        'Amount': result_df['Amount'],
        'Tags': ''  # Always empty
    })

    # Remove any rows with NaN values in critical columns
    monarch_df = monarch_df.dropna(subset=['Date', 'Merchant', 'Amount'])

    return monarch_df

def main():
    st.title("BAC to Monarch Money CSV Converter")
    st.write("Upload your BAC bank statement CSV file to convert it to Monarch Money format.")

    # Initialize database
    db_init()

    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        # Generate import ID based on current unix timestamp
        import_id = int(time.time())
        st.info(f"🆔 Import ID generated: {import_id}")

        try:
            # Try different encodings common in Latin America
            encodings = ['cp1252', 'windows-1252', 'latin-1', 'iso-8859-1', 'utf-8']
            file_content = None
            used_encoding = None

            for encoding in encodings:
                try:
                    uploaded_file.seek(0)  # Reset file pointer
                    file_content = uploaded_file.read().decode(encoding)
                    used_encoding = encoding
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    st.warning(f"Failed to read with {encoding}: {str(e)}")
                    continue

            if file_content is None:
                st.error("Could not read the CSV file with any supported encoding.")
                return

            # Parse the BAC CSV format
            try:
                df, num_transactions = parse_bac_csv(file_content)
                st.success(f"✅ Successfully parsed BAC CSV format. Found {num_transactions} transactions.")

                # Show transaction summary immediately in sidebar
                with st.sidebar:
                    st.header("💰 Transaction Summary")
                    # Calculate basic stats from original data
                    if 'Débito de Transacción' in df.columns or 'Debito de Transaccion' in df.columns:
                        debit_col = 'Débito de Transacción' if 'Débito de Transacción' in df.columns else 'Debito de Transaccion'
                        df[debit_col] = df[debit_col].astype(str).str.replace(',', '').str.strip()
                        df[debit_col] = pd.to_numeric(df[debit_col], errors='coerce').fillna(0)
                        total_debits = df[debit_col].sum()
                        st.metric("Total Debits", f"${total_debits:,.2f}")

                    if 'Crédito de Transacción' in df.columns or 'Credito de Transaccion' in df.columns:
                        credit_col = 'Crédito de Transacción' if 'Crédito de Transacción' in df.columns else 'Credito de Transaccion'
                        df[credit_col] = df[credit_col].astype(str).str.replace(',', '').str.strip()
                        df[credit_col] = pd.to_numeric(df[credit_col], errors='coerce').fillna(0)
                        total_credits = df[credit_col].sum()
                        st.metric("Total Credits", f"${total_credits:,.2f}")

                    if 'total_debits' in locals() and 'total_credits' in locals():
                        net_change = total_credits - total_debits
                        st.metric("Net Change", f"${net_change:,.2f}")

            except Exception as e:
                st.error(f"Error parsing BAC CSV format: {str(e)}")
                st.info("The file doesn't appear to be in the expected BAC format.")
                return

            # ===== DETECT TEF ACCOUNTS AND SHOW MAPPING MODAL =====
            detected_bac_accounts = detect_tef_accounts(df)
            detected_sinpe_accounts = detect_sinpe_accounts(df)
            current_bac_mappings = db_get_account_mappings()
            current_sinpe_mappings = db_get_sinpe_account_mappings()

            if detected_bac_accounts or detected_sinpe_accounts:
                st.info(f"🏦 Found {len(detected_bac_accounts)} BAC account(s) and {len(detected_sinpe_accounts)} SINPE account(s) in your transactions")

                # Show mapping interface
                st.write("### 🔄 Account Mapping")
                st.write("Add friendly names to your account numbers to make your transactions easier to understand.")

                # BAC Account Mappings
                if detected_bac_accounts:
                    st.write("**🏦 BAC to BAC Transfers**")
                    for account in sorted(detected_bac_accounts):
                        col1, col2, col3 = st.columns([1, 2, 0.3])

                        with col1:
                            st.text_input(
                                "BAC Account",
                                value=account,
                                disabled=True,
                                key=f"bac_account_{account}",
                                label_visibility="collapsed"
                            )

                        with col2:
                            # Pre-fill with existing mapping if available
                            default_value = current_bac_mappings.get(account, "")
                            friendly_name = st.text_input(
                                "Friendly Name",
                                value=default_value,
                                placeholder="e.g., Mom, John Doe, Sister Maria",
                                key=f"bac_name_{account}",
                                label_visibility="collapsed"
                            )

                        with col3:
                            if st.button("📋", key=f"bac_copy_{account}", help="Copy account number"):
                                st.rerun()

                # SINPE Account Mappings
                if detected_sinpe_accounts:
                    st.write("**💳 SINPE Transfers (Bank to Bank)**")
                    for account in sorted(detected_sinpe_accounts):
                        col1, col2, col3 = st.columns([1, 2, 0.3])

                        with col1:
                            st.text_input(
                                "SINPE Account",
                                value=account,
                                disabled=True,
                                key=f"sinpe_account_{account}",
                                label_visibility="collapsed"
                            )

                        with col2:
                            # Pre-fill with existing mapping if available
                            default_value = current_sinpe_mappings.get(account, "")
                            friendly_name = st.text_input(
                                "Friendly Name",
                                value=default_value,
                                placeholder="e.g., Mom, John Doe, Sister Maria",
                                key=f"sinpe_name_{account}",
                                label_visibility="collapsed"
                            )

                        with col3:
                            if st.button("📋", key=f"sinpe_copy_{account}", help="Copy account number"):
                                st.rerun()

                # Check for changes and auto-save
                for account in sorted(detected_bac_accounts):
                    current_value = st.session_state.get(f"bac_name_{account}", "")
                    if current_value.strip() and current_value != current_bac_mappings.get(account, ""):
                        db_add_account_mapping(account, current_value.strip())
                        if f"bac_auto_saved_{account}" not in st.session_state:
                            st.success(f"✅ Auto-saved BAC mapping for {account}")
                            st.session_state[f"bac_auto_saved_{account}"] = True

                for account in sorted(detected_sinpe_accounts):
                    current_value = st.session_state.get(f"sinpe_name_{account}", "")
                    if current_value.strip() and current_value != current_sinpe_mappings.get(account, ""):
                        db_add_sinpe_account_mapping(account, current_value.strip())
                        if f"sinpe_auto_saved_{account}" not in st.session_state:
                            st.success(f"✅ Auto-saved SINPE mapping for {account}")
                            st.session_state[f"sinpe_auto_saved_{account}"] = True

                # Proceed button
                if st.button("➡️ Continue to Conversion", type="primary"):
                    st.session_state.mapping_complete = True
                    st.session_state.use_mappings = True
                    st.rerun()

            else:
                # No accounts detected, proceed normally
                st.session_state.mapping_complete = True
                st.session_state.use_mappings = True

            # ===== PROCESS CSV ONLY AFTER MAPPING IS COMPLETE =====
            if st.session_state.get('mapping_complete', False):
                # Get final mappings based on user choice
                if st.session_state.get('use_mappings', True):
                    final_bac_mappings = db_get_account_mappings()
                    final_sinpe_mappings = db_get_sinpe_account_mappings()
                else:
                    final_bac_mappings = {}  # Empty mappings = keep original format
                    final_sinpe_mappings = {}

                # Convert to Monarch Money format
                result_df = convert_bac_to_monarch_format(df, import_id, final_bac_mappings, final_sinpe_mappings)

                if not result_df.empty:
                    st.write("### 💰 Converted Data Preview (Monarch Money Format):")
                    st.dataframe(result_df.head(10))

                    st.write(f"✅ Successfully converted {len(result_df)} transactions")

                    # Show mapping results
                    if detected_bac_accounts and final_bac_mappings:
                        mapped_count = len([acc for acc in detected_bac_accounts if acc in final_bac_mappings])
                        st.info(f"🔄 {mapped_count} of {len(detected_bac_accounts)} BAC accounts mapped to friendly names")

                        # Show sample mapped transactions
                        bac_transactions = result_df[result_df['Merchant'].str.contains('Transfer to.*- BAC:', na=False)]
                        if not bac_transactions.empty:
                            st.write("### 🔄 Sample Mapped BAC Transactions:")
                            st.dataframe(bac_transactions[['Date', 'Merchant', 'Amount']].head(5))

                    if detected_sinpe_accounts and final_sinpe_mappings:
                        mapped_count = len([acc for acc in detected_sinpe_accounts if acc in final_sinpe_mappings])
                        st.info(f"🔄 {mapped_count} of {len(detected_sinpe_accounts)} SINPE accounts mapped to friendly names")

                        # Show sample mapped transactions
                        sinpe_transactions = result_df[result_df['Merchant'].str.contains('Transfer to.*- SINPE:', na=False)]
                        if not sinpe_transactions.empty:
                            st.write("### 🔄 Sample Mapped SINPE Transactions:")
                            st.dataframe(sinpe_transactions[['Date', 'Merchant', 'Amount']].head(5))

                    # Download button - NO HEADERS as required by Monarch Money
                    csv = result_df.to_csv(index=False, header=False)
                    st.download_button(
                        label="📥 Download CSV for Monarch Money (No Headers)",
                        data=csv,
                        file_name=f"monarch_money_import_{import_id}.csv",
                        mime="text/csv"
                    )

                    st.success("🎉 Ready to import into Monarch Money!")
                    st.info("💡 The CSV file has no headers as required by Monarch Money's import format.")

                    # Reset session state for next upload
                    if st.button("🔄 Upload Another CSV"):
                        for key in ['mapping_complete', 'use_mappings']:
                            if key in st.session_state:
                                del st.session_state[key]
                        st.rerun()

                else:
                    st.error("Conversion failed. Please check the CSV file format.")

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.write("Please make sure you uploaded a valid BAC bank statement CSV file.")

if __name__ == "__main__":
    main()

