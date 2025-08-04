import sqlite3

def db_init():
    """Initialize SQLite database for account mappings"""
    conn = sqlite3.connect('bac_accounts.db')

    # Create BAC account mappings table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS account_mapping (
            account_number TEXT PRIMARY KEY,
            friendly_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Create SINPE account mappings table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS sinpe_account_mapping (
            account_number TEXT PRIMARY KEY,
            friendly_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()

def db_add_account_mapping(account_number, friendly_name):
    """Add or update BAC account mapping"""
    conn = sqlite3.connect('bac_accounts.db')
    conn.execute('''
        INSERT OR REPLACE INTO account_mapping (account_number, friendly_name, last_used)
        VALUES (?, ?, CURRENT_TIMESTAMP)
    ''', (account_number, friendly_name))
    conn.commit()
    conn.close()

def db_add_sinpe_account_mapping(account_number, friendly_name):
    """Add or update SINPE account mapping"""
    conn = sqlite3.connect('bac_accounts.db')
    conn.execute('''
        INSERT OR REPLACE INTO sinpe_account_mapping (account_number, friendly_name, last_used)
        VALUES (?, ?, CURRENT_TIMESTAMP)
    ''', (account_number, friendly_name))
    conn.commit()
    conn.close()

def db_get_account_mappings():
    """Get all BAC account mappings as dictionary"""
    db_init()  # Ensure DB exists
    conn = sqlite3.connect('bac_accounts.db')
    cursor = conn.execute('SELECT account_number, friendly_name FROM account_mapping ORDER BY last_used DESC')
    mappings = dict(cursor.fetchall())
    conn.close()
    return mappings

def db_get_sinpe_account_mappings():
    """Get all SINPE account mappings as dictionary"""
    db_init()  # Ensure DB exists
    conn = sqlite3.connect('bac_accounts.db')
    cursor = conn.execute('SELECT account_number, friendly_name FROM sinpe_account_mapping ORDER BY last_used DESC')
    mappings = dict(cursor.fetchall())
    conn.close()
    return mappings

def db_delete_account_mapping(account_number):
    """Delete a BAC account mapping"""
    conn = sqlite3.connect('bac_accounts.db')
    conn.execute('DELETE FROM account_mapping WHERE account_number = ?', (account_number,))
    conn.commit()
    conn.close()

def db_delete_sinpe_account_mapping(account_number):
    """Delete a SINPE account mapping"""
    conn = sqlite3.connect('bac_accounts.db')
    conn.execute('DELETE FROM sinpe_account_mapping WHERE account_number = ?', (account_number,))
    conn.commit()
    conn.close()