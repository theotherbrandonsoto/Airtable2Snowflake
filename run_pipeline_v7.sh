#!/bin/bash

# Pipeline script for Airtable to Snowflake data transfer
# This script orchestrates the entire process of:
# 1. Downloading data from Airtable using airtable_export.rb
# 2. Processing the data using translate_codes.py
# 3. Uploading to Snowflake
# 4. Cleaning up temporary files

# Exit on any error
set -e

echo "=== Starting Airtable to Snowflake Pipeline ==="
echo "Started at: $(date)"

# Create necessary directories if they don't exist
mkdir -p logs
mkdir -p backup
mkdir -p data

# Function to log messages with timestamps
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to clean up data directory
cleanup_data() {
    local status=$1
    if [ "$status" -eq 0 ]; then
        # If pipeline was successful, backup the last successful run
        log_message "Backing up successful run data..."
        timestamp=$(date +%Y%m%d_%H%M%S)
        if [ -f "data/snowflake_import.csv" ]; then
            cp data/snowflake_import.csv "backup/snowflake_import_${timestamp}.csv"
            log_message "Backup created: backup/snowflake_import_${timestamp}.csv"
        fi
        
        # Clean up data directory
        log_message "Cleaning up data directory..."
        rm -rf data/*
        log_message "Data directory cleaned"
        
        # Keep only the last 5 backups
        log_message "Maintaining backup history..."
        cd backup && ls -t snowflake_import_*.csv | tail -n +6 | xargs rm -f 2>/dev/null || true
        cd ..
    else
        log_message "Pipeline failed - keeping data directory for debugging"
    fi
}

# Trap to ensure cleanup runs on script exit
trap 'cleanup_data $?' EXIT

# Clear data directory before starting
log_message "Clearing data directory before starting..."
rm -rf data/*
mkdir -p data

# 1. Download data from Airtable
log_message "Step 1: Downloading data from Airtable..."
ruby airtable_export.rb 2>&1 | tee logs/airtable_export.log
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    log_message "ERROR: Airtable export failed"
    exit 1
fi

# 2. Process the data using translate_codes.py
log_message "Step 2: Processing data using translate_codes.py..."
python translate_codes.py 2>&1 | tee logs/translate_codes.log
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    log_message "ERROR: Code translation failed"
    exit 1
fi

# 3. Upload to Snowflake
log_message "Step 3: Uploading to Snowflake..."

# First, create a Python script for the upload
cat > upload_to_snowflake.py << 'EOL'
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def upload_to_snowflake():
    try:
        # Read the CSV file
        print("Reading CSV file...")
        df = pd.read_csv('data/snowflake_import.csv')
        
        # Create Snowflake connection
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            authenticator='externalbrowser',
            account='SQUAREINC-SQUARE',
            user='bsoto@squareup.com',
            database='APP_CASH_BETA',
            schema='BSOTO',
            role='BSOTO'
        )
        
        # Create cursor
        cur = conn.cursor()
        
        # Create the SUBSTANTIATIONS_RAW table
        print("Creating/replacing SUBSTANTIATIONS_RAW table...")
        create_table_sql = """
        CREATE OR REPLACE TABLE APP_CASH_BETA.BSOTO.SUBSTANTIATIONS_RAW (
            "Complaint Number" VARCHAR,
            "Assignee" VARCHAR,
            "Review Status" VARCHAR,
            "Case Number" FLOAT,
            "Complaint Link" VARCHAR,
            "Primary Issue Root Cause" VARCHAR,
            "Codes" VARCHAR,
            "Product Type Sub Category" VARCHAR,
            "Insights Review Notes" VARCHAR,
            "Created" VARCHAR,
            "Last Modified" VARCHAR,
            "Complaint Statements" VARCHAR,
            "Substantiation" VARCHAR,
            "Redress Required" VARCHAR,
            "Regulatory Case Spend" VARCHAR,
            "Month Closed" VARCHAR,
            "Product Type" VARCHAR,
            "Primary Issue" VARCHAR,
            "Spirit Bucket" VARCHAR,
            "Workflow" VARCHAR,
            "Resulting Action" VARCHAR,
            "Code Category" VARCHAR,
            "Secondary Issue" VARCHAR,
            "Secondary Issue Root Cause" VARCHAR,
            "Redress Requested" VARCHAR,
            "Review Completed At" VARCHAR,
            "Redress Paid" VARCHAR,
            "Edge Case Fields" VARCHAR,
            "Edge Case Notes" VARCHAR,
            "Edge Case" VARCHAR,
            "Edge Case Reviewed" VARCHAR,
            "Required Fields Complete" VARCHAR
        )
        """
        cur.execute(create_table_sql)
        
        # Upload the data to RAW table
        print("Uploading data to SUBSTANTIATIONS_RAW...")
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name='SUBSTANTIATIONS_RAW',
            schema='BSOTO',
            database='APP_CASH_BETA'
        )
        
        print(f"Raw table upload completed successfully: {success}")
        print(f"Number of rows uploaded: {nrows}")
        
        # Create SUBSTANTIATIONS_REVISED
        print("\nCreating SUBSTANTIATIONS_REVISED...")
        cur.execute("""
        CREATE OR REPLACE TABLE APP_CASH_BETA.BSOTO.SUBSTANTIATIONS_REVISED AS
        SELECT *
        FROM APP_CASH_BETA.BSOTO.SUBSTANTIATIONS_RAW
        """)
        
        # Create SUBSTANTIATIONS_CODED
        print("Creating SUBSTANTIATIONS_CODED...")
        cur.execute("""
        CREATE OR REPLACE TABLE APP_CASH_BETA.BSOTO.SUBSTANTIATIONS_CODED AS
        WITH SPLIT_CODES AS (
            SELECT 
                "Complaint Number",
                TRIM(value::VARCHAR, '[]" ') as code,
                CASE 
                    WHEN value::VARCHAR LIKE '01.%' THEN '01. Service and Support Issues'
                    WHEN value::VARCHAR LIKE '02.%' THEN '02. Transaction and Processing Issues'
                    WHEN value::VARCHAR LIKE '03.%' THEN '03. Account Access and Security'
                    WHEN value::VARCHAR LIKE '04.%' THEN '04. Product Features and Functionality'
                    WHEN value::VARCHAR LIKE '05.%' THEN '05. Fees and Charges'
                    WHEN value::VARCHAR LIKE '06.%' THEN '06. Security and Fraud Concerns'
                    WHEN value::VARCHAR LIKE '07.%' THEN '07. Technical Issues'
                    WHEN value::VARCHAR LIKE '08.%' THEN '08. Compliance and Regulatory'
                    WHEN value::VARCHAR LIKE '09.%' THEN '09. Third Party Integration'
                    WHEN value::VARCHAR LIKE '10.%' THEN '10. User Experience and Expectations'
                    WHEN value::VARCHAR LIKE '11.%' THEN '11. Marketing and Communications'
                    WHEN value::VARCHAR LIKE '12.%' THEN '12. Documentation and Procedure'
                    WHEN value::VARCHAR LIKE '13.%' THEN '13. Privacy and Data Protection'
                    ELSE 'Uncategorized'
                END as code_category
            FROM APP_CASH_BETA.BSOTO.SUBSTANTIATIONS_RAW,
            LATERAL FLATTEN(input=>PARSE_JSON("Codes"))
        )
        SELECT DISTINCT
            "Complaint Number",
            code,
            code_category
        FROM SPLIT_CODES
        WHERE code IS NOT NULL
        """)
        
        # Verify the results
        print("\nVerifying results...")
        cur.execute('SELECT COUNT(*) FROM APP_CASH_BETA.BSOTO.SUBSTANTIATIONS_RAW')
        raw_count = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(*) FROM APP_CASH_BETA.BSOTO.SUBSTANTIATIONS_REVISED')
        revised_count = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(*) FROM APP_CASH_BETA.BSOTO.SUBSTANTIATIONS_CODED')
        coded_count = cur.fetchone()[0]
        
        print(f"SUBSTANTIATIONS_RAW row count: {raw_count}")
        print(f"SUBSTANTIATIONS_REVISED row count: {revised_count}")
        print(f"SUBSTANTIATIONS_CODED row count: {coded_count}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
        
    finally:
        # Close the connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            print("\nConnection closed.")

if __name__ == '__main__':
    upload_to_snowflake()
EOL

# Run the upload script
python upload_to_snowflake.py 2>&1 | tee logs/snowflake_upload.log
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    log_message "ERROR: Snowflake upload failed"
    exit 1
fi

log_message "Pipeline completed successfully!"
echo "=== Pipeline Execution Summary ==="
echo "Started at: $(head -n1 logs/airtable_export.log | grep -o '[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}')"
echo "Completed at: $(date)"
echo "Log files available in ./logs/"
echo "Last successful run backed up in ./backup/"