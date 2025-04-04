# Airtable2Snowflake
This project implements an automated pipeline for transferring complaint data from Airtable to Snowflake, including data transformation and LookML view generation.

## Pipeline Overview

The pipeline extracts complaint data from Airtable, processes it, and loads it into Snowflake in three related tables:
- `SUBSTANTIATIONS_RAW`: Raw data as extracted from Airtable
- `SUBSTANTIATIONS_REVISED`: Clean copy of the raw data
- `SUBSTANTIATIONS_CODED`: Exploded view of complaint codes with categorization

## Required Files

### Core Pipeline Scripts

1. `run_pipeline_v7.sh` - Main pipeline orchestration script
   - Coordinates the entire ETL process
   - Handles logging and error management
   - Manages backup and cleanup operations

2. `airtable_export.rb` - Airtable Data Extraction
   - Downloads data from Airtable
   - Handles pagination and rate limiting
   - Manages duplicate records
   - Creates `airtable_export_final.csv`

3. `translate_codes.py` - Data Transformation
   - Processes `airtable_export_final.csv`
   - Translates codes and subcategories
   - Creates `snowflake_import.csv`

### LookML View Files

1. `substantiations_revised_v2.view.lkml` - Revised Table View
   - Defines dimensions and measures for the SUBSTANTIATIONS_REVISED table
   - Includes all fields from raw data
   - Provides proper timestamp handling
   - Includes drill-down capabilities

2. `substantiations_coded_v2.view.lkml` - Coded Table View
   - Defines dimensions and measures for the SUBSTANTIATIONS_CODED table
   - Implements code categorization
   - Provides aggregation capabilities
   - Includes category-specific measures

### Documentation

1. `erd_v3.html` - Entity Relationship Diagram
   - Interactive visualization of table relationships
   - Field definitions and data types
   - Relationship cardinality
   - Access at: `file:///Users/bsoto/Goose_Projects/Airtable2Snowflake/erd_v3.html`

## Directory Structure

\`\`\`
.
├── run_pipeline_v7.sh          # Main pipeline script
├── airtable_export.rb         # Airtable data extraction
├── translate_codes.py         # Code translation and processing
├── data/                     # Temporary data storage
│   └── .gitignore           # Ignores temporary files
├── backup/                   # Backup storage (last 5 runs)
├── logs/                     # Pipeline execution logs
└── lookml/                   # LookML view files
    ├── substantiations_revised_v2.view.lkml
    └── substantiations_coded_v2.view.lkml
\`\`\`

## Table Relationships

1. SUBSTANTIATIONS_RAW
   - Primary landing table
   - Contains all fields from Airtable
   - Includes JSON arrays for codes

2. SUBSTANTIATIONS_REVISED
   - 1:1 relationship with RAW
   - Exact copy for data consistency
   - Used for general querying

3. SUBSTANTIATIONS_CODED
   - 1:N relationship with REVISED
   - Exploded view of codes
   - Each row represents one code per complaint
   - Includes standardized categorization

## Running the Pipeline

1. Ensure all required files are present
2. Execute the pipeline:
   \`\`\`bash
   ./run_pipeline_v7.sh
   \`\`\`

The pipeline will:
- Download data from Airtable
- Process and transform the data
- Upload to Snowflake
- Create backup of successful run
- Clean up temporary files

## LookML Views

### SUBSTANTIATIONS_REVISED View
- Primary dimensions:
  - complaint_number (PK)
  - case_number
  - product_type
  - primary_issue
  - review_status
- Time-based dimensions:
  - created_date
  - last_modified_date
  - review_completed_date
- Measures:
  - count
  - substantiation_rate
  - average_days_to_complete

### SUBSTANTIATIONS_CODED View
- Primary dimensions:
  - complaint_number (FK)
  - code
  - code_category
- Measures:
  - count
  - distinct_complaints
  - codes_per_complaint
  - Category-specific counts

## Entity Relationship Diagram (ERD)

The ERD (`erd_v3.html`) provides a visual representation of:
- Table structures and relationships
- Field definitions and data types
- Primary and foreign keys
- Relationship cardinality
- Data flow

To view the ERD:
1. Open in a web browser: `file:///Users/bsoto/Goose_Projects/Airtable2Snowflake/erd_v3.html`
2. Interactive features:
   - Hover over fields for details
   - Click relationships for cardinality
   - View data type information

## Dependencies

1. Ruby Gems:
   - json
   - csv
   - fileutils
   - uri
   - net/http

2. Python Packages:
   - pandas
   - snowflake-connector-python[pandas]
   - requests

## Maintenance

- Logs are stored in `./logs/`
- Backups are kept in `./backup/` (last 5 runs)
- The data directory is cleaned after successful runs
- Failed runs preserve data for debugging

## Error Handling

The pipeline includes comprehensive error handling:
- Airtable rate limiting and pagination
- Duplicate record management
- Data transformation validation
- Snowflake connection and upload verification
- Backup and cleanup management

## Monitoring

- Each run generates detailed logs
- Row counts are verified at each stage
- Data samples are provided for validation
- Backup copies are maintained for recovery
