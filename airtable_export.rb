#!/usr/bin/env ruby
require 'net/http'
require 'json'
require 'csv'
require 'fileutils'
require 'uri'

# Configuration
CONFIG = {
  base_id: 'appirNSRfONSuJzM6',
  complaints_table_id: 'tblBpLdX6B5DfK8pt',  # US - Regulatory Complaints table
  subcategories_table_id: 'tblKHi5AqBkTrFJEN',  # Product Type Sub Categories table
  codes_table_id: 'tbl13uzimoOgGOIG6',  # Codes table
  api_key: 'PAT TOKEN HERE'
}

# Create data directory if it doesn't exist
DATA_DIR = File.join(File.dirname(__FILE__), 'data')
FileUtils.mkdir_p(DATA_DIR)

def fetch_records(api_key, base_id, table_id, filter_formula=nil)
  all_records = []
  offset = nil

  loop do
    uri = URI("https://api.airtable.com/v0/#{base_id}/#{table_id}")
    
    # Combine filter and offset parameters if needed
    query_params = {}
    query_params['filterByFormula'] = filter_formula if filter_formula
    query_params['offset'] = offset if offset
    
    uri.query = URI.encode_www_form(query_params) unless query_params.empty?
    
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true

    request = Net::HTTP::Get.new(uri)
    request['Authorization'] = "Bearer #{api_key}"
    request['Content-Type'] = 'application/json'

    response = http.request(request)
    puts "Response Code: #{response.code}"
    
    if response.code != '200'
      puts "Error Response: #{response.body}"
      raise "API request failed with status #{response.code}"
    end
    
    data = JSON.parse(response.body)
    
    if data['error']
      raise "API Error: #{data['error']}"
    end
    
    if data['records']
      all_records.concat(data['records'])
      puts "Fetched #{data['records'].length} records..."
    else
      raise "Unexpected response format: #{data}"
    end
    
    offset = data['offset']
    break unless offset
  end

  all_records
end

# Main execution
begin
  puts "\n=== Airtable Export Script ==="
  puts "Starting export at: #{Time.now}"
  puts "Output directory: #{DATA_DIR}"
  
  # 1. First, fetch all Product Type Sub Categories to create ID -> Name mapping
  puts "\nFetching Product Type Sub Categories..."
  subcategories = fetch_records(
    CONFIG[:api_key],
    CONFIG[:base_id],
    CONFIG[:subcategories_table_id]
  )
  
  # Create mapping of subcategory record IDs to names
  subcategory_map = {}
  subcategories.each do |record|
    record_id = record['id']  # This is the record ID that will be referenced in other tables
    subcategory_map[record_id] = record['fields']['Name']
  end
  
  # Save subcategory mapping for reference
  File.write('subcategory_mapping.json', JSON.pretty_generate(subcategory_map))
  puts "Saved #{subcategory_map.size} subcategory mappings"
  
  # 2. Fetch Codes
  puts "\nFetching code mappings..."
  codes = fetch_records(
    CONFIG[:api_key],
    CONFIG[:base_id],
    CONFIG[:codes_table_id]
  )
  
  # Create mapping of code IDs to names
  code_map = {}
  codes.each do |record|
    code_map[record['id']] = record['fields']['Code Name']
  end
  
  # Save code mapping
  File.write('code_mapping.json', JSON.pretty_generate(code_map))
  puts "Saved #{code_map.size} code mappings"
  
  # 3. Fetch main complaint records
  puts "\nFetching complaint records..."
  filter_formula = "OR({Review Status}='Done',{Review Status}='Archive')"
  records = fetch_records(
    CONFIG[:api_key],
    CONFIG[:base_id],
    CONFIG[:complaints_table_id],
    filter_formula
  )
  puts "Retrieved #{records.length} total records"

  # Check for duplicates
  complaint_numbers = records.map { |r| r['fields']['Complaint Number'] }
  duplicates = complaint_numbers.select { |e| complaint_numbers.count(e) > 1 }.uniq
  if duplicates.any?
    puts "\nWARNING: Found #{duplicates.size} duplicate complaint numbers:"
    duplicates.take(5).each do |complaint_number|
      count = complaint_numbers.count(complaint_number)
      dupes = records.select { |r| r['fields']['Complaint Number'] == complaint_number }
      puts "\nComplaint Number #{complaint_number} appears #{count} times:"
      dupes.each do |dupe|
        puts "  - Created: #{dupe['fields']['Created']}"
        puts "  - Last Modified: #{dupe['fields']['Last Modified']}"
        puts "  - Review Status: #{dupe['fields']['Review Status']}"
      end
    end

    # Keep only the most recently modified version of each record
    puts "\nDeduplicating records..."
    unique_records = {}
    records.each do |record|
      complaint_number = record['fields']['Complaint Number']
      last_modified = record['fields']['Last Modified']
      
      if !unique_records[complaint_number] || 
         unique_records[complaint_number]['fields']['Last Modified'] < last_modified
        unique_records[complaint_number] = record
      end
    end
    records = unique_records.values
    puts "After deduplication: #{records.length} records"
  end

  # Convert to CSV
  if records.any?
    output_file = File.join(DATA_DIR, "airtable_export_final.csv")
    
    # Define headers
    headers = [
      'Complaint Number',
      'Assignee',
      'Review Status',
      'Case Number',
      'Complaint Link',
      'Primary Issue Root Cause',
      'Codes',
      'Product Type Sub Category',  # This will now contain the actual names
      'Insights Review Notes',
      'Created',
      'Last Modified',
      'Complaint Statements',
      'Substantiation',
      'Redress Required',
      'Regulatory Case Spend',
      'Month Closed',
      'Product Type',
      'Primary Issue',
      'Spirit Bucket',
      'Workflow',
      'Resulting Action',
      'Code Category',
      'Secondary Issue',
      'Secondary Issue Root Cause',
      'Redress Requested',
      'Review Completed At',
      'Redress Paid',
      'Edge Case Fields',
      'Edge Case Notes',
      'Edge Case',
      'Edge Case Reviewed',
      'Required Fields Complete'
    ]
    
    CSV.open(output_file, 'wb') do |csv|
      # Write headers
      csv << headers

      # Write data
      records.each do |record|
        fields = record['fields']
        
        # Process subcategories - convert IDs to actual names
        subcategories = if fields['Product Type Sub Category']&.is_a?(Array)
          fields['Product Type Sub Category'].map { |id| subcategory_map[id] || id }
        else
          []
        end
        
        # Process codes
        codes = if fields['Codes']&.is_a?(Array)
          fields['Codes'].map { |id| code_map[id] || id }
        else
          []
        end

        # Handle root cause fields
        primary_root_cause = if fields['Primary Issue Root Cause']&.is_a?(Array)
          fields['Primary Issue Root Cause'].first
        else
          fields['Primary Issue Root Cause']
        end

        secondary_root_cause = if fields['Secondary Issue Root Cause']&.is_a?(Array)
          fields['Secondary Issue Root Cause'].first
        else
          fields['Secondary Issue Root Cause']
        end

        row_data = {
          'Complaint Number' => fields['Complaint Number'],
          'Assignee' => fields['Assignee']&.dig('name'),
          'Review Status' => fields['Review Status'],
          'Case Number' => fields['Case Number'],
          'Complaint Link' => fields['Complaint Link'],
          'Primary Issue Root Cause' => primary_root_cause,
          'Codes' => codes.to_json,
          'Product Type Sub Category' => subcategories.to_json,
          'Insights Review Notes' => fields['Insights Review Notes'],
          'Created' => fields['Created'],
          'Last Modified' => fields['Last Modified'],
          'Complaint Statements' => fields['Complaint Statements'],
          'Substantiation' => fields['Substantiation'],
          'Redress Required' => fields['Redress Required'],
          'Regulatory Case Spend' => fields['Regulatory Case Spend'],
          'Month Closed' => fields['Month Closed'],
          'Product Type' => fields['Product Type'],
          'Primary Issue' => fields['Primary Issue'],
          'Spirit Bucket' => fields['Spirit Bucket'],
          'Workflow' => fields['Workflow'],
          'Resulting Action' => fields['Resulting Action'],
          'Code Category' => fields['Code Category']&.to_json,
          'Secondary Issue' => fields['Secondary Issue'],
          'Secondary Issue Root Cause' => secondary_root_cause,
          'Redress Requested' => fields['Redress Requested'],
          'Review Completed At' => fields['Review Completed At'],
          'Redress Paid' => fields['Redress Paid'],
          'Edge Case Fields' => fields['Which Field/s make this an edge case?'],
          'Edge Case Notes' => fields['Edge Case Notes'],
          'Edge Case' => fields['Edge Case'],
          'Edge Case Reviewed' => fields['Edge Case Reviewed'],
          'Required Fields Complete' => fields['Required Fields Complete?']
        }

        csv << headers.map { |h| row_data[h] || '' }
      end
    end
    
    puts "\nExport completed successfully!"
    puts "Data saved to: #{output_file}"
    puts "Fields included: #{headers.join(', ')}"
    puts "\nFile size: #{(File.size(output_file).to_f / 1024 / 1024).round(2)} MB"
    puts "Review Status filter: Only 'Done' or 'Archive' records included"
    puts "Completed at: #{Time.now}"

    # Debug: Print first few records with subcategories
    puts "\nFirst 5 records with subcategories:"
    records.take(5).each do |record|
      if record['fields']['Product Type Sub Category']
        puts "\nComplaint Number: #{record['fields']['Complaint Number']}"
        puts "Original subcategory IDs: #{record['fields']['Product Type Sub Category'].inspect}"
        subcategories = record['fields']['Product Type Sub Category'].map { |id| subcategory_map[id] || id }
        puts "Mapped subcategory names: #{subcategories.inspect}"
      end
    end
  else
    puts "No records found matching the filter criteria"
  end
rescue => e
  puts "\nERROR: #{e.message}"
  puts "Backtrace: #{e.backtrace.join("\n")}"
end
