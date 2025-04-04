view: substantiations_coded {
  sql_table_name: "APP_CASH_BETA"."BSOTO"."SUBSTANTIATIONS_CODED" ;;

  # Foreign Key to substantiations_revised
  dimension: complaint_number {
    type: string
    sql: ${TABLE}."Complaint Number" ;;
    description: "Foreign key to substantiations_revised"
  }

  # Regular Dimensions
  dimension: code {
    type: string
    sql: ${TABLE}."code" ;;
    description: "Specific code assigned"
  }

  dimension: code_category {
    type: string
    sql: ${TABLE}."code_category" ;;
    description: "Category of the code"
    order_by_field: code_category_sort
  }

  # Hidden dimension for sorting code categories
  dimension: code_category_sort {
    hidden: yes
    type: string
    sql: CASE ${code_category}
          WHEN '01. Service and Support Issues' THEN '01'
          WHEN '02. Transaction and Processing Issues' THEN '02'
          WHEN '03. Account Access and Security' THEN '03'
          WHEN '04. Product Features and Functionality' THEN '04'
          WHEN '05. Fees and Charges' THEN '05'
          WHEN '06. Security and Fraud Concerns' THEN '06'
          WHEN '07. Technical Issues' THEN '07'
          WHEN '08. Compliance and Regulatory' THEN '08'
          WHEN '09. Third Party Integration' THEN '09'
          WHEN '10. User Experience and Expectations' THEN '10'
          WHEN '11. Marketing and Communications' THEN '11'
          WHEN '12. Documentation and Procedure' THEN '12'
          WHEN '13. Privacy and Data Protection' THEN '13'
          ELSE '99'
         END ;;
  }

  # Measures
  measure: count {
    type: count
    drill_fields: [detail*]
    description: "Total number of code assignments"
  }

  measure: distinct_complaints {
    type: count_distinct
    sql: ${complaint_number} ;;
    description: "Number of unique complaints"
  }

  measure: codes_per_complaint {
    type: number
    sql: ${count}::float / NULLIF(${distinct_complaints}, 0) ;;
    value_format: "0.00"
    description: "Average number of codes per complaint"
  }

  # Category-specific counts
  measure: service_support_issues_count {
    type: count
    filters: [code_category: "01. Service and Support Issues"]
    group_label: "Category Counts"
  }

  measure: transaction_processing_issues_count {
    type: count
    filters: [code_category: "02. Transaction and Processing Issues"]
    group_label: "Category Counts"
  }

  measure: account_access_security_count {
    type: count
    filters: [code_category: "03. Account Access and Security"]
    group_label: "Category Counts"
  }

  measure: product_features_count {
    type: count
    filters: [code_category: "04. Product Features and Functionality"]
    group_label: "Category Counts"
  }

  measure: fees_charges_count {
    type: count
    filters: [code_category: "05. Fees and Charges"]
    group_label: "Category Counts"
  }

  measure: security_fraud_concerns_count {
    type: count
    filters: [code_category: "06. Security and Fraud Concerns"]
    group_label: "Category Counts"
  }

  measure: technical_issues_count {
    type: count
    filters: [code_category: "07. Technical Issues"]
    group_label: "Category Counts"
  }

  measure: compliance_regulatory_count {
    type: count
    filters: [code_category: "08. Compliance and Regulatory"]
    group_label: "Category Counts"
  }

  measure: third_party_integration_count {
    type: count
    filters: [code_category: "09. Third Party Integration"]
    group_label: "Category Counts"
  }

  measure: user_experience_count {
    type: count
    filters: [code_category: "10. User Experience and Expectations"]
    group_label: "Category Counts"
  }

  measure: marketing_communications_count {
    type: count
    filters: [code_category: "11. Marketing and Communications"]
    group_label: "Category Counts"
  }

  measure: documentation_procedure_count {
    type: count
    filters: [code_category: "12. Documentation and Procedure"]
    group_label: "Category Counts"
  }

  measure: privacy_data_protection_count {
    type: count
    filters: [code_category: "13. Privacy and Data Protection"]
    group_label: "Category Counts"
  }

  # Percentage measures
  measure: percent_of_total {
    type: percent_of_total
    sql: ${count} ;;
    description: "Percentage of total code assignments"
  }

  # Sets for drilling
  set: detail {
    fields: [
      complaint_number,
      code,
      code_category
    ]
  }
}