view: substantiations_revised {
  sql_table_name: "APP_CASH_BETA"."BSOTO"."SUBSTANTIATIONS_REVISED" ;;

  # Primary Key
  dimension: complaint_number {
    primary_key: yes
    type: string
    sql: ${TABLE}."Complaint Number" ;;
    description: "Unique identifier for each complaint"
  }

  # Regular Dimensions
  dimension: assignee {
    type: string
    sql: ${TABLE}."Assignee" ;;
    description: "Person assigned to the complaint"
  }

  dimension: review_status {
    type: string
    sql: ${TABLE}."Review Status" ;;
    description: "Current status of the review"
  }

  dimension: case_number {
    type: number
    sql: ${TABLE}."Case Number" ;;
    description: "Associated case number"
  }

  dimension: complaint_link {
    type: string
    sql: ${TABLE}."Complaint Link" ;;
    description: "Link to the original complaint"
    html: <a href="{{ value }}" target="_blank">View Complaint</a> ;;
  }

  dimension: primary_issue_root_cause {
    type: string
    sql: ${TABLE}."Primary Issue Root Cause" ;;
    description: "Root cause of the primary issue"
  }

  dimension: codes {
    type: string
    sql: ${TABLE}."Codes" ;;
    description: "JSON array of assigned codes"
  }

  dimension: product_type_sub_category {
    type: string
    sql: ${TABLE}."Product Type Sub Category" ;;
    description: "Sub-category of the product type"
  }

  dimension: insights_review_notes {
    type: string
    sql: ${TABLE}."Insights Review Notes" ;;
    description: "Notes from the review process"
  }

  dimension: complaint_statements {
    type: string
    sql: ${TABLE}."Complaint Statements" ;;
    description: "Original complaint statements"
  }

  dimension: substantiation {
    type: string
    sql: ${TABLE}."Substantiation" ;;
    description: "Whether the complaint was substantiated"
  }

  dimension: redress_required {
    type: string
    sql: ${TABLE}."Redress Required" ;;
    description: "Redress required flag"
  }

  dimension: regulatory_case_spend {
    type: string
    sql: ${TABLE}."Regulatory Case Spend" ;;
    description: "Regulatory case spend amount"
  }

  dimension: month_closed {
    type: string
    sql: ${TABLE}."Month Closed" ;;
    description: "Month when the case was closed"
  }

  dimension: product_type {
    type: string
    sql: ${TABLE}."Product Type" ;;
    description: "Type of product associated with the complaint"
  }

  dimension: primary_issue {
    type: string
    sql: ${TABLE}."Primary Issue" ;;
    description: "Main issue identified in the complaint"
  }

  dimension: spirit_bucket {
    type: string
    sql: ${TABLE}."Spirit Bucket" ;;
    description: "Spirit bucket categorization"
  }

  dimension: workflow {
    type: string
    sql: ${TABLE}."Workflow" ;;
    description: "Workflow status"
  }

  dimension: resulting_action {
    type: string
    sql: ${TABLE}."Resulting Action" ;;
    description: "Action taken as a result"
  }

  dimension: code_category {
    type: string
    sql: ${TABLE}."Code Category" ;;
    description: "Category of the codes"
  }

  dimension: secondary_issue {
    type: string
    sql: ${TABLE}."Secondary Issue" ;;
    description: "Secondary issue identified"
  }

  dimension: secondary_issue_root_cause {
    type: string
    sql: ${TABLE}."Secondary Issue Root Cause" ;;
    description: "Root cause of the secondary issue"
  }

  dimension: redress_requested {
    type: string
    sql: ${TABLE}."Redress Requested" ;;
    description: "Amount of redress requested"
  }

  dimension: redress_paid {
    type: string
    sql: ${TABLE}."Redress Paid" ;;
    description: "Amount of redress paid"
  }

  dimension: edge_case_fields {
    type: string
    sql: ${TABLE}."Edge Case Fields" ;;
    description: "Fields that make this an edge case"
  }

  dimension: edge_case_notes {
    type: string
    sql: ${TABLE}."Edge Case Notes" ;;
    description: "Notes about edge case status"
  }

  dimension: edge_case {
    type: string
    sql: ${TABLE}."Edge Case" ;;
    description: "Edge case flag"
  }

  dimension: edge_case_reviewed {
    type: string
    sql: ${TABLE}."Edge Case Reviewed" ;;
    description: "Edge case review status"
  }

  dimension: required_fields_complete {
    type: string
    sql: ${TABLE}."Required Fields Complete" ;;
    description: "Flag indicating if all required fields are complete"
  }

  # Date/Time Dimensions
  dimension_group: created {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: CAST(${TABLE}."Created" AS TIMESTAMP_NTZ) ;;
    description: "Timestamp when the record was created"
  }

  dimension_group: last_modified {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: CAST(${TABLE}."Last Modified" AS TIMESTAMP_NTZ) ;;
    description: "Timestamp of last modification"
  }

  dimension_group: review_completed {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: CAST(${TABLE}."Review Completed At" AS TIMESTAMP_NTZ) ;;
    description: "Timestamp when review was completed"
  }

  # Derived Dimensions
  dimension: days_to_complete_review {
    type: number
    sql: DATEDIFF(day, ${created_raw}, ${review_completed_raw}) ;;
    description: "Number of days taken to complete the review"
  }

  # Measures
  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: total_cases {
    type: count_distinct
    sql: ${case_number} ;;
    description: "Total number of unique cases"
  }

  measure: substantiated_complaints {
    type: count
    filters: [substantiation: "Yes"]
    description: "Number of substantiated complaints"
  }

  measure: substantiation_rate {
    type: number
    sql: ${substantiated_complaints}::float / NULLIF(${count}, 0) ;;
    value_format_name: percent_2
    description: "Percentage of complaints that were substantiated"
  }

  measure: average_days_to_complete {
    type: average
    sql: ${days_to_complete_review} ;;
    value_format: "0.0"
    description: "Average number of days to complete review"
  }

  # Sets for drilling
  set: detail {
    fields: [
      complaint_number,
      case_number,
      assignee,
      review_status,
      product_type,
      primary_issue,
      substantiation,
      created_date,
      review_completed_date
    ]
  }
}