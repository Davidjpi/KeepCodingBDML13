CREATE OR REPLACE TABLE keepcoding.ivr_summary AS
WITH detail_documents 
  AS (SELECT calls_ivr_id
           , document_type
           , document_identification
      FROM keepcoding.ivr_detail
      WHERE document_type <> 'UNKNOWN' AND document_identification <> 'UNKNOWN' AND document_type <> 'DESCONOCIDO'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS STRING) ORDER BY document_type DESC) = 1)

, detail_customer_phone 
  AS (SELECT calls_ivr_id
           , customer_phone
      FROM keepcoding.ivr_detail
      WHERE customer_phone <> 'UNKNOWN')

, detail_billing_account_id 
  AS (SELECT calls_ivr_id
           , billing_account_id
      FROM keepcoding.ivr_detail
      WHERE billing_account_id <> 'UNKNOWN'
      GROUP BY calls_ivr_id
            , billing_account_id
      QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(calls_ivr_id AS STRING) ORDER BY billing_account_id DESC) = 1)

, detail_masiva 
  AS (SELECT calls_ivr_id
           , module_name
      FROM keepcoding.ivr_detail
      WHERE module_name = 'AVERIA_MASIVA'
      GROUP BY calls_ivr_id
            , module_name)

, detail_info_by_phone 
  AS (SELECT calls_ivr_id
           , step_name
           , step_description_error
      FROM keepcoding.ivr_detail
      WHERE step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_description_error = 'UNKNOWN'
      GROUP BY calls_ivr_id
            , step_name
            , step_description_error)

, detail_info_by_dni 
  AS (SELECT calls_ivr_id
           , step_name
           , step_description_error
      FROM keepcoding.ivr_detail
      WHERE step_name = 'CUSTOMERINFOBYDNI.TX' AND step_description_error = 'UNKNOWN'
      GROUP BY calls_ivr_id
            , step_name
            , step_description_error)

, calls_repeated_phone 
  AS (SELECT ivr_id
           , LAG(start_date) OVER(PARTITION BY phone_number ORDER BY start_date) previous_calls_start_date
           , start_date
           , LEAD(start_date) OVER(PARTITION BY phone_number ORDER BY start_date) next_calls_start_date
      FROM keepcoding.ivr_calls)

SELECT ivr_detail.calls_ivr_id AS ivr_id
     , ivr_detail.calls_phone_number AS phone_number
     , ivr_detail.calls_ivr_result AS ivr_result
     , CASE WHEN STARTS_WITH(ivr_detail.calls_vdn_label, 'ATC') THEN 'FRONT'
            WHEN STARTS_WITH(ivr_detail.calls_vdn_label, 'TECH') THEN 'TECH'
            WHEN ivr_detail.calls_vdn_label = 'ABSORTION' THEN 'ABSORTION'
            ELSE 'RESTO'
       END AS vdn_aggregation
     , ivr_detail.calls_start_date AS start_date
     , ivr_detail.calls_end_date AS end_date
     , ivr_detail.calls_total_duration AS total_duration
     , ivr_detail.calls_customer_segment AS customer_segment
     , ivr_detail.calls_ivr_language AS ivr_language
     , ivr_detail.calls_steps_module AS steps_module
     , ivr_detail.calls_module_aggregation AS module_aggregation
     , COALESCE(detail_documents.document_type, 'UNKNOWN') AS document_type
     , COALESCE(detail_documents.document_identification, 'UNKNOWN') AS document_identification
     , COALESCE(detail_customer_phone.customer_phone, 'UNKNOWN') AS customer_phone
     , COALESCE(detail_billing_account_id.billing_account_id, 'UNKNOWN') AS billing_account_id
     , IF(detail_masiva.module_name IS NULL, 0, 1) AS masiva_lg
     , IF(detail_info_by_phone.step_name IS NULL, 0, 1) AS info_by_phone_lg
     , IF(detail_info_by_dni.step_name IS NULL, 0, 1) AS info_by_dni_lg
     , IF(calls_repeated_phone.previous_calls_start_date IS NOT NULL AND TIMESTAMP_DIFF(start_date, calls_repeated_phone.previous_calls_start_date, HOUR) < 24, 1, 0) AS repeated_phone_24H
     , IF(calls_repeated_phone.next_calls_start_date IS NOT NULL AND TIMESTAMP_DIFF(calls_repeated_phone.next_calls_start_date, start_date, HOUR) < 24, 1, 0) AS cause_recall_phone_24H
  FROM keepcoding.ivr_detail
  LEFT
  JOIN detail_documents
    ON ivr_detail.calls_ivr_id = detail_documents.calls_ivr_id
  LEFT
  JOIN detail_customer_phone
    ON ivr_detail.calls_ivr_id = detail_customer_phone.calls_ivr_id
  LEFT
  JOIN detail_billing_account_id
    ON ivr_detail.calls_ivr_id = detail_billing_account_id.calls_ivr_id
  LEFT
  JOIN detail_masiva
    ON ivr_detail.calls_ivr_id = detail_masiva.calls_ivr_id
  LEFT
  JOIN detail_info_by_phone
    ON ivr_detail.calls_ivr_id = detail_info_by_phone.calls_ivr_id
  LEFT
  JOIN detail_info_by_dni
    ON ivr_detail.calls_ivr_id = detail_info_by_dni.calls_ivr_id
  LEFT
  JOIN calls_repeated_phone
    ON ivr_detail.calls_ivr_id = calls_repeated_phone.ivr_id
GROUP BY ivr_id
       , phone_number
       , ivr_result
       , vdn_aggregation
       , start_date
       , end_date
       , total_duration
       , customer_segment
       , ivr_language
       , steps_module
       , module_aggregation
       , document_type
       , document_identification
       , customer_phone
       , billing_account_id
       , masiva_lg
       , info_by_phone_lg
       , info_by_dni_lg
       , repeated_phone_24H
       , cause_recall_phone_24H