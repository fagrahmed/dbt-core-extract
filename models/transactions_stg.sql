-- models/transactions_dimension.sql
{{ 
    config(
        materialized='incremental',
        unique_key= ['txndetailsid'],
        on_schema_change='append_new_columns',
        pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'transactions_stg') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'transactions_stg') }};{% endif %}"
    ]
) }}
    
{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'transactions_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'transactions_stg')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists = stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}
    
SELECT
    md5(random()::text || '-' || COALESCE(txndetailsid, '') || '-' || COALESCE(walletdetailsid, '') || '-' || COALESCE(lastmodified::text, '') || '-' || now()::text) AS id,
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    txndetailsid,
    walletdetailsid,
    clientdetails,
    (createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS transaction_createdat_local,
    (lastmodified::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS transaction_modifiedat_local,
    (committs::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS transaction_commitat_local,
    (failedts::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS transaction_failedat_local,
    3 as utc,
    txntype,
    transactionstatus,
    transactionchannel,
    transactiondomain,
    transactionaction,
    interchangeaction,
    interchangeamount::float as interchange_amount,
    servicefees_aibyte_transform::float as service_fees,
    txnrequestedamount_aibyte_transform::float as txn_amount,
    walletbalancebefore_aibyte_transform::float as balance_before,
    walletbalanceafter_aibyte_transform::float as balance_after,
    walletactualbalancebefore_aibyte_transform as actual_balance_before,
    walletactualbalanceafter_aibyte_transform as actual_balance_after,
    receivername as wallet_receiver_name,
    receiverwalletnumber as wallet_receiver_number,
    sendername as wallet_sender_name,
    senderwalletnumber as wallet_sender_number,
    hasservicefees,
    transactionreference,
    isreversedflag,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate,
    CASE
        WHEN txntype NOT LIKE '%FEES' OR txntype = 'TransactionTypes_CREATE-VCN_FEES' THEN false
        ELSE true
    END AS is_fees

FROM {{ source('axis_core', 'transactiondetails')}} src
{% if is_incremental() and table_exists and stg_table_exists %}
    WHERE (src._airbyte_emitted_at::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'transactions_dimension') }}), '1900-01-01'::timestamp)
{% endif %}