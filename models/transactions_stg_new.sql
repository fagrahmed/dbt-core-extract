
{{ config(
    materialized='incremental',
    unique_key= ['txndetailsid'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'transactions_stg_new') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'transactions_stg_new') }};{% endif %}"
    ]
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'transactions_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}
    

{% if table_exists %}
-- dimension exists, get only new records; new id(new entry) , or hash_column is different(exp entry's new values)

SELECT 
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    0 as upd_counter,
    stg.txndetailsid,
    stg.walletdetailsid,
    stg.clientdetails,
    stg.transaction_createdat_local,
    stg.transaction_modifiedat_local,
    stg.transaction_commitat_local,
    stg.transaction_failedat_local,
    stg.utc,
    stg.txntype,
    stg.transactionstatus,
    stg.transactionchannel,
    stg.transactiondomain,
    stg.transactionaction,
    stg.interchangeaction,
    stg.interchange_amount,
    stg.service_fees,
    stg.amount,
    stg.balance_before,
    stg.balance_after,
    stg.actual_balance_before,
    stg.actual_balance_after,
    stg.hasservicefees,
    stg.transactionreference,
    stg.isreversedflag,
    stg.loaddate,
    stg.is_fees

FROM {{ source('dbt-dimensions', 'transactions_stg') }} stg
LEFT JOIN {{ source('dbt-dimensions', 'transactions_dimension') }} dim ON stg.txndetailsid = dim.txndetailsid
WHERE dim.txndetailsid IS NULL 

{% else %}
-- dimension doesnt exists so all is new

SELECT 
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    0 as upd_counter,
    stg.txndetailsid,
    stg.walletdetailsid,
    stg.clientdetails,
    stg.transaction_createdat_local,
    stg.transaction_modifiedat_local,
    stg.transaction_commitat_local,
    stg.transaction_failedat_local,
    stg.utc,
    stg.txntype,
    stg.transactionstatus,
    stg.transactionchannel,
    stg.transactiondomain,
    stg.transactionaction,
    stg.interchangeaction,
    stg.interchange_amount,
    stg.service_fees,
    stg.amount,
    stg.balance_before,
    stg.balance_after,
    stg.actual_balance_before,
    stg.actual_balance_after,
    stg.hasservicefees,
    stg.transactionreference,
    stg.isreversedflag,
    stg.loaddate,
    stg.is_fees

FROM {{ source('dbt-dimensions', 'transactions_stg') }} stg

{% endif %}
