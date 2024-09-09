{{ config(
    materialized='incremental',
    unique_key= ['txndetailsid'],
    depends_on=['transactions_stg'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'transactions_stg_update') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'transactions_stg_update') }};{% endif %}"
    ]
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'transactions_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

SELECT
    final.id,
    'update' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    (final.upd_counter) + 1 AS upd_counter,
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
    stg.txn_amount,
    stg.balance_before,
    stg.balance_after,
    stg.actual_balance_before,
    stg.actual_balance_after,
    stg.hasservicefees,
    stg.transactionreference,
    stg.isreversedflag,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate,
    stg.is_fees

FROM {{ source('dbt-dimensions', 'transactions_stg') }} stg
JOIN {{ source('dbt-dimensions', 'transactions_dimension')}} final
    ON stg.txndetailsid = final.txndetailsid 
WHERE stg.loaddate > final.loaddate

{% else %}
-- do nothing (extremely high comparison date)

SELECT 
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    stg.txndetailsid,
    stg.walletdetailsid,
    stg.clientdetails,
    stg.transaction_createdat_local,
    stg.transaction_modifiedat_local,
    stg.transaction_commitat_local,
    stg.transaction_failedat_local,
    stg.utc,
    0 AS upd_counter,
    stg.txntype,
    stg.transactionstatus,
    stg.transactionchannel,
    stg.transactiondomain,
    stg.transactionaction,
    stg.interchangeaction,
    stg.interchange_amount,
    stg.service_fees,
    stg.txn_amount,
    stg.balance_before,
    stg.balance_after,
    stg.actual_balance_before,
    stg.actual_balance_after,
    stg.hasservicefees,
    stg.transactionreference,
    stg.isreversedflag,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate,
    stg.is_fees

FROM {{ ref('transactions_stg') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz

{% endif %}