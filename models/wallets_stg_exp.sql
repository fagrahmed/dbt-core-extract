
{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    depends_on=['wallets_stg'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'wallets_stg_exp') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'inc_wallets_stg_exp') }};{% endif %}"
    ]
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}


SELECT
    final.id AS id,
    'exp' AS operation,
    false AS currentflag,
    (now()::timestamp AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS expdate,
    final.walletid,
    final.walletnumber,
    final.hash_column,
    final.wallet_createdat_local,
    final.wallet_modifiedat_local,
    final.wallet_suspendedat_local,
    final.wallet_unsuspendedat_local,
    final.wallet_unregisteredat_local,
    final.wallet_activatedat_local,
    final.wallet_reactivatedat_local,
    final.wallet_lasttxnts_local,
    final.utc,
    final.wallet_type,
    final.wallet_status,
    final.profileid,
    final.partnerid,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate  

FROM {{ source('dbt-dimensions', 'wallets_stg') }} stg
JOIN {{ source('dbt-dimensions', 'wallets_dimension')}} final
    ON stg.walletid = final.walletid AND stg.walletnumber = final.walletnumber
WHERE stg.loaddate > final.loaddate AND stg.hash_column != final.hash_column AND final.currentflag = true


{% else %}
-- do nothing (extremely high comparison date)
SELECT 
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    stg.walletid,
    stg.walletnumber,
    stg.hash_column,
    stg.wallet_createdat_local,
    stg.wallet_modifiedat_local,
    stg.wallet_suspendedat_local,
    stg.wallet_unsuspendedat_local,
    stg.wallet_unregisteredat_local,
    stg.wallet_activatedat_local,
    stg.wallet_reactivatedat_local,
    stg.wallet_lasttxnts_local,
    stg.utc,
    stg.wallet_type,
    stg.wallet_status,
    stg.profileid,
    stg.partnerid,
    stg.loaddate

FROM {{ ref('wallets_stg') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz 
{% endif %}
