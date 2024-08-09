
{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    depends_on=['wallets_stg'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'wallets_stg_update') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'wallets_stg_update') }};{% endif %}"
    ]
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

with update_old as (
    SELECT
        final.id AS id,
        'update' AS operation,
        true AS currentflag,
        null::timestamptz AS expdate,
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
        stg.registeredby,
        (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate  

    FROM {{ source('dbt-dimensions', 'wallets_stg') }} stg
    JOIN {{ source('dbt-dimensions', 'wallets_dimension')}} final
        ON stg.walletid = final.walletid AND stg.walletnumber = final.walletnumber
    WHERE final.hash_column is not null AND final.hash_column = stg.hash_column and final.operation != 'exp'
        AND stg.loaddate > final.loaddate
)

SELECT * from update_old

{% else %}

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
    stg.registeredby,
    stg.loaddate

FROM {{ ref('wallets_stg') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz
{% endif %}
