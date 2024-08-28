{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'wallets_stg') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'wallets_stg') }};{% endif %}"
    ]
)}}
    
{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}
    
{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'wallets_stg')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists = stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}

SELECT
    md5( COALESCE(walletid, '') || '-' || COALESCE(walletnumber, '') || '-' || COALESCE(lastmodified::text, '') || '-' || (now()::timestamptz)::text)
    AS id,
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    walletid AS walletid,
    walletnumber AS walletnumber,
    md5(
        COALESCE(walletid, '') || '::' || COALESCE(walletnumber, '') || '::' || COALESCE(walletStatus, '') || '::' ||
        COALESCE(nationalid, '') || '::' || COALESCE(firstname, '') || '::' || COALESCE(lastname, '') || '::' ||
        COALESCE(clientdata::text, '') || '::' || COALESCE(partnerid, '') || '::' || COALESCE(activatedat, '') || '::' || 
        COALESCE(reactivatedat, '') || '::' || COALESCE(waivedamount::text, '') || '::' || COALESCE(suspendedreason, '') || '::' ||
        COALESCE(pinsetflag::text, '')
    ) AS hash_column,
    (createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS wallet_createdat_local,
    (lastmodified::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS wallet_modifiedat_local,
    (suspendedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS wallet_suspendedat_local,
    (unsuspendedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS wallet_unsuspendedat_local,
    (unregisteredat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS wallet_unregisteredat_local,
    (activatedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS wallet_activatedat_local,
    (registeredat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS wallet_registeredat_local,
    (reactivatedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS wallet_reactivatedat_local,
    (lasttxnts::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS wallet_lasttxnts_local,
    3 as utc,
    wallettype AS wallet_type,
    walletStatus AS wallet_status,
    walletprofileid AS profileid,
    partnerid AS partnerid,
    pinsetflag AS pinsetflag,
    registeredby->>'userId' as registeredby,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate
FROM
    {{ source('axis_core', 'walletdetails') }} src
{% if is_incremental() and table_exists and stg_table_exists %}
    WHERE (_airbyte_emitted_at::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours')
            > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'wallets_dimension') }}), '1900-01-01'::timestamp)
{% endif %}
