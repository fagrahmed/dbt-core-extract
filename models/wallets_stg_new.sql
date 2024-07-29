
{{ config(
    materialized='incremental',
    unique_key= ['walletid', 'walletnumber'],
    depends_on=['wallets_stg'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}
    

{% if table_exists %}
-- dimension exists, get only new records; new id(new entry) , or hash_column is different(exp entry's new values)

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

FROM {{ source('dbt-dimensions', 'wallets_stg') }} stg
LEFT JOIN {{ source('dbt-dimensions', 'wallets_dimension') }} dim ON stg.walletid = dim.walletid
WHERE dim.walletid IS NULL OR (dim.hash_column != stg.hash_column AND dim.currentflag = true)


{% else %}
-- dimension doesnt exists so all is new

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

FROM {{ source('dbt-dimensions', 'wallets_stg') }} stg

{% endif %}