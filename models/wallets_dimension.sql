
{{
    config(
        materialized="incremental",
        unique_key= ["hash_column"],
        on_schema_change='append_new_columns',
	incremental_strategy = 'merge'
	)
}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'wallets_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

-- Ensure dependencies are clearly defined for dbt
{% set _ = ref('wallets_stg_update') %}
{% set _ = ref('wallets_stg_exp') %}
{% set _ = ref('wallets_stg_new') %}
{% set _ = ref('wallets_stg') %}

SELECT

	id,
	operation,
	currentflag,
	expdate,
	walletid,
	walletnumber,
	hash_column,
	wallet_createdat_local,
	wallet_modifiedat_local,
	wallet_suspendedat_local,
	wallet_unsuspendedat_local,
	wallet_unregisteredat_local,
	wallet_activatedat_local,
	wallet_registeredat_local,
	wallet_reactivatedat_local,
	wallet_lasttxnts_local,
	utc,
	wallet_type,
	wallet_subtype,
	wallet_status,
	wallet_name,
	profileid,
	partnerid,
	pinsetflag,
	registeredby,
	loaddate

FROM {{ref("wallets_stg_update")}}

UNION ALL

SELECT 

	id,
	operation,
	currentflag,
	expdate,
	walletid,
	walletnumber,
	hash_column,
	wallet_createdat_local,
	wallet_modifiedat_local,
	wallet_suspendedat_local,
	wallet_unsuspendedat_local,
	wallet_unregisteredat_local,
	wallet_activatedat_local,
	wallet_registeredat_local,
	wallet_reactivatedat_local,
	wallet_lasttxnts_local,
	utc,
	wallet_type,
	wallet_subtype,
	wallet_status,
	wallet_name,
	profileid,
	partnerid,
	pinsetflag,
	registeredby,
	loaddate

FROM {{ref("wallets_stg_exp")}}


UNION ALL

SELECT

	id,
	operation,
	currentflag,
	expdate,
	walletid,
	walletnumber,
	hash_column,
	wallet_createdat_local,
	wallet_modifiedat_local,
	wallet_suspendedat_local,
	wallet_unsuspendedat_local,
	wallet_unregisteredat_local,
	wallet_activatedat_local,
	wallet_registeredat_local,
	wallet_reactivatedat_local,
	wallet_lasttxnts_local,
	utc,
	wallet_type,
	wallet_subtype,
	wallet_status,
	wallet_name,
	profileid,
	partnerid,
	pinsetflag,
	registeredby,
	loaddate

FROM {{ref("wallets_stg_new")}}
