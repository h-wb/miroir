{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    incremental_strategy='timeline_replace',
    grants=timeline_grants(),
    post_hook="{{ timeline_exact_acl(true) }}",
) }}
-- depends_on: {{ ref('days') }}
-- What the viewer reads, as one row (package split, step 11): `version`, the
-- tables' version as major.minor (src/analyses/views.py CONTRACT_VERSION), and
-- `lanes`, the lanes this build declared, in order, as a JSON array of what the
-- models read of each (role, source, media_kind; the wording is the viewer's
-- and is not part of the build). Tables built before this relation existed are
-- the same tables, which the viewer reads as 1.0 until a build writes it. A column
-- or relation added is a minor change, one removed or changed a major one; the
-- viewer refuses a major it does not know. Written last, after days, so a build
-- that fails before the end leaves the previous version's row. Rebuilt in full
-- every build, its row replaced in the model's transaction.
{% set version = var('timeline_contract', '') %}
{% if execute and not modules.re.fullmatch('[0-9]+\\.[0-9]+', version) %}
    {{ exceptions.raise_compiler_error('Invalid timeline_contract ' ~ version) }}
{% endif %}
select
    {{ timeline_literal(version) }} as version,
    {{ timeline_literal(tojson(timeline_lanes())) }}::jsonb as lanes
