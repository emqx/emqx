%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_utils).

%% API
-export([
    validate_card_schema/1,
    validate_id/3
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_a2a_registry_internal.hrl").

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

validate_card_schema(CardBin) ->
    case emqx_a2a_registry_config:is_schema_validation_enabled() of
        false ->
            ok;
        true ->
            do_validate_card_schema(CardBin)
    end.

validate_id(OrgId, UnitId, AgentId) ->
    maybe
        ok ?= do_validate_id(OrgId, org_id),
        ok ?= do_validate_id(UnitId, unit_id),
        ok ?= do_validate_id(AgentId, agent_id),
        ok
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

do_validate_card_schema(CardBin) ->
    IsValid = emqx_schema_registry_serde:schema_check(?A2A_SCHEMA_REGISTRY_SERDE_NAME, CardBin, []),
    case IsValid of
        true ->
            ok;
        false ->
            %% Reason is already logged at debug level by `schema_check`.
            {error, bad_card}
    end.

do_validate_id(Id, Field) when is_binary(Id) ->
    case re:run(Id, ?SEGMENT_ID_RE, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            {error, {bad_id, Field, Id}}
    end;
do_validate_id(Id, Field) ->
    {error, {bad_id, Field, Id}}.
