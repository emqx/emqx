%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_aggregator_utils).

%% API
-export([
    validate_key_template/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

validate_key_template(Conf) ->
    Template = emqx_template:parse(Conf),
    case validate_bindings(emqx_template:placeholders(Template)) of
        Bindings when is_list(Bindings) ->
            ok;
        {error, {disallowed_placeholders, Disallowed}} ->
            {error, emqx_utils:format("Template placeholders are disallowed: ~p", [Disallowed])}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

validate_bindings(Bindings) ->
    case [B || B <- Bindings, not is_allowed_binding(B)] of
        [] ->
            Bindings;
        Disallowed ->
            {error, {disallowed_placeholders, Disallowed}}
    end.

is_allowed_binding("action") -> true;
is_allowed_binding("node") -> true;
is_allowed_binding("sequence") -> true;
is_allowed_binding("datetime." ++ Format) -> is_valid_datetime_format(Format);
is_allowed_binding("datetime_until." ++ Format) -> is_valid_datetime_format(Format);
is_allowed_binding(_) -> false.

is_valid_datetime_format(Format) ->
    emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(iolist_to_binary(Format)).
