%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_adapter).

%% API
-export([
    card_out/1,

    format_register_error/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

card_out(Card) ->
    Card.

format_register_error({bad_id, Field, Id}) ->
    {ok, iolist_to_binary(io_lib:format("Bad ~s id: ~s", [Field, Id]))};
format_register_error(bad_card) ->
    {ok, <<"Card does not conform to schema">>};
format_register_error(not_a_json_object) ->
    {ok, <<"Card must be a JSON object">>};
format_register_error({namespace_not_found, Namespace}) when is_binary(Namespace) ->
    {ok, <<"Namespace not found: ", Namespace/binary>>};
format_register_error(_Reason) ->
    error.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
