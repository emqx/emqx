%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_test_helpers).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    check_fields/1,
    start_apps/1,
    stop_apps/1
]).

check_fields({FieldName, FieldValue}) ->
    ?assert(is_atom(FieldName)),
    if
        is_map(FieldValue) ->
            ct:pal("~p~n", [{FieldName, FieldValue}]),
            ?assert(
                (maps:is_key(type, FieldValue) andalso
                    maps:is_key(default, FieldValue)) orelse
                    (maps:is_key(required, FieldValue) andalso
                        maps:get(required, FieldValue) =:= false)
            );
        true ->
            ?assert(is_function(FieldValue))
    end.

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
