%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_cli).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    load/0,
    license/1,
    unload/0,
    print_warnings/1
]).

-define(PRINT_MSG(Msg), io:format(Msg)).

-define(PRINT(Format, Args), io:format(Format, Args)).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

load() ->
    ok = emqx_ctl:register_command(license, {?MODULE, license}, []).

license(["update", EncodedLicense]) ->
    do_update_license_key(fun() ->
        emqx_license:update_key(EncodedLicense)
    end);
license(["info"]) ->
    lists:foreach(
        fun
            ({K, V}) when is_binary(V); is_atom(V); is_list(V) ->
                ?PRINT("~-16s: ~s~n", [K, V]);
            ({K, V}) ->
                ?PRINT("~-16s: ~p~n", [K, V])
        end,
        emqx_license_checker:dump()
    );
license(["unset"]) ->
    do_update_license_key(fun() ->
        ?tp(unset_to_default_evaluation_license_key, #{operation_method => "cli"}),
        ?SLOG(info, #{msg => "unset_to_default_evaluation_license_key", operation_method => "cli"}),
        emqx_license:unset()
    end);
license(_) ->
    emqx_ctl:usage(
        [
            {"license info", "Show license info"},
            {"license unset", "Unset license to default evaluation license"},
            {"license update <License>", "Update license given as a string"}
        ]
    ).

do_update_license_key(Fun) when is_function(Fun, 0) ->
    case Fun() of
        {ok, Warnings} ->
            ok = print_warnings(Warnings),
            ok = ?PRINT_MSG("ok~n");
        {error, Reason} ->
            ?PRINT("Error: ~p~n", [Reason])
    end.

unload() ->
    ok = emqx_ctl:unregister_command(license).

print_warnings(Warnings) ->
    emqx_license_checker:print_warnings(Warnings).
