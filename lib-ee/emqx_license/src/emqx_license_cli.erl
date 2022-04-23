%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_cli).

-include("emqx_license.hrl").

-export([load/0, license/1, unload/0, print_warnings/1]).

-define(PRINT_MSG(Msg), io:format(Msg)).

-define(PRINT(Format, Args), io:format(Format, Args)).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

load() ->
    ok = emqx_ctl:register_command(license, {?MODULE, license}, []).

license(["reload"]) ->
    case emqx:get_config([license]) of
        #{file := Filename} ->
            license(["reload", Filename]);
        #{key := _Key} ->
            ?PRINT_MSG("License is not configured as a file, please specify file explicitly~n")
    end;
license(["reload", Filename]) ->
    case emqx_license:update_file(Filename) of
        {ok, Warnings} ->
            ok = print_warnings(Warnings),
            ok = ?PRINT_MSG("ok~n");
        {error, Reason} ->
            ?PRINT("Error: ~p~n", [Reason])
    end;
license(["update", EncodedLicense]) ->
    case emqx_license:update_key(EncodedLicense) of
        {ok, Warnings} ->
            ok = print_warnings(Warnings),
            ok = ?PRINT_MSG("ok~n");
        {error, Reason} ->
            ?PRINT("Error: ~p~n", [Reason])
    end;
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
license(_) ->
    emqx_ctl:usage(
        [
            {"license info", "Show license info"},
            {"license reload [<File>]",
                "Reload license from a file specified with an absolute path"},
            {"license update License", "Update license given as a string"}
        ]
    ).

unload() ->
    ok = emqx_ctl:unregister_command(license).

print_warnings(Warnings) ->
    emqx_license_checker:print_warnings(Warnings).
