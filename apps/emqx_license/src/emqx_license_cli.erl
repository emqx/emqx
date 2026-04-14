%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

license(["update", EncodedLicense]) ->
    case emqx_license:update_key(EncodedLicense) of
        {ok, Warnings} ->
            ok = print_warnings(Warnings),
            ok = ?PRINT_MSG("ok~n");
        {error, Reason} when is_atom(Reason) ->
            ?PRINT("Error: ~s~n", [Reason]);
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
license(["history" | Args]) ->
    case parse_history_args(Args, #{json => false, limit => 24, period => monthly}) of
        {ok, #{json := IsJSON, limit := Limit, period := Period}} ->
            Rows = emqx_license_session_hwm:list_history(Period, Limit),
            print_history(Period, IsJSON, Rows);
        {error, Reason} ->
            ?PRINT("Error: ~ts~n", [Reason]),
            usage()
    end;
license(_) ->
    usage().

unload() ->
    ok = emqx_ctl:unregister_command(license).

print_warnings(Warnings) ->
    emqx_license_checker:print_warnings(Warnings).

usage() ->
    emqx_ctl:usage(
        [
            {"license info", "Show license info"},
            {"license history [N] [--period daily|monthly] [--json]",
                "Show session high-watermark history in plain text or JSON"},
            {"license update '<License>'|'file:///tmp/emqx.lic'",
                "Update license given as a string\nor referenced by a file path via 'file://' prefix"}
        ]
    ).

parse_history_args([], Acc) ->
    {ok, Acc};
parse_history_args(["--json" | Rest], Acc) ->
    parse_history_args(Rest, Acc#{json => true});
parse_history_args(["--period", "daily" | Rest], Acc) ->
    parse_history_args(Rest, Acc#{period => daily});
parse_history_args(["--period", "monthly" | Rest], Acc) ->
    parse_history_args(Rest, Acc#{period => monthly});
parse_history_args(["--period", _ | _], _Acc) ->
    {error, <<"bad_history_period">>};
parse_history_args([Arg | Rest], Acc) ->
    case string:to_integer(Arg) of
        {Limit, []} when Limit > 0 ->
            parse_history_args(Rest, Acc#{limit => Limit});
        _ ->
            {error, <<"bad_history_argument">>}
    end.

print_history(Period, true = _IsJSON, Rows) ->
    Payload = #{
        <<"period">> => atom_to_binary(Period),
        <<"count">> => length(Rows),
        <<"data">> => [format_json_row(Row) || Row <- Rows]
    },
    ?PRINT("~ts~n", [emqx_utils_json:best_effort_json(Payload)]);
print_history(_Period, false = _IsJSON, []) ->
    ?PRINT_MSG("No session high-watermark history recorded.~n");
print_history(_Period, false = _IsJSON, Rows) ->
    lists:foreach(
        fun(#{period := Period, high_watermark := HighWatermark, observed_at := ObservedAt}) ->
            ?PRINT(
                "period=~ts high_watermark=~p observed_at=~ts~n",
                [Period, HighWatermark, ObservedAt]
            )
        end,
        Rows
    ).

format_json_row(#{period := Period, high_watermark := HighWatermark, observed_at := ObservedAt}) ->
    #{
        <<"period">> => Period,
        <<"high_watermark">> => HighWatermark,
        <<"observed_at">> => ObservedAt
    }.
