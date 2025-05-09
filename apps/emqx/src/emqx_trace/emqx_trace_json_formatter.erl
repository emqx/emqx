%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_trace_json_formatter).

-include("emqx_mqtt.hrl").
-include("emqx_trace.hrl").

-export([format/2]).

%% logger_formatter:config/0 is not exported.
-type config() :: map().

-define(DEFAULT_FORMATTER, fun logger:format_otp_report/1).

%%%-----------------------------------------------------------------
%%% Callback Function
%%%-----------------------------------------------------------------

-spec format(LogEvent, Config) -> unicode:chardata() when
    LogEvent :: logger:log_event(),
    Config :: config().
format(
    LogMap,
    #{payload_fmt_opts := PayloadFmtOpts} = Config
) ->
    LogMap0 = emqx_logger_textfmt:evaluate_lazy_values(LogMap),
    LogMap1 = maybe_format_msg(LogMap0, Config),
    %% We just make some basic transformations on the input LogMap and then do
    %% an external call to create the JSON text
    Time = emqx_utils_calendar:now_to_rfc3339(microsecond),
    LogMap2 = LogMap1#{time => Time},
    LogMap3 = prepare_log_data(LogMap2, PayloadFmtOpts),
    [emqx_utils_json:best_effort_json(LogMap3, [force_utf8]), "\n"].

%%%-----------------------------------------------------------------
%%% Helper Functions
%%%-----------------------------------------------------------------

maybe_format_msg(#{msg := Msg, meta := Meta} = LogMap, Config) ->
    try do_maybe_format_msg(Msg, Meta, Config) of
        Map when is_map(Map) ->
            LogMap#{meta => maps:merge(Meta, Map), msg => maps:get(msg, Map, "no_message")};
        Bin when is_binary(Bin) ->
            LogMap#{msg => Bin}
    catch
        C:R:S ->
            Meta#{
                msg => "emqx_logger_jsonfmt_format_error",
                fmt_raw_input => Msg,
                fmt_error => C,
                fmt_reason => R,
                fmt_stacktrace => S,
                more => #{
                    original_log_entry => LogMap,
                    config => Config
                }
            }
    end.

do_maybe_format_msg(String, _Meta, _Config) when is_list(String); is_binary(String) ->
    unicode:characters_to_binary(String);
do_maybe_format_msg(undefined, _Meta, _Config) ->
    #{};
do_maybe_format_msg({report, Report} = Msg, #{report_cb := Cb} = Meta, Config) ->
    case is_map(Report) andalso Cb =:= ?DEFAULT_FORMATTER of
        true ->
            %% reporting a map without a customised format function
            Report;
        false ->
            emqx_logger_jsonfmt:format_msg(Msg, Meta, Config)
    end;
do_maybe_format_msg(Msg, Meta, Config) ->
    emqx_logger_jsonfmt:format_msg(Msg, Meta, Config).

prepare_log_data(LogMap, PayloadFmtOpts) when is_map(LogMap) ->
    NewKeyValuePairs = lists:flatmap(
        fun({K, V}) ->
            case prepare_key_value(K, V, PayloadFmtOpts) of
                KV when is_tuple(KV) ->
                    [KV];
                KVs when is_list(KVs) ->
                    KVs
            end
        end,
        maps:to_list(LogMap)
    ),
    maps:from_list(NewKeyValuePairs);
prepare_log_data(V, PayloadFmtOpts) when is_list(V) ->
    [prepare_log_data(Item, PayloadFmtOpts) || Item <- V];
prepare_log_data(V, PayloadFmtOpts) when is_tuple(V) ->
    List = erlang:tuple_to_list(V),
    PreparedList = [prepare_log_data(Item, PayloadFmtOpts) || Item <- List],
    erlang:list_to_tuple(PreparedList);
prepare_log_data(V, _PayloadFmtOpts) ->
    V.

prepare_key_value(host, {I1, I2, I3, I4} = IP, _PayloadFmtOpts) when
    is_integer(I1),
    is_integer(I2),
    is_integer(I3),
    is_integer(I4)
->
    %% We assume this is an IP address
    {host, unicode:characters_to_binary(inet:ntoa(IP))};
prepare_key_value(host, {I1, I2, I3, I4, I5, I6, I7, I8} = IP, _PayloadFmtOpts) when
    is_integer(I1),
    is_integer(I2),
    is_integer(I3),
    is_integer(I4),
    is_integer(I5),
    is_integer(I6),
    is_integer(I7),
    is_integer(I8)
->
    %% We assume this is an IP address
    {host, unicode:characters_to_binary(inet:ntoa(IP))};
prepare_key_value(payload = K, V, PayloadFmtOpts) ->
    {NewV, PEncode1} =
        try
            format_payload(V, PayloadFmtOpts)
        catch
            _:_ ->
                V
        end,
    [{K, NewV}, {payload_encode, PEncode1}];
prepare_key_value(<<"payload">>, V, PayloadFmtOpts) ->
    prepare_key_value(payload, V, PayloadFmtOpts);
prepare_key_value(packet = K, V, PayloadFmtOpts) ->
    NewV =
        try
            format_packet(V, PayloadFmtOpts)
        catch
            _:_ ->
                V
        end,
    {K, NewV};
prepare_key_value(K, {recoverable_error, Msg} = OrgV, PayloadFmtOpts) ->
    try
        prepare_key_value(
            K,
            #{
                error_type => recoverable_error,
                msg => Msg,
                additional_info => <<"The operation may be retried.">>
            },
            PayloadFmtOpts
        )
    catch
        _:_ ->
            {K, OrgV}
    end;
prepare_key_value(rule_ids = K, V, _PayloadFmtOpts) ->
    NewV =
        try
            format_map_set_to_list(V)
        catch
            _:_ ->
                V
        end,
    {K, NewV};
prepare_key_value(client_ids = K, V, _PayloadFmtOpts) ->
    NewV =
        try
            format_map_set_to_list(V)
        catch
            _:_ ->
                V
        end,
    {K, NewV};
prepare_key_value(action_id = K, V, _PayloadFmtOpts) ->
    try
        {action_info, format_action_info(V)}
    catch
        _:_ ->
            {K, V}
    end;
prepare_key_value(K, V, PayloadFmtOpts) ->
    {K, prepare_log_data(V, PayloadFmtOpts)}.

format_packet(undefined, _) -> "";
format_packet(Packet, PayloadFmtOpts) -> emqx_packet:format(Packet, PayloadFmtOpts).

format_payload(undefined, #{payload_encode := Type}) ->
    {"", Type};
format_payload(Payload, PayloadFmtOpts) when is_binary(Payload) ->
    emqx_packet:format_payload(Payload, PayloadFmtOpts);
format_payload(Payload, #{payload_encode := Type}) ->
    {Payload, Type}.

format_map_set_to_list(Map) ->
    Items = [
        begin
            %% Assert that it is really a map set
            true = V,
            %% Assert that the keys have the expected type
            true = is_binary(K),
            K
        end
     || {K, V} <- maps:to_list(Map)
    ],
    lists:sort(Items).

format_action_info(#{mod := _Mod, func := _Func} = FuncCall) ->
    FuncCall;
format_action_info(V) ->
    [<<"action">>, Type, Name | _] = binary:split(V, <<":">>, [global]),
    #{
        type => Type,
        name => Name
    }.
