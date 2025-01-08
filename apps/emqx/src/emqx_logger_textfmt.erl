%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_logger_textfmt).

-include("emqx_trace.hrl").

-export([format/2]).
-export([check_config/1]).
-export([try_format_unicode/1, try_encode_meta/2]).
%% Used in the other log formatters
-export([evaluate_lazy_values_if_dbg_level/1, evaluate_lazy_values/1]).

check_config(X) ->
    logger_formatter:check_config(maps:without([timestamp_format, with_mfa, payload_encode], X)).

%% Principle here is to delegate the formatting to logger_formatter:format/2
%% as much as possible, and only enrich the report with clientid, peername, topic, username
format(#{msg := {report, ReportMap0}, meta := _Meta} = Event0, Config) when is_map(ReportMap0) ->
    #{msg := {report, ReportMap}, meta := Meta} = Event = evaluate_lazy_values_if_dbg_level(Event0),
    %% The most common case, when entering from SLOG macro
    %% i.e. logger:log(Level, #{msg => "my_msg", foo => bar})
    ReportList = enrich_report(ReportMap, Meta, Config),
    Report =
        case is_list_report_acceptable(Meta) of
            true ->
                ReportList;
            false ->
                maps:from_list(ReportList)
        end,
    fmt(Event#{msg := {report, Report}}, maps:remove(with_mfa, Config));
format(#{msg := {string, String}} = Event, Config) ->
    %% copied from logger_formatter:format/2
    %% unsure how this case is triggered
    format(Event#{msg => {"~ts ", [String]}}, maps:remove(with_mfa, Config));
format(#{msg := _Msg, meta := _Meta} = Event0, Config) ->
    #{msg := Msg0, meta := Meta} = Event1 = evaluate_lazy_values_if_dbg_level(Event0),
    %% For format strings like logger:log(Level, "~p", [Var])
    %% and logger:log(Level, "message", #{key => value})
    Msg1 = enrich_client_info(Msg0, Meta),
    Msg2 = enrich_mfa(Msg1, Meta, Config),
    Msg3 = enrich_topic(Msg2, Meta),
    fmt(Event1#{msg := Msg3}, maps:remove(with_mfa, Config)).

enrich_mfa({Fmt, Args}, Data, #{with_mfa := true} = Config) when is_list(Fmt) ->
    {Fmt ++ " mfa: ~ts", Args ++ [emqx_utils:format_mfal(Data, Config)]};
enrich_mfa(Msg, _, _) ->
    Msg.

%% Most log entries with lazy values are trace events with level debug. So to
%% be more efficient we only search for lazy values to evaluate in the entries
%% with level debug in the main log formatters.
evaluate_lazy_values_if_dbg_level(#{level := debug} = Map) ->
    evaluate_lazy_values(Map);
evaluate_lazy_values_if_dbg_level(Map) ->
    Map.

evaluate_lazy_values(Map) when is_map(Map) ->
    maps:map(fun evaluate_lazy_values_kv/2, Map);
evaluate_lazy_values({report, Report}) ->
    {report, evaluate_lazy_values(Report)};
evaluate_lazy_values(V) ->
    V.

evaluate_lazy_values_kv(_K, #emqx_trace_format_func_data{function = Formatter, data = V}) ->
    try
        NewV = Formatter(V),
        evaluate_lazy_values(NewV)
    catch
        _:_ ->
            V
    end;
evaluate_lazy_values_kv(_K, V) ->
    evaluate_lazy_values(V).

fmt(#{meta := #{time := Ts}} = Data, Config) ->
    Timestamp =
        case Config of
            #{timestamp_format := epoch} ->
                integer_to_list(Ts);
            _ ->
                % auto | rfc3339
                TimeOffset = maps:get(time_offset, Config, ""),
                calendar:system_time_to_rfc3339(Ts, [
                    {unit, microsecond},
                    {offset, TimeOffset},
                    {time_designator, $T}
                ])
        end,
    [Timestamp, " ", logger_formatter:format(Data, Config)].

%% Other report callbacks may only accept map() reports such as gen_server formatter
is_list_report_acceptable(#{report_cb := Cb}) ->
    Cb =:= fun logger:format_otp_report/1 orelse Cb =:= fun logger:format_report/1;
is_list_report_acceptable(_) ->
    false.

enrich_report(ReportRaw0, Meta, Config) ->
    %% clientid and peername always in emqx_conn's process metadata.
    %% topic and username can be put in meta using ?SLOG/3, or put in msg's report by ?SLOG/2
    ReportRaw = try_encode_meta(ReportRaw0, Config),
    Topic =
        case maps:get(topic, Meta, undefined) of
            undefined -> maps:get(topic, ReportRaw, undefined);
            Topic0 -> Topic0
        end,
    Username =
        case maps:get(username, Meta, undefined) of
            undefined -> maps:get(username, ReportRaw, undefined);
            Username0 -> Username0
        end,
    Tns = maps:get(tns, Meta, undefined),
    ClientId = maps:get(clientid, Meta, undefined),
    Peer = maps:get(peername, Meta, undefined),
    Msg = maps:get(msg, ReportRaw, undefined),
    MFA = emqx_utils:format_mfal(Meta, Config),
    %% TODO: move all tags to Meta so we can filter traces
    %% based on tags (currently not supported)
    Tag = maps:get(tag, ReportRaw, maps:get(tag, Meta, undefined)),
    %% turn it into a list so that the order of the fields is determined
    lists:foldl(
        fun
            ({_, undefined}, Acc) -> Acc;
            (Item, Acc) -> [Item | Acc]
        end,
        maps:to_list(maps:without([topic, msg, tns, clientid, username, tag], ReportRaw)),
        [
            {topic, try_format_unicode(Topic)},
            {username, try_format_unicode(Username)},
            {peername, Peer},
            {mfa, try_format_unicode(MFA)},
            {msg, Msg},
            {clientid, try_format_unicode(ClientId)},
            {tns, try_format_unicode(Tns)},
            {tag, Tag}
        ]
    ).

try_format_unicode(undefined) ->
    undefined;
try_format_unicode(Char) ->
    List =
        try
            case unicode:characters_to_list(Char) of
                {error, _, _} -> error;
                {incomplete, _, _} -> error;
                List1 -> List1
            end
        catch
            _:_ ->
                error
        end,
    case List of
        error -> io_lib:format("~0p", [Char]);
        _ -> List
    end.

enrich_client_info({Fmt, Args}, #{clientid := ClientId, peername := Peer}) when is_list(Fmt) ->
    {" ~ts@~ts " ++ Fmt, [ClientId, Peer | Args]};
enrich_client_info({Fmt, Args}, #{clientid := ClientId}) when is_list(Fmt) ->
    {" ~ts " ++ Fmt, [ClientId | Args]};
enrich_client_info({Fmt, Args}, #{peername := Peer}) when is_list(Fmt) ->
    {" ~ts " ++ Fmt, [Peer | Args]};
enrich_client_info(Msg, _) ->
    Msg.

enrich_topic({Fmt, Args}, #{topic := Topic}) when is_list(Fmt) ->
    {" topic: ~ts" ++ Fmt, [Topic | Args]};
enrich_topic(Msg, _) ->
    Msg.

try_encode_meta(Report, Config) ->
    lists:foldl(
        fun(Meta, Acc) ->
            try_encode_meta(Meta, Acc, Config)
        end,
        Report,
        [payload, packet]
    ).

try_encode_meta(payload, #{payload := Payload} = Report, #{payload_encode := Encode}) ->
    {Payload1, Encode1} = format_payload(Payload, Encode),
    Report#{payload => Payload1, payload_encode => Encode1};
try_encode_meta(packet, #{packet := Packet} = Report, #{payload_encode := Encode}) when
    is_tuple(Packet)
->
    Report#{packet := try_format_unicode(emqx_packet:format(Packet, Encode))};
try_encode_meta(_, Report, _Config) ->
    Report.

format_payload(undefined, Type) ->
    {"", Type};
format_payload(Payload, Type) when is_binary(Payload) ->
    {Payload1, Type1} = emqx_packet:format_payload(Payload, Type),
    {try_format_unicode(Payload1), Type1};
format_payload(Payload, Type) ->
    {Payload, Type}.
