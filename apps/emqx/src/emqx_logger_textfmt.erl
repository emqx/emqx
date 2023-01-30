%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([format/2]).
-export([check_config/1]).
-export([try_format_unicode/1]).

check_config(X) -> logger_formatter:check_config(X).

format(#{msg := {report, ReportMap}, meta := Meta} = Event, Config) when is_map(ReportMap) ->
    Report = enrich_report(ReportMap, Meta),
    logger_formatter:format(Event#{msg := {report, Report}}, Config);
format(#{msg := {string, String}} = Event, Config) ->
    format(Event#{msg => {"~ts ", [String]}}, Config);
%% trace
format(#{msg := Msg0, meta := Meta} = Event, Config) ->
    Msg1 = enrich_client_info(Msg0, Meta),
    Msg2 = enrich_mfa(Msg1, Meta),
    Msg3 = enrich_topic(Msg2, Meta),
    logger_formatter:format(Event#{msg := Msg3}, Config).

enrich_report(ReportRaw, Meta) ->
    %% clientid and peername always in emqx_conn's process metadata.
    %% topic can be put in meta using ?SLOG/3, or put in msg's report by ?SLOG/2
    Topic =
        case maps:get(topic, Meta, undefined) of
            undefined -> maps:get(topic, ReportRaw, undefined);
            Topic0 -> Topic0
        end,
    ClientId = maps:get(clientid, Meta, undefined),
    Peer = maps:get(peername, Meta, undefined),
    MFA = maps:get(mfa, Meta, undefined),
    Line = maps:get(line, Meta, undefined),
    Msg = maps:get(msg, ReportRaw, undefined),
    lists:foldl(
        fun
            ({_, undefined}, Acc) -> Acc;
            (Item, Acc) -> [Item | Acc]
        end,
        maps:to_list(maps:without([topic, msg, clientid], ReportRaw)),
        [
            {topic, try_format_unicode(Topic)},
            {clientid, try_format_unicode(ClientId)},
            {peername, Peer},
            {line, Line},
            {mfa, mfa(MFA)},
            {msg, Msg}
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
                Binary -> Binary
            end
        catch
            _:_ ->
                error
        end,
    case List of
        error -> io_lib:format("~0p", [Char]);
        _ -> List
    end.

enrich_mfa({Fmt, Args}, #{mfa := Mfa, line := Line}) when is_list(Fmt) ->
    {Fmt ++ " mfa: ~ts line: ~w", Args ++ [mfa(Mfa), Line]};
enrich_mfa(Msg, _) ->
    Msg.

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

mfa({M, F, A}) -> atom_to_list(M) ++ ":" ++ atom_to_list(F) ++ "/" ++ integer_to_list(A).
