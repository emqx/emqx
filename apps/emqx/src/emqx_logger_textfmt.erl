%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

format(#{msg := {report, Report0}, meta := Meta} = Event, Config) when is_map(Report0) ->
    Report1 = enrich_report_mfa(Report0, Meta),
    Report2 = enrich_report_clientid(Report1, Meta),
    Report3 = enrich_report_peername(Report2, Meta),
    Report4 = enrich_report_topic(Report3, Meta),
    logger_formatter:format(Event#{msg := {report, Report4}}, Config);
format(#{msg := {string, String}} = Event, Config) ->
    format(Event#{msg => {"~ts ", [String]}}, Config);
format(#{msg := Msg0, meta := Meta} = Event, Config) ->
    Msg1 = enrich_client_info(Msg0, Meta),
    Msg2 = enrich_mfa(Msg1, Meta),
    Msg3 = enrich_topic(Msg2, Meta),
    logger_formatter:format(Event#{msg := Msg3}, Config).

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

enrich_report_mfa(Report, #{mfa := Mfa, line := Line}) ->
    Report#{mfa => mfa(Mfa), line => Line};
enrich_report_mfa(Report, _) ->
    Report.

enrich_report_clientid(Report, #{clientid := ClientId}) ->
    Report#{clientid => try_format_unicode(ClientId)};
enrich_report_clientid(Report, _) ->
    Report.

enrich_report_peername(Report, #{peername := Peername}) ->
    Report#{peername => Peername};
enrich_report_peername(Report, _) ->
    Report.

%% clientid and peername always in emqx_conn's process metadata.
%% topic can be put in meta using ?SLOG/3, or put in msg's report by ?SLOG/2
enrich_report_topic(Report, #{topic := Topic}) ->
    Report#{topic => try_format_unicode(Topic)};
enrich_report_topic(Report = #{topic := Topic}, _) ->
    Report#{topic => try_format_unicode(Topic)};
enrich_report_topic(Report, _) ->
    Report.

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
