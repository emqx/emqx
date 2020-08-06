%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_telemetry).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").

-logger_header("[Telemetry]").

-export([ start_link/1
        , stop/0
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([ enable/0
        , disable/0
        , os_info/0
        , license/0
        , uptime/0
        , emqx_version/0
        , otp_version/0
        , generate_uuid/0
        % , get_uuid/0
        ]).

-import(proplists, [ get_value/2
                   , get_value/3
                   ]).

-record(telemetry, {
          id :: non_neg_integer(),

          uuid :: binary(),

          enabled :: boolean()
        }).

-record(state, {
          uuid :: binary(),

          enabled :: boolean(),

          report_interval :: non_neg_integer(),

          timer = undefined :: undefined | reference()
        }).

%% The count of 100-nanosecond intervals between the UUID epoch 
%% 1582-10-15 00:00:00 and the UNIX epoch 1970-01-01 00:00:00.
-define(GREGORIAN_EPOCH_OFFSET, 16#01b21dd213814000).

-define(UNIQUE_ID, 9527).

-define(TELEMETRY, emqx_telemetry).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

stop() ->
    gen_server:stop(?MODULE).

enable() ->
    gen_server:call(?MODULE, enable).

disable() ->
    gen_server:call(?MODULE, disable).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Opts]) ->
    ok = ekka_mnesia:create_table(?TELEMETRY,
             [{type, set},
              {disc_copies, [node()]},
              {local_content, true},
              {record_name, telemetry},
              {attributes, record_info(fields, telemetry)}]),
    case mnesia:dirty_read(?TELEMETRY, ?UNIQUE_ID) of
        [] ->
            Enabled = get_value(enabled, Opts),
            UUID = generate_uuid(),
            mnesia:dirty_write(?TELEMETRY, #telemetry{id = ?UNIQUE_ID,
                                                      uuid = UUID,
                                                      enabled = Enabled}),
            {ok, ensure_report_timer(#state{report_interval = timer:hours(7 * 24),
                                            uuid = UUID,
                                            enabled = Enabled})};
        [#telemetry{uuid = UUID, enabled = Enabled} | _] ->
            {ok, ensure_report_timer(#state{report_interval = timer:hours(7 * 24),
                                            uuid = UUID,
                                            enabled = Enabled})}
    end.

handle_call(enable, _From, State = #state{uuid = UUID}) ->
    mnesia:dirty_write(?TELEMETRY, #telemetry{id = ?UNIQUE_ID,
                                              uuid = UUID,
                                              enabled = true}),
    {reply, ensure_report_timer(State#state{enabled = true})};

handle_call(disable, _From, State = #state{uuid = UUID}) ->
    mnesia:dirty_write(?TELEMETRY, #telemetry{id = ?UNIQUE_ID,
                                              uuid = UUID,
                                              enabled = false}),
    {reply, State#state{enabled = false}};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, TRef, time_to_report_telemetry_data}, State = #state{timer = TRef,
                                                                           enabled = false}) ->
    {noreply, State};
handle_info({timeout, TRef, time_to_report_telemetry_data}, State = #state{timer = TRef,
                                                                           uuid = UUID}) ->
    OSInfo = os_info(),
    [{emqx_version, emqx_version()},
     {license, license()},
     {os_name, get_value(os_name, OSInfo)},
     {os_version, get_value(os_version, OSInfo)},
     {otp_version, otp_version()},
     {up_time, uptime()},
     {uuid, UUID}],
    
    {noreply, ensure_report_timer(State)};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

ensure_report_timer(State = #state{report_interval = ReportInterval}) ->
    State#state{timer = emqx_misc:start_timer(ReportInterval, time_to_report_telemetry_data)}.

emqx_version() ->
    {ok, Version} = application:get_key(emqx, vsn),
    Version.

license() ->
    case application:get_key(emqx, description) of
        {ok, "EMQ X Broker"} ->
            [{edition, "community"}];
        {ok, "EMQ X Enterprise"} ->
            case erlang:function_exported(emqx_license_mgr, info, 0) of
                false ->
                    [{edition, "enterprise"}];
                true ->
                    LicenseInfo = emqx_license_mgr:info(),
                    [{edition, "enterprise"},
                     {kind, get_value(type, LicenseInfo)}]
            end
    end.

os_info() ->
    case erlang:system_info(os_type) of
        {unix,darwin} ->
            [Name | _] = string:tokens(os:cmd("sw_vers -productName"), "\n"),
            [Version | _] = string:tokens(os:cmd("sw_vers -productVersion"), "\n"),
            [{os_name, Name},
             {os_version, Version}];
        {unix, _} ->
            case file:read_file_info("/etc/os-release") of
                {error, enonet} ->
                    [{os_name, "Unknown"},
                     {os_version, "Unknown"}];
                {ok, FileInfo} ->
                    case get_value(FileInfo, access) of
                        Access when Access =:= read orelse Access =:= read_write ->
                            OSInfo = lists:foldl(fun(Line, Acc) ->
                                                     [Var, Value] = string:tokens(Line, "="),
                                                     [{Var, Value} | Acc]
                                                 end, [], string:tokens(os:cmd("cat /etc/os-release"), "\n")),
                            [{os_name, get_value("NAME", OSInfo),
                              os_version, get_value("VERSION", OSInfo, get_value("VERSION_ID", OSInfo))}];
                        _ ->
                            [{os_name, "Unknown"},
                             {os_version, "Unknown"}]
                    end
            end;
        {win32, nt} ->
            Ver = os:cmd("ver"),
            case re:run(Ver, "[a-zA-Z ]+ \\[Version ([0-9]+[\.])+[0-9]+\\]", [{capture, none}]) of
                match ->
                    [NVer | _] = string:tokens(Ver, "\r\n"),
                    {match, [Version]} = re:run(NVer, "([0-9]+[\.])+[0-9]+", [{capture, first, list}]),
                    [Name | _] = string:split(NVer, " [Version "),
                    [{os_name, Name},
                     {os_version, Version}];
                nomatch ->
                    [{os_name, "Unknown"},
                     {os_version, "Unknown"}]
            end
    end.

otp_version() ->
    erlang:system_info(otp_release).

uptime() ->
    element(1, erlang:statistics(wall_clock)).

generate_uuid() ->
    MicroSeconds = erlang:system_time(microsecond),
    Timestamp = MicroSeconds * 10 + ?GREGORIAN_EPOCH_OFFSET,
    <<TimeHigh:12, TimeMid:16, TimeLow:32>> = <<Timestamp:60>>,
    <<ClockSeq:32>> = crypto:strong_rand_bytes(4),
    <<First:7, _:1, Last:40>> = crypto:strong_rand_bytes(6),
    <<NTimeHigh:16>> = <<16#01:4, TimeHigh:12>>,
    <<NClockSeq:16>> = <<1:1, 0:1, ClockSeq:14>>,
    <<Node:48>> = <<First:7, 1:1, Last:40>>,
    list_to_binary(io_lib:format("~.16B-~.16B-~.16B-~.16B-~.16B", [TimeLow, TimeMid, NTimeHigh, NClockSeq, Node])).