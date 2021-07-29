%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_telemetry).

-behaviour(gen_server).

-behaviour(emqx_gen_mod).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-include_lib("kernel/include/file.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_modules.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([ start_link/1
        , stop/0
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_continue/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% emqx_gen_mod callbacks
-export([ load/1
        , unload/1
        , description/0
        ]).

-export([ get_status/0
        , get_uuid/0
        , get_telemetry/0
        ]).

-export([official_version/1]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-import(proplists, [ get_value/2
                   , get_value/3
                   ]).

-record(telemetry, {
    id :: non_neg_integer(),
    uuid :: binary()
}).

-record(state, {
    uuid :: undefined | binary(),
    enabled :: undefined | boolean(),
    url :: string(),
    report_interval :: undefined | non_neg_integer(),
    timer = undefined :: undefined | reference()
}).

%% The count of 100-nanosecond intervals between the UUID epoch
%% 1582-10-15 00:00:00 and the UNIX epoch 1970-01-01 00:00:00.
-define(GREGORIAN_EPOCH_OFFSET, 16#01b21dd213814000).

-define(UNIQUE_ID, 9527).

-define(TELEMETRY, emqx_telemetry).

-rlog_shard({?COMMON_SHARD, ?TELEMETRY}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TELEMETRY,
             [{type, set},
              {disc_copies, [node()]},
              {local_content, true},
              {record_name, telemetry},
              {attributes, record_info(fields, telemetry)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TELEMETRY, disc_copies).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

stop() ->
    gen_server:stop(?MODULE).

load(_Env) ->
    gen_server:call(?MODULE, enable).

unload(_Env) ->
    gen_server:call(?MODULE, disable).

get_status() ->
    gen_server:call(?MODULE, get_status).

get_uuid() ->
    gen_server:call(?MODULE, get_uuid).

get_telemetry() ->
    gen_server:call(?MODULE, get_telemetry).

description() ->
    "".

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

%% This is to suppress dialyzer warnings for mnesia:dirty_write and
%% dirty_read race condition. Given that the init function is not evaluated
%% concurrently in one node, it should be free of race condition.
%% Given the chance of having two nodes bootstraping with the write
%% is very small, it should be safe to ignore.
-dialyzer([{nowarn_function, [init/1]}]).
init(_Opts) ->
    UUID1 = case mnesia:dirty_read(?TELEMETRY, ?UNIQUE_ID) of
        [] ->
            UUID = generate_uuid(),
            ekka_mnesia:dirty_write(?TELEMETRY, #telemetry{id = ?UNIQUE_ID,
                                                           uuid = UUID}),
            UUID;
        [#telemetry{uuid = UUID} | _] ->
            UUID
    end,
    {ok, #state{url = ?TELEMETRY_URL,
                report_interval = timer:seconds(?REPORT_INTERVAR),
                enabled = false,
                uuid = UUID1}}.

handle_call(enable, _From, State) ->
    case ?MODULE:official_version(emqx_app:get_release()) of
        true ->
            report_telemetry(State),
            {reply, ok, ensure_report_timer(State#state{enabled = true})};
        false ->
            {reply, {error, not_official_version}, State}
    end;

handle_call(disable, _From, State = #state{timer = Timer}) ->
    case ?MODULE:official_version(emqx_app:get_release()) of
        true ->
            emqx_misc:cancel_timer(Timer),
            {reply, ok, State#state{enabled = false, timer = undefined}};
        false ->
            {reply, {error, not_official_version}, State}
    end;

handle_call(get_status, _From, State = #state{enabled = Enabled}) ->
    {reply, Enabled, State};

handle_call(get_uuid, _From, State = #state{uuid = UUID}) ->
    {reply, {ok, UUID}, State};

handle_call(get_telemetry, _From, State) ->
    {reply, {ok, get_telemetry(State)}, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_continue(Continue, State) ->
    ?LOG(error, "Unexpected continue: ~p", [Continue]),
    {noreply, State}.

handle_info({timeout, TRef, time_to_report_telemetry_data}, State = #state{timer = TRef,
                                                                           enabled = false}) ->
    {noreply, State};
handle_info({timeout, TRef, time_to_report_telemetry_data}, State = #state{timer = TRef}) ->
    report_telemetry(State),
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

official_version(Version) ->
    Pt = "^\\d+\\.\\d+(?:(-(?:alpha|beta|rc)\\.[1-9][0-9]*)|\\.\\d+)$",
    match =:= re:run(Version, Pt, [{capture, none}]).

ensure_report_timer(State = #state{report_interval = ReportInterval}) ->
    State#state{timer = emqx_misc:start_timer(ReportInterval, time_to_report_telemetry_data)}.

os_info() ->
    case erlang:system_info(os_type) of
        {unix,darwin} ->
            [Name | _] = string:tokens(os:cmd("sw_vers -productName"), "\n"),
            [Version | _] = string:tokens(os:cmd("sw_vers -productVersion"), "\n"),
            [{os_name, Name},
             {os_version, Version}];
        {unix, _} ->
            case file:read_file_info("/etc/os-release") of
                {error, _} ->
                    [{os_name, "Unknown"},
                     {os_version, "Unknown"}];
                {ok, FileInfo} ->
                    case FileInfo#file_info.access of
                        Access when Access =:= read orelse Access =:= read_write ->
                            OSInfo = lists:foldl(fun(Line, Acc) ->
                                                     [Var, Value] = string:tokens(Line, "="),
                                                     NValue = case Value of
                                                                  _ when is_list(Value) ->
                                                                      lists:nth(1, string:tokens(Value, "\""));
                                                                  _ ->
                                                                      Value
                                                              end,
                                                     [{Var, NValue} | Acc]
                                                 end, [], string:tokens(os:cmd("cat /etc/os-release"), "\n")),
                            [{os_name, get_value("NAME", OSInfo)},
                             {os_version, get_value("VERSION", OSInfo, get_value("VERSION_ID", OSInfo))}];
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

nodes_uuid() ->
    Nodes = lists:delete(node(), ekka_mnesia:running_nodes()),
    lists:foldl(fun(Node, Acc) ->
                    case rpc:call(Node, ?MODULE, get_uuid, []) of
                        {badrpc, _Reason} ->
                            Acc;
                        {ok, UUID} ->
                            [UUID | Acc]
                    end
                end, [], Nodes).

active_plugins() ->
    lists:foldl(fun(#plugin{name = Name, active = Active}, Acc) ->
                        case Active of
                            true -> [Name | Acc];
                            false -> Acc
                        end
                    end, [], emqx_plugins:list()).

active_modules() ->
    emqx_modules:list().

num_clients() ->
    emqx_stats:getstat('connections.max').

messages_sent() ->
    emqx_metrics:val('messages.sent').

messages_received() ->
    emqx_metrics:val('messages.received').

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

get_telemetry(#state{uuid = UUID}) ->
    OSInfo = os_info(),
    [{emqx_version, bin(emqx_app:get_release())},
     {license, [{edition, <<"community">>}]},
     {os_name, bin(get_value(os_name, OSInfo))},
     {os_version, bin(get_value(os_version, OSInfo))},
     {otp_version, bin(otp_version())},
     {up_time, uptime()},
     {uuid, UUID},
     {nodes_uuid, nodes_uuid()},
     {active_plugins, active_plugins()},
     {active_modules, active_modules()},
     {num_clients, num_clients()},
     {messages_received, messages_received()},
     {messages_sent, messages_sent()}].

report_telemetry(State = #state{url = URL}) ->
    Data = get_telemetry(State),
    case emqx_json:safe_encode(Data) of
        {ok, Bin} ->
            httpc_request(post, URL, [], Bin),
            ?tp(debug, telemetry_data_reported, #{});
        {error, Reason} ->
            %% debug? why?
            ?tp(debug, telemetry_data_encode_error, #{data => Data, reason => Reason})
    end.

httpc_request(Method, URL, Headers, Body) ->
    httpc:request(Method, {URL, Headers, "application/json", Body}, [], []).

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.
