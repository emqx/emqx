%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-include_lib("kernel/include/file.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_modules.hrl").

-export([
    start_link/0,
    stop/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([
    enable/0,
    disable/0
]).

-export([
    get_uuid/0,
    get_telemetry/0,
    get_status/0
]).

-export([official_version/1]).

%% internal export
-export([read_raw_build_info/0]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-import(proplists, [
    get_value/2,
    get_value/3
]).

-record(telemetry, {
    id :: non_neg_integer(),
    uuid :: binary()
}).

-record(state, {
    uuid :: undefined | binary(),
    url :: string(),
    report_interval :: non_neg_integer(),
    timer = undefined :: undefined | reference(),
    previous_metrics = #{} :: map()
}).

-type state() :: #state{}.

%% The count of 100-nanosecond intervals between the UUID epoch
%% 1582-10-15 00:00:00 and the UNIX epoch 1970-01-01 00:00:00.
-define(GREGORIAN_EPOCH_OFFSET, 16#01b21dd213814000).

-define(UNIQUE_ID, 9527).

-define(TELEMETRY, emqx_telemetry).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    ok = mria:create_table(
        ?TELEMETRY,
        [
            {type, set},
            {storage, disc_copies},
            {local_content, true},
            {record_name, telemetry},
            {attributes, record_info(fields, telemetry)}
        ]
    ),
    _ = mria:wait_for_tables([?TELEMETRY]),
    Opts = emqx:get_config([telemetry], #{}),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

stop() ->
    gen_server:stop(?MODULE).

enable() ->
    gen_server:call(?MODULE, enable).

disable() ->
    gen_server:call(?MODULE, disable).

get_status() ->
    emqx_conf:get([telemetry, enable], true).

get_uuid() ->
    gen_server:call(?MODULE, get_uuid).

get_telemetry() ->
    gen_server:call(?MODULE, get_telemetry).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

%% This is to suppress dialyzer warnings for mria:dirty_write and
%% dirty_read race condition. Given that the init function is not evaluated
%% concurrently in one node, it should be free of race condition.
%% Given the chance of having two nodes bootstraping with the write
%% is very small, it should be safe to ignore.
-dialyzer([{nowarn_function, [init/1]}]).
init(_Opts) ->
    State0 = empty_state(),
    UUID1 =
        case mnesia:dirty_read(?TELEMETRY, ?UNIQUE_ID) of
            [] ->
                UUID = generate_uuid(),
                mria:dirty_write(?TELEMETRY, #telemetry{
                    id = ?UNIQUE_ID,
                    uuid = UUID
                }),
                UUID;
            [#telemetry{uuid = UUID} | _] ->
                UUID
        end,
    {ok, State0#state{uuid = UUID1}}.

handle_call(enable, _From, State0) ->
    case ?MODULE:official_version(emqx_app:get_release()) of
        true ->
            State = report_telemetry(State0),
            {reply, ok, ensure_report_timer(State)};
        false ->
            {reply, {error, not_official_version}, State0}
    end;
handle_call(disable, _From, State = #state{timer = Timer}) ->
    case ?MODULE:official_version(emqx_app:get_release()) of
        true ->
            emqx_misc:cancel_timer(Timer),
            {reply, ok, State#state{timer = undefined}};
        false ->
            {reply, {error, not_official_version}, State}
    end;
handle_call(get_uuid, _From, State = #state{uuid = UUID}) ->
    {reply, {ok, UUID}, State};
handle_call(get_telemetry, _From, State) ->
    {_State, Telemetry} = get_telemetry(State),
    {reply, {ok, Telemetry}, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_continue(Continue, State) ->
    ?SLOG(error, #{msg => "unexpected_continue", continue => Continue}),
    {noreply, State}.

handle_info({timeout, TRef, time_to_report_telemetry_data}, State0 = #state{timer = TRef}) ->
    State =
        case get_status() of
            true -> report_telemetry(State0);
            false -> State0
        end,
    {noreply, ensure_report_timer(State)};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
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
        {unix, darwin} ->
            [Name | _] = string:tokens(os:cmd("sw_vers -productName"), "\n"),
            [Version | _] = string:tokens(os:cmd("sw_vers -productVersion"), "\n"),
            [
                {os_name, Name},
                {os_version, Version}
            ];
        {unix, _} ->
            case file:read_file("/etc/os-release") of
                {error, _} ->
                    [
                        {os_name, "Unknown"},
                        {os_version, "Unknown"}
                    ];
                {ok, FileContent} ->
                    OSInfo = parse_os_release(FileContent),
                    [
                        {os_name, get_value("NAME", OSInfo)},
                        {os_version,
                            get_value(
                                "VERSION",
                                OSInfo,
                                get_value(
                                    "VERSION_ID",
                                    OSInfo,
                                    get_value("PRETTY_NAME", OSInfo)
                                )
                            )}
                    ]
            end;
        {win32, nt} ->
            Ver = os:cmd("ver"),
            case re:run(Ver, "[a-zA-Z ]+ \\[Version ([0-9]+[\.])+[0-9]+\\]", [{capture, none}]) of
                match ->
                    [NVer | _] = string:tokens(Ver, "\r\n"),
                    {match, [Version]} =
                        re:run(NVer, "([0-9]+[\.])+[0-9]+", [{capture, first, list}]),
                    [Name | _] = string:split(NVer, " [Version "),
                    [
                        {os_name, Name},
                        {os_version, Version}
                    ];
                nomatch ->
                    [
                        {os_name, "Unknown"},
                        {os_version, "Unknown"}
                    ]
            end
    end.

otp_version() ->
    erlang:system_info(otp_release).

uptime() ->
    element(1, erlang:statistics(wall_clock)).

nodes_uuid() ->
    Nodes = lists:delete(node(), mria_mnesia:running_nodes()),
    lists:foldl(
        fun(Node, Acc) ->
            case emqx_telemetry_proto_v1:get_uuid(Node) of
                {badrpc, _Reason} ->
                    Acc;
                {ok, UUID} ->
                    [UUID | Acc]
            end
        end,
        [],
        Nodes
    ).

active_plugins() ->
    lists:foldl(
        fun(#plugin{name = Name, active = Active}, Acc) ->
            case Active of
                true -> [Name | Acc];
                false -> Acc
            end
        end,
        [],
        emqx_plugins:list()
    ).

num_clients() ->
    emqx_stats:getstat('connections.max').

messages_sent() ->
    emqx_metrics:val('messages.sent').

messages_received() ->
    emqx_metrics:val('messages.received').

topic_count() ->
    emqx_stats:getstat('topics.count').

generate_uuid() ->
    MicroSeconds = erlang:system_time(microsecond),
    Timestamp = MicroSeconds * 10 + ?GREGORIAN_EPOCH_OFFSET,
    <<TimeHigh:12, TimeMid:16, TimeLow:32>> = <<Timestamp:60>>,
    <<ClockSeq:32>> = crypto:strong_rand_bytes(4),
    <<First:7, _:1, Last:40>> = crypto:strong_rand_bytes(6),
    <<NTimeHigh:16>> = <<16#01:4, TimeHigh:12>>,
    <<NClockSeq:16>> = <<1:1, 0:1, ClockSeq:14>>,
    <<Node:48>> = <<First:7, 1:1, Last:40>>,
    list_to_binary(
        io_lib:format(
            "~.16B-~.16B-~.16B-~.16B-~.16B",
            [TimeLow, TimeMid, NTimeHigh, NClockSeq, Node]
        )
    ).

-spec get_telemetry(state()) -> {state(), proplists:proplist()}.
get_telemetry(State0 = #state{uuid = UUID}) ->
    OSInfo = os_info(),
    {MQTTRTInsights, State} = mqtt_runtime_insights(State0),
    {State, [
        {emqx_version, bin(emqx_app:get_release())},
        {license, [{edition, <<"community">>}]},
        {os_name, bin(get_value(os_name, OSInfo))},
        {os_version, bin(get_value(os_version, OSInfo))},
        {otp_version, bin(otp_version())},
        {up_time, uptime()},
        {uuid, UUID},
        {nodes_uuid, nodes_uuid()},
        {active_plugins, active_plugins()},
        {num_clients, num_clients()},
        {messages_received, messages_received()},
        {messages_sent, messages_sent()},
        {build_info, build_info()},
        {vm_specs, vm_specs()},
        {mqtt_runtime_insights, MQTTRTInsights},
        {advanced_mqtt_features, advanced_mqtt_features()}
    ]}.

report_telemetry(State0 = #state{url = URL}) ->
    {State, Data} = get_telemetry(State0),
    case emqx_json:safe_encode(Data) of
        {ok, Bin} ->
            httpc_request(post, URL, [], Bin),
            ?tp(debug, telemetry_data_reported, #{});
        {error, Reason} ->
            %% debug? why?
            ?tp(debug, telemetry_data_encode_error, #{data => Data, reason => Reason})
    end,
    State.

httpc_request(Method, URL, Headers, Body) ->
    HTTPOptions = [{timeout, 10_000}],
    Options = [],
    httpc:request(Method, {URL, Headers, "application/json", Body}, HTTPOptions, Options).

parse_os_release(FileContent) ->
    lists:foldl(
        fun(Line, Acc) ->
            [Var, Value] = string:tokens(Line, "="),
            NValue =
                case Value of
                    _ when is_list(Value) ->
                        lists:nth(1, string:tokens(Value, "\""));
                    _ ->
                        Value
                end,
            [{Var, NValue} | Acc]
        end,
        [],
        string:tokens(binary:bin_to_list(FileContent), "\n")
    ).

build_info() ->
    case ?MODULE:read_raw_build_info() of
        {ok, BuildInfo} ->
            %% running on EMQX release
            {ok, Fields} = hocon:binary(BuildInfo),
            Fields;
        _ ->
            #{}
    end.

read_raw_build_info() ->
    Filename = filename:join([
        code:root_dir(),
        "releases",
        emqx_app:get_release(),
        "BUILD_INFO"
    ]),
    file:read_file(Filename).

vm_specs() ->
    SysMemData = memsup:get_system_memory_data(),
    [
        {num_cpus, erlang:system_info(logical_processors)},
        {total_memory, proplists:get_value(total_memory, SysMemData)}
    ].

-spec mqtt_runtime_insights(state()) -> {map(), state()}.
mqtt_runtime_insights(State0) ->
    {MQTTRates, State} = update_mqtt_rates(State0),
    MQTTRTInsights = MQTTRates#{num_topics => topic_count()},
    {MQTTRTInsights, State}.

-spec update_mqtt_rates(state()) -> {map(), state()}.
update_mqtt_rates(
    State = #state{
        previous_metrics = PrevMetrics0,
        report_interval = ReportInterval
    }
) when
    is_integer(ReportInterval), ReportInterval > 0
->
    MetricsToCheck =
        [
            {messages_sent_rate, messages_sent, fun messages_sent/0},
            {messages_received_rate, messages_received, fun messages_received/0}
        ],
    {Metrics, PrevMetrics} =
        lists:foldl(
            fun({RateKey, CountKey, Fun}, {Rates0, PrevMetrics1}) ->
                NewCount = Fun(),
                OldCount = maps:get(CountKey, PrevMetrics1, 0),
                Rate = (NewCount - OldCount) / ReportInterval,
                Rates = Rates0#{RateKey => Rate},
                PrevMetrics2 = PrevMetrics1#{CountKey => NewCount},
                {Rates, PrevMetrics2}
            end,
            {#{}, PrevMetrics0},
            MetricsToCheck
        ),
    {Metrics, State#state{previous_metrics = PrevMetrics}};
update_mqtt_rates(State) ->
    {#{}, State}.

advanced_mqtt_features() ->
    AdvancedFeatures = emqx_modules:get_advanced_mqtt_features_in_use(),
    maps:map(fun(_K, V) -> bool2int(V) end, AdvancedFeatures).

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(A) when is_atom(A) ->
    atom_to_binary(A);
bin(B) when is_binary(B) ->
    B.

bool2int(true) -> 1;
bool2int(false) -> 0.

empty_state() ->
    #state{
        url = ?TELEMETRY_URL,
        report_interval = timer:seconds(?REPORT_INTERVAL)
    }.
