%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% config change hook points
-export([
    start_reporting/0,
    stop_reporting/0
]).

-export([
    get_node_uuid/0,
    get_cluster_uuid/0,
    get_telemetry/0
]).

%% Internal exports (RPC)
-export([
    do_ensure_uuids/0
]).

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

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% The destination URL for the telemetry data report
-define(TELEMETRY_URL, "https://telemetry.emqx.io/api/telemetry").
-define(REPORT_INTERVAL, 604800).

-record(telemetry, {
    id :: atom(),
    uuid :: binary()
}).

-record(state, {
    node_uuid :: undefined | binary(),
    cluster_uuid :: undefined | binary(),
    url :: string(),
    report_interval :: non_neg_integer(),
    timer = undefined :: undefined | reference(),
    previous_metrics = #{} :: map()
}).

-type state() :: #state{}.

%% The count of 100-nanosecond intervals between the UUID epoch
%% 1582-10-15 00:00:00 and the UNIX epoch 1970-01-01 00:00:00.
-define(GREGORIAN_EPOCH_OFFSET, 16#01b21dd213814000).

-define(CLUSTER_UUID_KEY, cluster_uuid).

-define(TELEMETRY, emqx_telemetry).

-define(TELEMETRY_SHARD, emqx_telemetry_shard).

-define(NODE_UUID_FILENAME, "node.uuid").
-define(CLUSTER_UUID_FILENAME, "cluster.uuid").

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    Opts = emqx:get_config([telemetry], #{}),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

stop() ->
    gen_server:stop(?MODULE).

%% @doc Re-start the reporting timer after disabled.
%% This is an async notification which never fails.
%% This is a no-op in enterprise edition.
start_reporting() ->
    gen_server:cast(?MODULE, start_reporting).

%% @doc Stop the reporting timer.
%% This is an async notification which never fails.
%% This is a no-op in enterprise edition.
stop_reporting() ->
    gen_server:cast(?MODULE, stop_reporting).

get_node_uuid() ->
    gen_server:call(?MODULE, get_node_uuid).

get_cluster_uuid() ->
    gen_server:call(?MODULE, get_cluster_uuid).

get_telemetry() ->
    gen_server:call(?MODULE, get_telemetry).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(_Opts) ->
    process_flag(trap_exit, true),
    emqx_telemetry_config:on_server_start(),
    {ok, "ignored", {continue, init}}.

handle_continue(init, _) ->
    ok = mria:create_table(
        ?TELEMETRY,
        [
            {type, set},
            {storage, disc_copies},
            {rlog_shard, ?TELEMETRY_SHARD},
            {record_name, telemetry},
            {attributes, record_info(fields, telemetry)}
        ]
    ),
    ok = mria:wait_for_tables([?TELEMETRY]),
    State0 = empty_state(),
    {NodeUUID, ClusterUUID} = ensure_uuids(),
    case is_enabled() of
        true -> ok = start_reporting();
        false -> ok
    end,
    {noreply, State0#state{node_uuid = NodeUUID, cluster_uuid = ClusterUUID}}.

handle_call(get_node_uuid, _From, State = #state{node_uuid = UUID}) ->
    {reply, {ok, UUID}, State};
handle_call(get_cluster_uuid, _From, State = #state{cluster_uuid = UUID}) ->
    {reply, {ok, UUID}, State};
handle_call(get_telemetry, _From, State) ->
    {_State, Telemetry} = get_telemetry(State),
    {reply, {ok, Telemetry}, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(start_reporting, State) ->
    %% Wait a few moments before reporting the first telemetry, as the
    %% apps might still be starting up.
    FirstReportTimeoutMS = timer:seconds(10),
    {noreply, ensure_report_timer(FirstReportTimeoutMS, State)};
handle_cast(stop_reporting, State = #state{timer = Timer}) ->
    emqx_utils:cancel_timer(Timer),
    {noreply, State#state{timer = undefined}};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, TRef, time_to_report_telemetry_data}, State0 = #state{timer = TRef}) ->
    State = report_telemetry(State0),
    {noreply, ensure_report_timer(State)};
handle_info({timeout, _TRef, time_to_report_telemetry_data}, State = #state{timer = undefined}) ->
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_telemetry_config:on_server_stop().

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

is_enabled() ->
    emqx_telemetry_config:is_enabled().

ensure_report_timer(State = #state{report_interval = ReportInterval}) ->
    ensure_report_timer(ReportInterval, State).

ensure_report_timer(ReportInterval, State) ->
    State#state{timer = emqx_utils:start_timer(ReportInterval, time_to_report_telemetry_data)}.

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
    Nodes = lists:delete(node(), mria:running_nodes()),
    lists:foldl(
        fun(Node, Acc) ->
            case emqx_telemetry_proto_v1:get_node_uuid(Node) of
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
        fun
            (#{running_status := running} = Plugin, Acc) ->
                #{<<"name">> := Name, <<"rel_vsn">> := Vsn} = Plugin,
                [iolist_to_binary([Name, "-", Vsn]) | Acc];
            (_, Acc) ->
                Acc
        end,
        [],
        emqx_plugins:list()
    ).

num_clients() ->
    emqx_stats:getstat('live_connections.count').

num_cluster_sessions() ->
    emqx_stats:getstat('cluster_sessions.count').

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
get_telemetry(State0 = #state{node_uuid = NodeUUID, cluster_uuid = ClusterUUID}) ->
    OSInfo = os_info(),
    {MQTTRTInsights, State} = mqtt_runtime_insights(State0),
    #{
        rule_engine := RuleEngineInfo,
        bridge := BridgeInfo
    } = get_rule_engine_and_bridge_info(),
    {State, [
        {emqx_version, bin(emqx_app:get_release())},
        {license, [{edition, <<"opensource">>}]},
        {os_name, bin(get_value(os_name, OSInfo))},
        {os_version, bin(get_value(os_version, OSInfo))},
        {otp_version, bin(otp_version())},
        {up_time, uptime()},
        {uuid, NodeUUID},
        {cluster_uuid, ClusterUUID},
        {nodes_uuid, nodes_uuid()},
        {active_plugins, active_plugins()},
        {num_clients, num_clients()},
        {num_cluster_sessions, num_cluster_sessions()},
        {messages_received, messages_received()},
        {messages_sent, messages_sent()},
        {build_info, build_info()},
        {vm_specs, vm_specs()},
        {mqtt_runtime_insights, MQTTRTInsights},
        {advanced_mqtt_features, advanced_mqtt_features()},
        {authn_authz, get_authn_authz_info()},
        {gateway, get_gateway_info()},
        {rule_engine, RuleEngineInfo},
        {bridge, BridgeInfo},
        {exhook, get_exhook_info()}
    ]}.

report_telemetry(State0 = #state{url = URL}) ->
    {State, Data} = get_telemetry(State0),
    case emqx_utils_json:safe_encode(Data) of
        {ok, Bin} ->
            ok = httpc_request(post, URL, [], Bin),
            ?tp(debug, telemetry_data_reported, #{});
        {error, Reason} ->
            %% debug? why?
            ?tp(debug, telemetry_data_encode_error, #{data => Data, reason => Reason})
    end,
    State.

httpc_request(Method, URL, Headers, Body) ->
    HTTPOptions = [{timeout, 10_000}, {ssl, [{verify, verify_none}]}],
    Options = [],
    _ = httpc:request(Method, {URL, Headers, "application/json", Body}, HTTPOptions, Options),
    ok.

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
    [
        {num_cpus, erlang:system_info(logical_processors)},
        {total_memory, emqx_mgmt:vm_stats('total.memory')}
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
    #{retained_messages := RetainedMessages} = emqx_retainer:get_basic_usage_info(),
    #{topic_rewrite_rule_count := RewriteRules} = emqx_rewrite:get_basic_usage_info(),
    #{delayed_message_count := DelayedCount} = emqx_delayed:get_basic_usage_info(),
    #{auto_subscribe_count := AutoSubscribe} = emqx_auto_subscribe:get_basic_usage_info(),
    #{
        topic_rewrite => RewriteRules,
        delayed => DelayedCount,
        retained => RetainedMessages,
        auto_subscribe => AutoSubscribe
    }.

get_authn_authz_info() ->
    try
        #{
            authenticators := AuthnTypes,
            overridden_listeners := OverriddenListeners
        } = emqx_authn:get_enabled_authns(),
        AuthzTypes = emqx_authz:get_enabled_authzs(),
        #{
            authn => AuthnTypes,
            authn_listener => OverriddenListeners,
            authz => AuthzTypes
        }
    catch
        _:_ ->
            #{
                authn => [],
                authn_listener => [],
                authz => []
            }
    end.

get_gateway_info() ->
    try
        emqx_gateway:get_basic_usage_info()
    catch
        %% if gateway is not available, for instance
        _:_ ->
            #{}
    end.

get_rule_engine_and_bridge_info() ->
    #{
        num_rules := NumRules,
        referenced_bridges := ReferencedBridges
    } = emqx_rule_engine:get_basic_usage_info(),
    #{
        num_bridges := NumDataBridges,
        count_by_type := BridgeTypeCount
    } = emqx_bridge:get_basic_usage_info(),
    BridgeInfo =
        maps:fold(
            fun(BridgeType, BridgeCount, Acc) ->
                ReferencingRules = maps:get(BridgeType, ReferencedBridges, 0),
                Acc#{
                    BridgeType => #{
                        num => BridgeCount,
                        num_linked_by_rules => ReferencingRules
                    }
                }
            end,
            #{},
            BridgeTypeCount
        ),
    #{
        rule_engine => #{num_rules => NumRules},
        bridge => #{
            num_data_bridges => NumDataBridges,
            data_bridge => BridgeInfo
        }
    }.

get_exhook_info() ->
    emqx_exhook:get_basic_usage_info().

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(A) when is_atom(A) ->
    atom_to_binary(A);
bin(B) when is_binary(B) ->
    B.

ensure_uuids() ->
    {atomic, {NodeUUID, ClusterUUID}} = mria:transaction(
        ?TELEMETRY_SHARD, fun ?MODULE:do_ensure_uuids/0
    ),
    save_uuid_to_file(NodeUUID, node),
    save_uuid_to_file(ClusterUUID, cluster),
    {NodeUUID, ClusterUUID}.

do_ensure_uuids() ->
    NodeUUID =
        case mnesia:wread({?TELEMETRY, node()}) of
            [] ->
                NodeUUID0 =
                    case get_uuid_from_file(node) of
                        {ok, NUUID} -> NUUID;
                        undefined -> generate_uuid()
                    end,
                mnesia:write(
                    ?TELEMETRY,
                    #telemetry{
                        id = node(),
                        uuid = NodeUUID0
                    },
                    write
                ),
                NodeUUID0;
            [#telemetry{uuid = NodeUUID0}] ->
                NodeUUID0
        end,
    ClusterUUID =
        case mnesia:wread({?TELEMETRY, ?CLUSTER_UUID_KEY}) of
            [] ->
                ClusterUUID0 =
                    case get_uuid_from_file(cluster) of
                        {ok, CUUID} -> CUUID;
                        undefined -> generate_uuid()
                    end,
                mnesia:write(
                    ?TELEMETRY,
                    #telemetry{
                        id = ?CLUSTER_UUID_KEY,
                        uuid = ClusterUUID0
                    },
                    write
                ),
                ClusterUUID0;
            [#telemetry{uuid = ClusterUUID0}] ->
                ClusterUUID0
        end,
    {NodeUUID, ClusterUUID}.

get_uuid_from_file(Type) ->
    Path = uuid_file_path(Type),
    case file:read_file(Path) of
        {ok,
            UUID =
                <<_:8/binary, "-", _:4/binary, "-", _:4/binary, "-", _:4/binary, "-", _:12/binary>>} ->
            {ok, UUID};
        _ ->
            undefined
    end.

save_uuid_to_file(UUID, Type) when is_binary(UUID) ->
    Path = uuid_file_path(Type),
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, UUID).

uuid_file_path(Type) ->
    DataDir = emqx:data_dir(),
    Filename =
        case Type of
            node -> ?NODE_UUID_FILENAME;
            cluster -> ?CLUSTER_UUID_FILENAME
        end,
    filename:join(DataDir, Filename).

empty_state() ->
    #state{
        url = ?TELEMETRY_URL,
        report_interval = timer:seconds(?REPORT_INTERVAL)
    }.
