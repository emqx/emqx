%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_otel_metrics).
-include_lib("emqx/include/logger.hrl").

-export([start_otel/1, stop_otel/0]).
-export([get_cluster_gauge/1, get_stats_gauge/1, get_vm_gauge/1, get_metric_counter/1]).
-export([start_link/1]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SUPERVISOR, emqx_otel_sup).

start_otel(Conf) ->
    Spec = emqx_otel_sup:worker_spec(?MODULE, Conf),
    assert_started(supervisor:start_child(?SUPERVISOR, Spec)).

stop_otel() ->
    Res =
        case erlang:whereis(?SUPERVISOR) of
            undefined ->
                ok;
            Pid ->
                case supervisor:terminate_child(Pid, ?MODULE) of
                    ok -> supervisor:delete_child(Pid, ?MODULE);
                    {error, not_found} -> ok;
                    Error -> Error
                end
        end,
    ok = cleanup(),
    Res.

start_link(Conf) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Conf, []).

init(Conf) ->
    {ok, #{}, {continue, {setup, Conf}}}.

handle_continue({setup, Conf}, State) ->
    setup(Conf),
    {noreply, State, hibernate}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

setup(Conf = #{metrics := #{enable := true}}) ->
    ensure_apps(Conf),
    create_metric_views();
setup(_Conf) ->
    ok = cleanup(),
    ok.

ensure_apps(Conf) ->
    #{
        exporter := Exporter,
        metrics := #{interval := ExporterInterval}
    } = Conf,

    _ = opentelemetry_experimental:stop_default_metrics(),
    ok = application:set_env(
        opentelemetry_experimental,
        readers,
        [
            #{
                id => emqx_otel_metric_reader,
                module => otel_metric_reader,
                config => #{
                    exporter => emqx_otel_config:otel_exporter(Exporter),
                    export_interval_ms => ExporterInterval
                }
            }
        ]
    ),
    {ok, _} = opentelemetry_experimental:start_default_metrics(),
    ok.

cleanup() ->
    safe_stop_default_metrics().

safe_stop_default_metrics() ->
    try
        _ = opentelemetry_experimental:stop_default_metrics(),
        ok
    catch
        %% normal scenario, metrics supervisor is not started
        exit:{noproc, _} -> ok
    end.

create_metric_views() ->
    Meter = opentelemetry_experimental:get_meter(),
    StatsGauge = emqx_stats:getstats(),
    create_gauge(Meter, StatsGauge, fun ?MODULE:get_stats_gauge/1),
    VmGauge = lists:map(fun({K, V}) -> {normalize_name(K), V} end, emqx_mgmt:vm_stats()),
    create_gauge(Meter, VmGauge, fun ?MODULE:get_vm_gauge/1),
    ClusterGauge = [{'node.running', 0}, {'node.stopped', 0}],
    create_gauge(Meter, ClusterGauge, fun ?MODULE:get_cluster_gauge/1),
    Metrics0 = filter_olp_metrics(emqx_metrics:all()),
    Metrics = lists:map(fun({K, V}) -> {to_metric_name(K), V, unit(K)} end, Metrics0),
    create_counter(Meter, Metrics, fun ?MODULE:get_metric_counter/1),
    ok.

filter_olp_metrics(Metrics) ->
    case emqx_config_zones:is_olp_enabled() of
        true ->
            Metrics;
        false ->
            OlpMetrics = emqx_metrics:olp_metrics(),
            lists:filter(
                fun({K, _}) ->
                    not lists:member(K, OlpMetrics)
                end,
                Metrics
            )
    end.

to_metric_name('messages.dropped.await_pubrel_timeout') ->
    'messages.dropped.expired';
to_metric_name('packets.connect.received') ->
    'packets.connect';
to_metric_name(Name) ->
    Name.

unit(K) ->
    case lists:member(K, bytes_metrics()) of
        true -> kb;
        false -> '1'
    end.

bytes_metrics() ->
    [
        'bytes.received',
        'bytes.sent',
        'packets.received',
        'packets.sent',
        'packets.connect',
        'packets.connack.sent',
        'packets.connack.error',
        'packets.connack.auth_error',
        'packets.publish.received',
        'packets.publish.sent',
        'packets.publish.inuse',
        'packets.publish.error',
        'packets.publish.auth_error',
        'packets.publish.dropped',
        'packets.puback.received',
        'packets.puback.sent',
        'packets.puback.inuse',
        'packets.puback.missed',
        'packets.pubrec.received',
        'packets.pubrec.sent',
        'packets.pubrec.inuse',
        'packets.pubrec.missed',
        'packets.pubrel.received',
        'packets.pubrel.sent',
        'packets.pubrel.missed',
        'packets.pubcomp.received',
        'packets.pubcomp.sent',
        'packets.pubcomp.inuse',
        'packets.pubcomp.missed',
        'packets.subscribe.received',
        'packets.subscribe.error',
        'packets.subscribe.auth_error',
        'packets.suback.sent',
        'packets.unsubscribe.received',
        'packets.unsubscribe.error',
        'packets.unsuback.sent',
        'packets.pingreq.received',
        'packets.pingresp.sent',
        'packets.disconnect.received',
        'packets.disconnect.sent',
        'packets.auth.received',
        'packets.auth.sent'
    ].

get_stats_gauge(Name) ->
    [{emqx_stats:getstat(Name), #{}}].

get_vm_gauge('cpu.use') ->
    [{emqx_otel_cpu_sup:stats('cpu.use'), #{}}];
get_vm_gauge('cpu.idle') ->
    [{emqx_otel_cpu_sup:stats('cpu.idle'), #{}}];
get_vm_gauge(Name) ->
    [{emqx_mgmt:vm_stats(Name), #{}}].

get_cluster_gauge('node.running') ->
    [{length(emqx:cluster_nodes(running)), #{}}];
get_cluster_gauge('node.stopped') ->
    [{length(emqx:cluster_nodes(stopped)), #{}}].

get_metric_counter(Name) ->
    [{emqx_metrics:val(Name), #{}}].

create_gauge(Meter, Names, CallBack) ->
    lists:foreach(
        fun({Name, _}) ->
            true = otel_meter_server:add_view(
                #{instrument_name => Name},
                #{aggregation_module => otel_aggregation_last_value}
            ),
            otel_meter:create_observable_gauge(
                Meter,
                Name,
                CallBack,
                Name,
                #{
                    description => iolist_to_binary([
                        <<"observable ">>, atom_to_binary(Name), <<" gauge">>
                    ]),
                    unit => '1'
                }
            )
        end,
        Names
    ).

create_counter(Meter, Counters, CallBack) ->
    lists:foreach(
        fun({Name, _, Unit}) ->
            true = otel_meter_server:add_view(
                #{instrument_name => Name},
                #{aggregation_module => otel_aggregation_sum}
            ),
            otel_meter:create_observable_counter(
                Meter,
                Name,
                CallBack,
                Name,
                #{
                    description => iolist_to_binary([
                        <<"observable ">>, atom_to_binary(Name), <<" counter">>
                    ]),
                    unit => Unit
                }
            )
        end,
        Counters
    ).

normalize_name(cpu_use) ->
    'cpu.use';
normalize_name(cpu_idle) ->
    'cpu.idle';
normalize_name(run_queue) ->
    'run.queue';
normalize_name(total_memory) ->
    'total.memory';
normalize_name(used_memory) ->
    'used.memory';
normalize_name(Name) ->
    list_to_existing_atom(lists:flatten(string:replace(atom_to_list(Name), "_", ".", all))).

assert_started({ok, _Pid}) -> ok;
assert_started({ok, _Pid, _Info}) -> ok;
assert_started({error, {already_started, _Pid}}) -> ok;
assert_started({error, Reason}) -> {error, Reason}.
