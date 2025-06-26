%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus).

-behaviour(gen_server).

%% Please don't remove this attribute, it will
%% be used by the prometheus application
-behaviour(prometheus_collector).

-behaviour(emqx_prometheus_cluster).
-export([
    fetch_from_local_node/1,
    fetch_cluster_consistented_data/0,
    aggre_or_zip_init_acc/0,
    logic_sum_metrics/0
]).

-export([zip_json_prom_stats_metrics/3]).

-include("emqx_prometheus.hrl").

-include_lib("public_key/include/public_key.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_instr.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds_metrics.hrl").

%% APIs
-export([start_link/1, info/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

%% prometheus_collector callback
-export([
    deregister_cleanup/1,
    collect_mf/2,
    collect_metrics/2
]).

-export([collect/1]).

-export([
    %% For bpapi, deprecated_since 5.0.10, remove this when 5.1.x
    do_start/0,
    do_stop/0
]).

-ifdef(TEST).
-export([cert_expiry_at_from_path/1]).
-endif.

%%--------------------------------------------------------------------
%% Macros
%%--------------------------------------------------------------------

-define(MG(K, MAP), maps:get(K, MAP)).
-define(MG(K, MAP, DEFAULT), maps:get(K, MAP, DEFAULT)).

-define(C(K, L), proplists:get_value(K, L, 0)).

-define(TIMER_MSG, '#interval').

-define(HTTP_OPTIONS, [{autoredirect, true}, {timeout, 60000}]).

-define(SAFELY(EXPR, ELSE),
    (try
        EXPR
    catch
        _:_ -> ELSE
    end)
).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Conf) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Conf, []).

info() ->
    gen_server:call(?MODULE, info).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(Conf) ->
    {ok, #{}, {continue, Conf}}.

handle_continue(Conf, State) ->
    Opts = #{interval := Interval} = opts(Conf),
    {noreply, State#{
        timer => ensure_timer(Interval),
        opts => Opts,
        ok => 0,
        failed => 0
    }}.

handle_call(info, _From, State = #{timer := Timer, opts := Opts}) ->
    {reply, State#{opts => Opts, next_push_ms => erlang:read_timer(Timer)}, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, Timer, ?TIMER_MSG}, State = #{timer := Timer, opts := Opts}) ->
    #{interval := Interval, headers := Headers, url := Server, method := Method} = Opts,
    PushRes = push_to_push_gateway(Method, Server, Headers),
    NewTimer = ensure_timer(Interval),
    NewState = maps:update_with(PushRes, fun(C) -> C + 1 end, 1, State#{timer => NewTimer}),
    %% Data is too big, hibernate for saving memory and stop system monitor warning.
    {noreply, NewState, hibernate};
handle_info({update, Conf}, State = #{timer := Timer}) ->
    emqx_utils:cancel_timer(Timer),
    handle_continue(Conf, State);
handle_info(_Msg, State) ->
    {noreply, State}.

push_to_push_gateway(Method, Url, Headers) when
    is_list(Headers) andalso (Method =:= put orelse Method =:= post)
->
    Data = push_metrics_data(),
    case httpc:request(Method, {Url, Headers, "text/plain", Data}, ?HTTP_OPTIONS, []) of
        {ok, {{"HTTP/1.1", 200, _}, _RespHeaders, _RespBody}} ->
            ok;
        Error ->
            ?SLOG(error, #{
                msg => "post_to_push_gateway_failed",
                error => Error,
                url => Url,
                headers => Headers
            }),
            failed
    end.

push_metrics_data() ->
    Rows = [prometheus_text_format:format(Registry) || Registry <- ?PROMETHEUS_ALL_REGISTRIES],
    iolist_to_binary(Rows).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

ensure_timer(Interval) ->
    emqx_utils:start_timer(Interval, ?TIMER_MSG).

%%--------------------------------------------------------------------
%% prometheus callbacks
%%--------------------------------------------------------------------
opts(
    Conf = #{
        interval := Interval, headers := Headers, job_name := JobName, push_gateway_server := Url
    }
) ->
    #{
        interval => Interval,
        headers => Headers,
        url => join_url(Url, JobName),
        method => ?MG(method, Conf, put)
    };
opts(#{push_gateway := #{url := Url, job_name := JobName} = PushGateway}) ->
    PushGateway#{
        url => join_url(Url, JobName),
        method => ?MG(method, PushGateway, put)
    }.

join_url(Url, JobName0) ->
    ClusterName = atom_to_binary(emqx:get_config([cluster, name], emqxcl)),
    [Name, Ip] = string:tokens(atom_to_list(node()), "@"),
    % NOTE: allowing errors here to keep rough backward compatibility
    {JobName1, Errors} = emqx_template:render(
        emqx_template:parse(JobName0),
        #{<<"name">> => Name, <<"host">> => Ip, <<"cluster_name">> => ClusterName}
    ),
    _ =
        Errors == [] orelse
            ?SLOG(warning, #{
                msg => "prometheus_job_name_template_invalid",
                errors => Errors,
                template => JobName0
            }),
    lists:concat([Url, "/metrics/job/", unicode:characters_to_list(JobName1)]).

deregister_cleanup(?PROMETHEUS_DEFAULT_REGISTRY) ->
    ok.

collect_mf(?PROMETHEUS_DEFAULT_REGISTRY, Callback) ->
    RawData = emqx_prometheus_cluster:raw_data(?MODULE, ?GET_PROM_DATA_MODE()),
    %% TODO: license expiry epoch and cert expiry epoch should be cached
    ok = add_collect_family(Callback, stats_metric_meta(), ?MG(stats_data, RawData)),
    ok = add_collect_family(
        Callback,
        stats_metric_cluster_consistented_meta(),
        ?MG(stats_data_cluster_consistented, RawData)
    ),
    ok = add_collect_family(Callback, vm_metric_meta(), ?MG(vm_data, RawData)),
    ok = add_collect_family(Callback, cluster_metric_meta(), ?MG(cluster_data, RawData)),

    ok = add_collect_family(Callback, emqx_packet_metric_meta(), ?MG(emqx_packet_data, RawData)),
    ok = add_collect_family(Callback, message_metric_meta(), ?MG(emqx_message_data, RawData)),
    ok = add_collect_family(Callback, delivery_metric_meta(), ?MG(emqx_delivery_data, RawData)),
    ok = add_collect_family(Callback, client_metric_meta(), ?MG(emqx_client_data, RawData)),
    ok = add_collect_family(Callback, session_metric_meta(), ?MG(emqx_session_data, RawData)),
    ok = add_collect_family(Callback, olp_metric_meta(), ?MG(emqx_olp_data, RawData)),
    ok = add_collect_family(Callback, acl_metric_meta(), ?MG(emqx_acl_data, RawData)),
    ok = add_collect_family(Callback, authn_metric_meta(), ?MG(emqx_authn_data, RawData)),

    ok = collect_broker_instr_family(Callback, ?MG(emqx_broker_instr_data, RawData)),

    ok = add_collect_family(Callback, cert_metric_meta(), ?MG(cert_data, RawData)),
    ok = add_collect_family(Callback, cluster_rpc_meta(), ?MG(cluster_rpc, RawData)),
    ok = add_collect_family(Callback, mria_metric_meta(), ?MG(mria_data, RawData)),
    ok = maybe_add_ds_collect_family(Callback, RawData),
    ok = maybe_license_add_collect_family(Callback, RawData),
    ok;
collect_mf(_Registry, _Callback) ->
    ok.

maybe_add_ds_collect_family(Callback, RawData) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            add_collect_family(
                Callback, emqx_ds_builtin_metrics:prometheus_meta(), ?MG(ds_data, RawData)
            ),
            DSRaftPrefix = <<"emqx_ds_raft_">>,
            prefix_collect_helpful_family(
                Callback, DSRaftPrefix, ds_raft_node_meta(), ?MG(ds_raft_node_data, RawData)
            ),
            prefix_collect_helpful_family(
                Callback, DSRaftPrefix, ds_raft_cluster_meta(), ?MG(ds_raft_cluster_data, RawData)
            );
        false ->
            ok
    end.

maybe_collect_ds_data(Mode) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            #{
                ds_data => emqx_ds_builtin_metrics:prometheus_collect(with_node_label(Mode, [])),
                ds_raft_node_data => collect_ds_raft_node_data(Mode)
            };
        false ->
            #{}
    end.

maybe_collect_ds_cluster_data(Acc) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            Acc#{
                ds_raft_cluster_data => collect_ds_raft_cluster_data()
            };
        false ->
            Acc
    end.

collect_ds_raft_node_data(Mode) ->
    Labels0 = with_node_label(Mode, []),
    Acc = emqx_ds_builtin_raft_metrics:local_dbs(Labels0),
    maps:merge(emqx_ds_builtin_raft_metrics:local_shards(Labels0), Acc).

collect_ds_raft_cluster_data() ->
    Acc1 = emqx_ds_builtin_raft_metrics:cluster(),
    Acc2 = maps:merge(emqx_ds_builtin_raft_metrics:dbs(), Acc1),
    maps:merge(emqx_ds_builtin_raft_metrics:shards(), Acc2).

%% @private
collect(<<"json">>) ->
    RawData = emqx_prometheus_cluster:raw_data(?MODULE, ?GET_PROM_DATA_MODE()),
    (maybe_license_collect_json_data(RawData))#{
        stats => collect_stats_json_data(
            ?MG(stats_data, RawData), ?MG(stats_data_cluster_consistented, RawData)
        ),
        metrics => collect_vm_json_data(?MG(vm_data, RawData)),
        packets => collect_json_data(?MG(emqx_packet_data, RawData)),
        messages => collect_json_data(?MG(emqx_message_data, RawData)),
        delivery => collect_json_data(?MG(emqx_delivery_data, RawData)),
        client => collect_client_json_data(?MG(emqx_client_data, RawData)),
        session => collect_json_data(?MG(emqx_session_data, RawData)),
        cluster => collect_json_data(?MG(cluster_data, RawData)),
        olp => collect_json_data(?MG(emqx_olp_data, RawData)),
        acl => collect_json_data(?MG(emqx_acl_data, RawData)),
        authn => collect_json_data(?MG(emqx_authn_data, RawData)),
        certs => collect_cert_json_data(?MG(cert_data, RawData)),
        cluster_rpc => collect_json_data(?MG(cluster_rpc, RawData))
    };
collect(<<"prometheus">>) ->
    prometheus_text_format:format(?PROMETHEUS_DEFAULT_REGISTRY).

collect_metrics(Name, Metrics) ->
    emqx_collect(Name, Metrics).

add_collect_family(Callback, MetricWithType, Data) ->
    _ = [add_collect_family(Name, Data, Callback, Type) || {Name, Type, _} <- MetricWithType],
    ok.

add_collect_family(Name, Data, Callback, Type) ->
    Callback(prometheus_model_helpers:create_mf(Name, _Help = <<"">>, Type, ?MODULE, Data)).

prefix_collect_helpful_family(Callback, Prefix, MetricsTypeAndHelp, Metrics) ->
    lists:foreach(
        fun({Name, Type, Help}) ->
            %% Using `create_mf/4` that doesn't call back into `collect_metrics/2.`
            PromName = [Prefix, atom_to_binary(Name)],
            Callback(
                prometheus_model_helpers:create_mf(PromName, Help, Type, ?MG(Name, Metrics, []))
            )
        end,
        MetricsTypeAndHelp
    ).

%% behaviour
fetch_from_local_node(Mode) ->
    {node(), (maybe_collect_ds_data(Mode))#{
        stats_data => stats_data(Mode),
        vm_data => vm_data(Mode),
        cluster_data => cluster_data(Mode),
        %% Metrics
        emqx_packet_data => emqx_metric_data(emqx_packet_metric_meta(), Mode),
        emqx_message_data => emqx_metric_data(message_metric_meta(), Mode),
        emqx_delivery_data => emqx_metric_data(delivery_metric_meta(), Mode),
        emqx_client_data => client_metric_data(Mode),
        emqx_session_data => emqx_metric_data(session_metric_meta(), Mode),
        emqx_olp_data => emqx_metric_data(olp_metric_meta(), Mode),
        emqx_acl_data => emqx_metric_data(acl_metric_meta(), Mode),
        emqx_authn_data => emqx_metric_data(authn_metric_meta(), Mode),
        emqx_broker_instr_data => emqx_broker_instr_data(Mode),
        cluster_rpc => cluster_rpc_data(Mode),
        mria_data => mria_data(Mode)
    }}.

fetch_cluster_consistented_data() ->
    Acc1 = maybe_license_fetch_data(),
    Acc = maybe_collect_ds_cluster_data(Acc1),
    Acc#{
        stats_data_cluster_consistented => stats_data_cluster_consistented(),
        cert_data => cert_data()
    }.

aggre_or_zip_init_acc() ->
    (maybe_add_ds_meta())#{
        stats_data => meta_to_init_from(stats_metric_meta()),
        vm_data => meta_to_init_from(vm_metric_meta()),
        cluster_data => meta_to_init_from(cluster_metric_meta()),
        emqx_packet_data => meta_to_init_from(emqx_packet_metric_meta()),
        emqx_message_data => meta_to_init_from(message_metric_meta()),
        emqx_delivery_data => meta_to_init_from(delivery_metric_meta()),
        emqx_client_data => meta_to_init_from(client_metric_meta()),
        emqx_session_data => meta_to_init_from(session_metric_meta()),
        emqx_olp_data => meta_to_init_from(olp_metric_meta()),
        emqx_acl_data => meta_to_init_from(acl_metric_meta()),
        emqx_authn_data => meta_to_init_from(authn_metric_meta()),
        emqx_broker_instr_data => emqx_broker_instr_init(),
        cluster_rpc => meta_to_init_from(cluster_rpc_meta()),
        mria_data => meta_to_init_from(mria_metric_meta())
    }.

logic_sum_metrics() ->
    [].

%%--------------------------------------------------------------------
%% Collector
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Stats
%% connections
emqx_collect(K = emqx_connections_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_connections_max, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_live_connections_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_live_connections_max, D) -> gauge_metrics(?MG(K, D));
%% sessions
emqx_collect(K = emqx_sessions_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_sessions_max, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_channels_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_channels_max, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_cluster_sessions_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_cluster_sessions_max, D) -> gauge_metrics(?MG(K, D));
%% pub/sub stats
emqx_collect(K = emqx_durable_subscriptions_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_durable_subscriptions_max, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_topics_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_topics_max, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_suboptions_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_suboptions_max, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_subscribers_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_subscribers_max, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_subscriptions_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_subscriptions_max, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_subscriptions_shared_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_subscriptions_shared_max, D) -> gauge_metrics(?MG(K, D));
%% retained
emqx_collect(K = emqx_retained_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_retained_max, D) -> gauge_metrics(?MG(K, D));
%% delayed
emqx_collect(K = emqx_delayed_count, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_delayed_max, D) -> gauge_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% VM
emqx_collect(K = emqx_vm_cpu_use, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_vm_cpu_idle, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_vm_run_queue, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_vm_process_messages_in_queues, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_vm_total_memory, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_vm_used_memory, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_vm_mnesia_tm_mailbox_size, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_vm_broker_pool_max_mailbox_size, D) -> gauge_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% Cluster Info
emqx_collect(K = emqx_cluster_nodes_running, D) -> gauge_metrics(?MG(K, D));
emqx_collect(K = emqx_cluster_nodes_stopped, D) -> gauge_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% Metrics - packets & bytes
%% bytes
emqx_collect(K = emqx_bytes_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_bytes_sent, D) -> counter_metrics(?MG(K, D));
%% received.sent
emqx_collect(K = emqx_packets_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_sent, D) -> counter_metrics(?MG(K, D));
%% connect
emqx_collect(K = emqx_packets_connect, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_connack_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_connack_error, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_connack_auth_error, D) -> counter_metrics(?MG(K, D));
%% sub.unsub
emqx_collect(K = emqx_packets_subscribe_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_subscribe_auth_error, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_subscribe_error, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_suback_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_unsubscribe_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_unsubscribe_error, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_unsuback_sent, D) -> counter_metrics(?MG(K, D));
%% publish.puback
emqx_collect(K = emqx_packets_publish_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_publish_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_publish_inuse, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_publish_error, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_publish_auth_error, D) -> counter_metrics(?MG(K, D));
%% puback
emqx_collect(K = emqx_packets_puback_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_puback_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_puback_inuse, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_puback_missed, D) -> counter_metrics(?MG(K, D));
%% pubrec
emqx_collect(K = emqx_packets_pubrec_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_pubrec_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_pubrec_inuse, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_pubrec_missed, D) -> counter_metrics(?MG(K, D));
%% pubrel
emqx_collect(K = emqx_packets_pubrel_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_pubrel_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_pubrel_missed, D) -> counter_metrics(?MG(K, D));
%% pubcomp
emqx_collect(K = emqx_packets_pubcomp_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_pubcomp_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_pubcomp_inuse, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_pubcomp_missed, D) -> counter_metrics(?MG(K, D));
%% pingreq
emqx_collect(K = emqx_packets_pingreq_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_pingresp_sent, D) -> counter_metrics(?MG(K, D));
%% disconnect
emqx_collect(K = emqx_packets_disconnect_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_disconnect_sent, D) -> counter_metrics(?MG(K, D));
%% auth
emqx_collect(K = emqx_packets_auth_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_packets_auth_sent, D) -> counter_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% Metrics - messages
%% messages
emqx_collect(K = emqx_messages_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_qos0_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_qos0_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_qos1_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_qos1_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_qos2_received, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_qos2_sent, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_publish, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_dropped, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_dropped_expired, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_dropped_no_subscribers, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_dropped_quota_exceeded, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_dropped_receive_maximum, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_forward, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_retained, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_delayed, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_delivered, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_messages_acked, D) -> counter_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% Metrics - delivery
emqx_collect(K = emqx_delivery_dropped, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_delivery_dropped_no_local, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_delivery_dropped_too_large, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_delivery_dropped_qos0_msg, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_delivery_dropped_queue_full, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_delivery_dropped_expired, D) -> counter_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% Metrics - client
emqx_collect(K = emqx_client_connect, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_client_connack, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_client_connected, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_client_authenticate, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_client_auth_anonymous, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_client_authorize, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_client_subscribe, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_client_unsubscribe, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_client_disconnected, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_client_disconnected_reason, D) -> counter_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% Metrics - session
emqx_collect(K = emqx_session_created, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_session_resumed, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_session_takenover, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_session_discarded, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_session_terminated, D) -> counter_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% Metrics - overload protection
emqx_collect(K = emqx_overload_protection_delay_ok, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_overload_protection_delay_timeout, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_overload_protection_hibernation, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_overload_protection_gc, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_overload_protection_new_conn, D) -> counter_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% Metrics - acl
emqx_collect(K = emqx_authorization_allow, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_authorization_deny, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_authorization_cache_hit, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_authorization_cache_miss, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_authorization_superuser, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_authorization_nomatch, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_authorization_matched_allow, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_authorization_matched_deny, D) -> counter_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% Metrics - authn
emqx_collect(K = emqx_authentication_success, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_authentication_success_anonymous, D) -> counter_metrics(?MG(K, D));
emqx_collect(K = emqx_authentication_failure, D) -> counter_metrics(?MG(K, D));
%%--------------------------------------------------------------------
%% License
emqx_collect(K = emqx_license_expiry_at, D) -> gauge_metric(?MG(K, D));
%%--------------------------------------------------------------------
%% Certs
emqx_collect(K = emqx_cert_expiry_at, D) -> gauge_metrics(?MG(K, D));
%% Cluster RPC
emqx_collect(K = emqx_conf_sync_txid, D) -> gauge_metrics(?MG(K, D));
%% Mria
%% ========== core
emqx_collect(K = emqx_mria_last_intercepted_trans, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = emqx_mria_weight, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = emqx_mria_replicants, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = emqx_mria_server_mql, D) -> gauge_metrics(?MG(K, D, []));
%% ========== replicant
emqx_collect(K = emqx_mria_lag, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = emqx_mria_bootstrap_time, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = emqx_mria_bootstrap_num_keys, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = emqx_mria_message_queue_len, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = emqx_mria_replayq_len, D) -> gauge_metrics(?MG(K, D, []));
%% DS
emqx_collect(K = ?DS_BUFFER_BATCHES, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_BUFFER_BATCHES_RETRY, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_BUFFER_BATCHES_FAILED, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_BUFFER_MESSAGES, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_BUFFER_BYTES, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_BUFFER_FLUSH_TIME, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_BUFFER_LATENCY, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_STORE_BATCH_TIME, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_BUILTIN_NEXT_TIME, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_BITFIELD_LTS_SEEK_COUNTER, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_BITFIELD_LTS_NEXT_COUNTER, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_BITFIELD_LTS_COLLISION_COUNTER, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SKIPSTREAM_LTS_SEEK, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SKIPSTREAM_LTS_NEXT, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SKIPSTREAM_LTS_HASH_COLLISION, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SKIPSTREAM_LTS_HIT, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SKIPSTREAM_LTS_MISS, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SKIPSTREAM_LTS_FUTURE, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SKIPSTREAM_LTS_EOS, D) -> counter_metrics(?MG(K, D, []));
%% DS beamformer:
emqx_collect(K = ?DS_SUBS_FANOUT_TIME, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SUBS_STUCK_TOTAL, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SUBS_UNSTUCK_TOTAL, D) -> counter_metrics(?MG(K, D, []));
%% DS beamformer worker:
emqx_collect(K = ?DS_SUBS, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SUBS_HANDOVER, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SUBS_PROCESS_COMMANDS_TIME, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SUBS_BEAMS_SENT_TOTAL, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SUBS_REQUEST_SHARING, D) -> gauge_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SUBS_FULFILL_TIME, D) -> counter_metrics(?MG(K, D, []));
emqx_collect(K = ?DS_SUBS_SCAN_TIME, D) -> counter_metrics(?MG(K, D, [])).

%%--------------------------------------------------------------------
%% Indicators
%%--------------------------------------------------------------------

%%========================================
%% Stats
%%========================================

stats_metric_meta() ->
    [
        %% connections
        {emqx_connections_count, gauge, 'connections.count'},
        {emqx_connections_max, gauge, 'connections.max'},
        {emqx_live_connections_count, gauge, 'live_connections.count'},
        {emqx_live_connections_max, gauge, 'live_connections.max'},
        %% sessions
        {emqx_sessions_count, gauge, 'sessions.count'},
        {emqx_sessions_max, gauge, 'sessions.max'},
        {emqx_channels_count, gauge, 'channels.count'},
        {emqx_channels_max, gauge, 'channels.max'},
        %% pub/sub stats
        {emqx_suboptions_count, gauge, 'suboptions.count'},
        {emqx_suboptions_max, gauge, 'suboptions.max'},
        {emqx_subscribers_count, gauge, 'subscribers.count'},
        {emqx_subscribers_max, gauge, 'subscribers.max'},
        {emqx_subscriptions_count, gauge, 'subscriptions.count'},
        {emqx_subscriptions_max, gauge, 'subscriptions.max'},
        {emqx_durable_subscriptions_count, gauge, 'durable_subscriptions.count'},
        {emqx_durable_subscriptions_max, gauge, 'durable_subscriptions.max'},
        %% delayed
        {emqx_delayed_count, gauge, 'delayed.count'},
        {emqx_delayed_max, gauge, 'delayed.max'}
    ].

stats_metric_cluster_consistented_meta() ->
    [
        %% sessions
        {emqx_cluster_sessions_count, gauge, 'cluster_sessions.count'},
        {emqx_cluster_sessions_max, gauge, 'cluster_sessions.max'},
        %% topics
        {emqx_topics_max, gauge, 'topics.max'},
        {emqx_topics_count, gauge, 'topics.count'},
        %% retained
        {emqx_retained_count, gauge, 'retained.count'},
        {emqx_retained_max, gauge, 'retained.max'},
        %% shared subscriptions
        {emqx_subscriptions_shared_count, gauge, 'subscriptions.shared.count'},
        {emqx_subscriptions_shared_max, gauge, 'subscriptions.shared.max'}
    ].

stats_data(Mode) ->
    Stats = emqx_stats:getstats(),
    lists:foldl(
        fun({Name, _Type, MetricKAtom}, AccIn) ->
            AccIn#{Name => [{with_node_label(Mode, []), ?C(MetricKAtom, Stats)}]}
        end,
        #{},
        stats_metric_meta()
    ).

stats_data_cluster_consistented() ->
    Stats = emqx_stats:getstats(),
    lists:foldl(
        fun({Name, _Type, MetricKAtom}, AccIn) ->
            AccIn#{Name => [{[], ?C(MetricKAtom, Stats)}]}
        end,
        #{},
        stats_metric_cluster_consistented_meta()
    ).

%%========================================
%% Erlang VM
%%========================================

vm_metric_meta() ->
    [
        {emqx_vm_cpu_use, gauge, 'cpu_use'},
        {emqx_vm_cpu_idle, gauge, 'cpu_idle'},
        {emqx_vm_run_queue, gauge, 'run_queue'},
        {emqx_vm_process_messages_in_queues, gauge, 'process_total_messages'},
        {emqx_vm_total_memory, gauge, 'total_memory'},
        {emqx_vm_used_memory, gauge, 'used_memory'},
        {emqx_vm_mnesia_tm_mailbox_size, gauge, 'mnesia_tm_mailbox_size'},
        {emqx_vm_broker_pool_max_mailbox_size, gauge, 'broker_pool_max_mailbox_size'}
    ].

vm_data(Mode) ->
    VmStats = emqx_mgmt:vm_stats(),
    lists:foldl(
        fun({Name, _Type, MetricKAtom}, AccIn) ->
            Labels =
                case Mode of
                    ?PROM_DATA_MODE__NODE ->
                        [];
                    _ ->
                        [{node, node(self())}]
                end,
            AccIn#{Name => [{Labels, ?C(MetricKAtom, VmStats)}]}
        end,
        #{},
        vm_metric_meta()
    ).

%%========================================
%% Cluster
%%========================================

cluster_metric_meta() ->
    [
        {emqx_cluster_nodes_running, gauge, undefined},
        {emqx_cluster_nodes_stopped, gauge, undefined}
    ].

cluster_data(node) ->
    Labels = [],
    do_cluster_data(Labels);
cluster_data(_) ->
    Labels = [{node, node(self())}],
    do_cluster_data(Labels).

do_cluster_data(Labels) ->
    Running = emqx:cluster_nodes(running),
    Stopped = emqx:cluster_nodes(stopped),
    #{
        emqx_cluster_nodes_running => [{Labels, length(Running)}],
        emqx_cluster_nodes_stopped => [{Labels, length(Stopped)}]
    }.

%%========================================
%% Metrics
%%========================================

emqx_metric_data(MetricNameTypeKeyL, Mode) ->
    emqx_metric_data(MetricNameTypeKeyL, Mode, _Acc = #{}).

emqx_metric_data(MetricNameTypeKeyL, Mode, Acc) ->
    Metrics = emqx_metrics:all(),
    lists:foldl(
        fun
            ({_Name, _Type, undefined}, AccIn) ->
                AccIn;
            ({Name, _Type, MetricKAtom}, AccIn) ->
                AccIn#{Name => [{with_node_label(Mode, []), ?C(MetricKAtom, Metrics)}]}
        end,
        Acc,
        MetricNameTypeKeyL
    ).

client_metric_data(Mode) ->
    Acc = listener_shutdown_counts(Mode),
    emqx_metric_data(client_metric_meta(), Mode, Acc).

listener_shutdown_counts(Mode) ->
    Data =
        lists:flatmap(
            fun(Listener) ->
                get_listener_shutdown_counts_with_labels(Listener, Mode)
            end,
            emqx_listeners:list()
        ),
    #{emqx_client_disconnected_reason => Data}.

get_listener_shutdown_counts_with_labels({Id, #{bind := Bind, running := true}}, Mode) ->
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(Id),
    AddLabels = fun({Reason, Count}) ->
        Labels = [
            {listener_type, Type},
            {listener_name, Name},
            {reason, Reason}
        ],
        {with_node_label(Mode, Labels), Count}
    end,
    case emqx_listeners:shutdown_count(Id, Bind) of
        {error, _} ->
            [];
        Counts ->
            lists:map(AddLabels, Counts)
    end;
get_listener_shutdown_counts_with_labels({_Id, #{running := false}}, _Mode) ->
    [].

%%==========
%% Durable Storage
maybe_add_ds_meta() ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            #{
                ds_data => meta_to_init_from(emqx_ds_builtin_metrics:prometheus_meta()),
                ds_raft_node_data => meta_to_init_from(ds_raft_node_meta())
            };
        false ->
            #{}
    end.

ds_raft_node_meta() ->
    emqx_ds_builtin_raft_metrics:local_dbs_meta() ++
        emqx_ds_builtin_raft_metrics:local_shards_meta().

ds_raft_cluster_meta() ->
    emqx_ds_builtin_raft_metrics:cluster_meta() ++
        emqx_ds_builtin_raft_metrics:dbs_meta() ++
        emqx_ds_builtin_raft_metrics:shards_meta().

%%==========
%% Bytes && Packets
emqx_packet_metric_meta() ->
    [
        {emqx_bytes_received, counter, 'bytes.received'},
        {emqx_bytes_sent, counter, 'bytes.sent'},
        %% received.sent
        {emqx_packets_received, counter, 'packets.received'},
        {emqx_packets_sent, counter, 'packets.sent'},
        %% connect
        {emqx_packets_connect, counter, 'packets.connect.received'},
        {emqx_packets_connack_sent, counter, 'packets.connack.sent'},
        {emqx_packets_connack_error, counter, 'packets.connack.error'},
        {emqx_packets_connack_auth_error, counter, 'packets.connack.auth_error'},
        %% sub.unsub
        {emqx_packets_subscribe_received, counter, 'packets.subscribe.received'},
        {emqx_packets_subscribe_auth_error, counter, 'packets.subscribe.auth_error'},
        {emqx_packets_subscribe_error, counter, 'packets.subscribe.error'},
        {emqx_packets_suback_sent, counter, 'packets.suback.sent'},
        {emqx_packets_unsubscribe_received, counter, 'packets.unsubscribe.received'},
        {emqx_packets_unsubscribe_error, counter, 'packets.unsubscribe.error'},
        {emqx_packets_unsuback_sent, counter, 'packets.unsuback.sent'},
        %% publish.puback
        {emqx_packets_publish_received, counter, 'packets.publish.received'},
        {emqx_packets_publish_sent, counter, 'packets.publish.sent'},
        {emqx_packets_publish_inuse, counter, 'packets.publish.inuse'},
        {emqx_packets_publish_error, counter, 'packets.publish.error'},
        {emqx_packets_publish_auth_error, counter, 'packets.publish.auth_error'},
        %% puback
        {emqx_packets_puback_received, counter, 'packets.puback.received'},
        {emqx_packets_puback_sent, counter, 'packets.puback.sent'},
        {emqx_packets_puback_inuse, counter, 'packets.puback.inuse'},
        {emqx_packets_puback_missed, counter, 'packets.puback.missed'},
        %% pubrec
        {emqx_packets_pubrec_received, counter, 'packets.pubrec.received'},
        {emqx_packets_pubrec_sent, counter, 'packets.pubrec.sent'},
        {emqx_packets_pubrec_inuse, counter, 'packets.pubrec.inuse'},
        {emqx_packets_pubrec_missed, counter, 'packets.pubrec.missed'},
        %% pubrel
        {emqx_packets_pubrel_received, counter, 'packets.pubrel.received'},
        {emqx_packets_pubrel_sent, counter, 'packets.pubrel.sent'},
        {emqx_packets_pubrel_missed, counter, 'packets.pubrel.missed'},
        %% pubcomp
        {emqx_packets_pubcomp_received, counter, 'packets.pubcomp.received'},
        {emqx_packets_pubcomp_sent, counter, 'packets.pubcomp.sent'},
        {emqx_packets_pubcomp_inuse, counter, 'packets.pubcomp.inuse'},
        {emqx_packets_pubcomp_missed, counter, 'packets.pubcomp.missed'},
        %% pingreq
        {emqx_packets_pingreq_received, counter, 'packets.pingreq.received'},
        {emqx_packets_pingresp_sent, counter, 'packets.pingresp.sent'},
        %% disconnect
        {emqx_packets_disconnect_received, counter, 'packets.disconnect.received'},
        {emqx_packets_disconnect_sent, counter, 'packets.disconnect.sent'},
        %% auth
        {emqx_packets_auth_received, counter, 'packets.auth.received'},
        {emqx_packets_auth_sent, counter, 'packets.auth.sent'}
    ].

%%==========
%% Messages
message_metric_meta() ->
    [
        {emqx_messages_received, counter, 'messages.received'},
        {emqx_messages_sent, counter, 'messages.sent'},
        {emqx_messages_qos0_received, counter, 'messages.qos0.received'},
        {emqx_messages_qos0_sent, counter, 'messages.qos0.sent'},
        {emqx_messages_qos1_received, counter, 'messages.qos1.received'},
        {emqx_messages_qos1_sent, counter, 'messages.qos1.sent'},
        {emqx_messages_qos2_received, counter, 'messages.qos2.received'},
        {emqx_messages_qos2_sent, counter, 'messages.qos2.sent'},
        {emqx_messages_publish, counter, 'messages.publish'},
        {emqx_messages_dropped, counter, 'messages.dropped'},
        {emqx_messages_dropped_expired, counter, 'messages.dropped.await_pubrel_timeout'},
        {emqx_messages_dropped_no_subscribers, counter, 'messages.dropped.no_subscribers'},
        {emqx_messages_dropped_quota_exceeded, counter, 'messages.dropped.quota_exceeded'},
        {emqx_messages_dropped_receive_maximum, counter, 'messages.dropped.receive_maximum'},
        {emqx_messages_forward, counter, 'messages.forward'},
        {emqx_messages_retained, counter, 'messages.retained'},
        {emqx_messages_delayed, counter, 'messages.delayed'},
        {emqx_messages_delivered, counter, 'messages.delivered'},
        {emqx_messages_acked, counter, 'messages.acked'}
    ].

%%==========
%% Delivery
delivery_metric_meta() ->
    [
        {emqx_delivery_dropped, counter, 'delivery.dropped'},
        {emqx_delivery_dropped_no_local, counter, 'delivery.dropped.no_local'},
        {emqx_delivery_dropped_too_large, counter, 'delivery.dropped.too_large'},
        {emqx_delivery_dropped_qos0_msg, counter, 'delivery.dropped.qos0_msg'},
        {emqx_delivery_dropped_queue_full, counter, 'delivery.dropped.queue_full'},
        {emqx_delivery_dropped_expired, counter, 'delivery.dropped.expired'}
    ].

%%==========
%% Client
client_metric_meta() ->
    [
        {emqx_client_connect, counter, 'client.connect'},
        {emqx_client_connack, counter, 'client.connack'},
        {emqx_client_connected, counter, 'client.connected'},
        {emqx_client_authenticate, counter, 'client.authenticate'},
        {emqx_client_auth_anonymous, counter, 'client.auth.anonymous'},
        {emqx_client_authorize, counter, 'client.authorize'},
        {emqx_client_subscribe, counter, 'client.subscribe'},
        {emqx_client_unsubscribe, counter, 'client.unsubscribe'},
        {emqx_client_disconnected, counter, 'client.disconnected'},
        {emqx_client_disconnected_reason, counter, undefined}
    ].

%%==========
%% Metrics - session
session_metric_meta() ->
    [
        {emqx_session_created, counter, 'session.created'},
        {emqx_session_resumed, counter, 'session.resumed'},
        {emqx_session_takenover, counter, 'session.takenover'},
        {emqx_session_discarded, counter, 'session.discarded'},
        {emqx_session_terminated, counter, 'session.terminated'}
    ].

%%==========
%% Metrics - acl
acl_metric_meta() ->
    [
        {emqx_authorization_allow, counter, 'authorization.allow'},
        {emqx_authorization_deny, counter, 'authorization.deny'},
        {emqx_authorization_cache_hit, counter, 'authorization.cache_hit'},
        {emqx_authorization_cache_miss, counter, 'authorization.cache_miss'},
        {emqx_authorization_superuser, counter, 'authorization.superuser'},
        {emqx_authorization_nomatch, counter, 'authorization.nomatch'},
        {emqx_authorization_matched_allow, counter, 'authorization.matched_allow'},
        {emqx_authorization_matched_deny, counter, 'authorization.matched_deny'}
    ].

%%==========
%% Metrics - authn
authn_metric_meta() ->
    [
        {emqx_authentication_success, counter, 'authentication.success'},
        {emqx_authentication_success_anonymous, counter, 'authentication.success.anonymous'},
        {emqx_authentication_failure, counter, 'authentication.failure'}
    ].

%%==========
%% Overload Protection
olp_metric_meta() ->
    emqx_metrics_olp_meta(emqx_config_zones:is_olp_enabled()).

emqx_metrics_olp_meta(true) ->
    [
        {emqx_overload_protection_delay_ok, counter, 'overload_protection.delay.ok'},
        {emqx_overload_protection_delay_timeout, counter, 'overload_protection.delay.timeout'},
        {emqx_overload_protection_hibernation, counter, 'overload_protection.hibernation'},
        {emqx_overload_protection_gc, counter, 'overload_protection.gc'},
        {emqx_overload_protection_new_conn, counter, 'overload_protection.new_conn'}
    ];
emqx_metrics_olp_meta(false) ->
    [].

%%========================================
%% License
%%========================================

-if(?EMQX_RELEASE_EDITION == ee).

maybe_license_add_collect_family(Callback, RawData) ->
    ok = add_collect_family(Callback, license_metric_meta(), ?MG(license_data, RawData)),
    ok.

maybe_license_fetch_data() ->
    #{license_data => license_data()}.

maybe_license_collect_json_data(RawData) ->
    #{license => ?MG(license_data, RawData)}.

%% license
license_metric_meta() ->
    [
        {emqx_license_expiry_at, gauge, undefined}
    ].

license_data() ->
    #{emqx_license_expiry_at => emqx_license_checker:expiry_epoch()}.

-else.

maybe_license_add_collect_family(_, _) ->
    ok.

maybe_license_fetch_data() ->
    #{}.

maybe_license_collect_json_data(_RawData) ->
    #{}.

-endif.

%%========================================
%% Certs
%%========================================

cert_metric_meta() ->
    [
        {emqx_cert_expiry_at, gauge, undefined}
    ].

-define(LISTENER_TYPES, [ssl, wss, quic]).

-spec cert_data() ->
    [_Point :: {[Label], Epoch}]
when
    Label :: TypeLabel | NameLabel,
    TypeLabel :: {listener_type, ssl | wss | quic},
    NameLabel :: {listener_name, atom()},
    Epoch :: non_neg_integer().
cert_data() ->
    cert_data(emqx_config:get([listeners], undefined)).

cert_data(undefined) ->
    [];
cert_data(AllListeners) ->
    Points = lists:foldl(
        fun(ListenerType, PointsAcc) ->
            lists:append(PointsAcc, points_of_listeners(ListenerType, AllListeners))
        end,
        _PointsInitAcc = [],
        ?LISTENER_TYPES
    ),
    #{
        emqx_cert_expiry_at => Points
    }.

points_of_listeners(Type, AllListeners) ->
    do_points_of_listeners(Type, maps:get(Type, AllListeners, undefined)).

-spec do_points_of_listeners(Type, Listeners) ->
    [_Point :: {[{LabelKey, LabelValue}], Epoch}]
when
    Type :: ssl | wss | quic,
    Listeners :: #{ListenerName :: atom() => ListenerConf :: map()} | undefined,
    LabelKey :: atom(),
    LabelValue :: atom(),
    Epoch :: non_neg_integer().
do_points_of_listeners(_, undefined) ->
    [];
do_points_of_listeners(Type, Listeners) ->
    lists:foldl(
        fun(Name, PointsAcc) ->
            case
                emqx_utils_maps:deep_get([Name, enable], Listeners, false) andalso
                    emqx_utils_maps:deep_get(
                        [Name, ssl_options, certfile], Listeners, undefined
                    )
            of
                false -> PointsAcc;
                undefined -> PointsAcc;
                Path -> [gen_point_cert_expiry_at(Type, Name, Path) | PointsAcc]
            end
        end,
        [],
        %% listener names
        maps:keys(Listeners)
    ).

gen_point_cert_expiry_at(Type, Name, Path) ->
    {[{listener_type, Type}, {listener_name, Name}], cert_expiry_at_from_path(Path)}.

%% TODO: cert manager for more generic utils functions
cert_expiry_at_from_path(Path0) ->
    Path = emqx_schema:naive_env_interpolation(Path0),
    try
        case file:read_file(Path) of
            {ok, PemBin} ->
                [CertEntry | _] = public_key:pem_decode(PemBin),
                Cert = public_key:pem_entry_decode(CertEntry),
                %% XXX: Only pem cert supported by listeners
                not_after_epoch(Cert);
            {error, Reason} ->
                ?SLOG(error, #{
                    msg => "read_cert_file_failed",
                    path => Path0,
                    resolved_path => Path,
                    reason => Reason
                }),
                0
        end
    catch
        E:R:S ->
            ?SLOG(error, #{
                msg => "obtain_cert_expiry_time_failed",
                error => E,
                reason => R,
                stacktrace => S,
                path => Path0,
                resolved_path => Path
            }),
            0
    end.

%% 62167219200 =:= calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}).
-define(EPOCH_START, 62167219200).
not_after_epoch(#'Certificate'{
    'tbsCertificate' = #'TBSCertificate'{
        validity =
            #'Validity'{'notAfter' = NotAfter}
    }
}) ->
    pubkey_cert:'time_str_2_gregorian_sec'(NotAfter) - ?EPOCH_START;
not_after_epoch(_) ->
    0.

%%========================================
%% Cluster RPC
%%========================================

cluster_rpc_meta() ->
    [{emqx_conf_sync_txid, gauge, undefined}].

%%========================================
%% Mria
%%========================================

mria_metric_meta() ->
    mria_metric_meta(core) ++ mria_metric_meta(replicant).

mria_metric_meta(core) ->
    [
        {emqx_mria_last_intercepted_trans, gauge, last_intercepted_trans},
        {emqx_mria_weight, gauge, weight},
        {emqx_mria_replicants, gauge, replicants},
        {emqx_mria_server_mql, gauge, server_mql}
    ];
mria_metric_meta(replicant) ->
    [
        {emqx_mria_lag, gauge, lag},
        {emqx_mria_bootstrap_time, gauge, bootstrap_time},
        {emqx_mria_bootstrap_num_keys, gauge, bootstrap_num_keys},
        {emqx_mria_message_queue_len, gauge, message_queue_len},
        {emqx_mria_replayq_len, gauge, replayq_len}
    ].

cluster_rpc_data(Mode) ->
    Labels =
        case Mode of
            ?PROM_DATA_MODE__NODE -> [];
            _ -> [{node, node()}]
        end,
    Value = ?SAFELY(emqx_cluster_rpc:get_current_tnx_id(), undefined),
    #{
        emqx_conf_sync_txid => [{Labels, Value}]
    }.

mria_data(Mode) ->
    case mria_rlog:backend() of
        rlog ->
            mria_data(mria_rlog:role(), Mode);
        mnesia ->
            #{}
    end.

mria_data(Role, Mode) ->
    lists:foldl(
        fun({Name, _Type, MetricK}, AccIn) ->
            %% TODO: only report shards that are up
            AccIn#{Name => ?SAFELY(get_shard_metrics(Mode, MetricK), [])}
        end,
        #{},
        mria_metric_meta(Role)
    ).

get_shard_metrics(Mode, MetricK) ->
    Labels =
        case Mode of
            ?PROM_DATA_MODE__NODE -> [];
            _ -> [{node, node()}]
        end,
    [
        {[{shard, Shard} | Labels], get_shard_metric(MetricK, Shard)}
     || Shard <- mria_schema:shards(), Shard =/= undefined
    ].

get_shard_metric(replicants, Shard) ->
    length(mria_status:agents(Shard));
get_shard_metric(Metric, Shard) ->
    case mria_status:get_shard_stats(Shard) of
        #{Metric := Value} when is_number(Value) ->
            Value;
        _ ->
            undefined
    end.

%%========================================
%% Broker Instrumentation
%%========================================

emqx_broker_instr_data(_Mode) ->
    maps:map(
        fun(N, _) ->
            maps:to_list(emqx_metrics_worker:get_hists(?BROKER_INSTR_METRICS_WORKER, N))
        end,
        emqx_broker_instr_init()
    ).

emqx_broker_instr_init() ->
    maps:from_keys(?BROKER_INSTR_METRICS, []).

collect_broker_instr_family(Callback, Metrics) ->
    maps:foreach(
        fun(Id, Ms) ->
            lists:foreach(
                fun({Name, M}) ->
                    collect_hist_family(Callback, Id, Name, M)
                end,
                Ms
            )
        end,
        Metrics
    ).

collect_hist_family(Callback, Id, Name, #{count := Count, sum := Sum, bucket_counts := Buckets}) ->
    MName = [<<"emqx_instr_">>, atom_to_binary(Id), <<"_">>, atom_to_binary(Name)],
    Metric = prometheus_model_helpers:histogram_metric([{node, node()}], Buckets, Count, Sum),
    Callback(prometheus_model_helpers:create_mf(MName, <<>>, histogram, Metric)).

%%--------------------------------------------------------------------
%% Collect functions
%%--------------------------------------------------------------------

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merge / zip formatting funcs for type `application/json`

collect_stats_json_data(StatsData, StatsClData) ->
    StatsDatas = collect_json_data_(StatsData),
    CLData = hd(collect_json_data_(StatsClData)),
    Res = lists:map(
        fun(NodeData) ->
            maps:merge(NodeData, CLData)
        end,
        StatsDatas
    ),
    json_obj_or_array(Res).

%% always return json array
collect_cert_json_data(Data) ->
    collect_json_data_(Data).

collect_client_json_data(Data0) ->
    ShutdownCounts = maps:with([emqx_client_disconnected_reason], Data0),
    Data = maps:without([emqx_client_disconnected_reason], Data0),
    JSON0 = collect_json_data(Data),
    JSON1 = collect_json_data_(ShutdownCounts),
    lists:flatten([JSON0 | JSON1]).

collect_vm_json_data(Data) ->
    DataListPerNode = collect_json_data_(Data),
    case ?GET_PROM_DATA_MODE() of
        ?PROM_DATA_MODE__NODE ->
            hd(DataListPerNode);
        _ ->
            DataListPerNode
    end.

collect_json_data(Data0) ->
    DataListPerNode = collect_json_data_(Data0),
    json_obj_or_array(DataListPerNode).

%% compatibility with previous api format in json mode
json_obj_or_array(DataL) ->
    case ?GET_PROM_DATA_MODE() of
        ?PROM_DATA_MODE__NODE ->
            data_list_to_json_obj(DataL);
        ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED ->
            DataL;
        ?PROM_DATA_MODE__ALL_NODES_AGGREGATED ->
            data_list_to_json_obj(DataL)
    end.

data_list_to_json_obj([]) ->
    %% olp maybe not enabled, with empty list to empty object
    #{};
data_list_to_json_obj(DataL) ->
    hd(DataL).

collect_json_data_(Data) ->
    emqx_prometheus_cluster:collect_json_data(Data, fun zip_json_prom_stats_metrics/3).

zip_json_prom_stats_metrics(Key, Points, [] = _AccIn) ->
    lists:foldl(
        fun({Labels, Metric}, AccIn2) ->
            LabelsKVMap = maps:from_list(Labels),
            Point = LabelsKVMap#{Key => Metric},
            [Point | AccIn2]
        end,
        [],
        Points
    );
zip_json_prom_stats_metrics(Key, Points, AllResultedAcc) ->
    ThisKeyResult = lists:foldl(emqx_prometheus_cluster:point_to_map_fun(Key), [], Points),
    lists:zipwith(fun maps:merge/2, AllResultedAcc, ThisKeyResult).

meta_to_init_from(Meta) ->
    maps:from_keys(metrics_name(Meta), []).

metrics_name(MetricsAll) ->
    [Name || {Name, _, _} <- MetricsAll].

with_node_label(?PROM_DATA_MODE__NODE, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, Labels) ->
    [{node, node()} | Labels].

%%--------------------------------------------------------------------
%% bpapi

%% deprecated_since 5.0.10, remove this when 5.1.x
do_start() ->
    emqx_prometheus_sup:start_child(?APP).

%% deprecated_since 5.0.10, remove this when 5.1.x
do_stop() ->
    emqx_prometheus_sup:stop_child(?APP).

%%--------------------------------------------------------------------
%% prometheus_model_helpers proxy
%%
gauge_metric(Metric) ->
    prometheus_model_helpers:gauge_metric(Metric).

gauge_metrics(Values) ->
    prometheus_model_helpers:gauge_metrics(Values).

counter_metrics(Specs) ->
    prometheus_model_helpers:counter_metrics(Specs).
