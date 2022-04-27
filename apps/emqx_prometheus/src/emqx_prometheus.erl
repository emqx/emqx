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

-module(emqx_prometheus).

-behaviour(gen_server).

%% Please don't remove this attribute, it will
%% be used by the prometheus application
-behaviour(prometheus_collector).

-include("emqx_prometheus.hrl").

-include_lib("prometheus/include/prometheus.hrl").
-include_lib("prometheus/include/prometheus_model.hrl").
-include_lib("emqx/include/logger.hrl").

-import(
    prometheus_model_helpers,
    [
        create_mf/5,
        gauge_metric/1,
        counter_metric/1
    ]
).

-export([
    update/1,
    start/0,
    stop/0,
    restart/0,
    % for rpc
    do_start/0,
    do_stop/0
]).

%% APIs
-export([start_link/1]).

%% gen_server callbacks
-export([
    init/1,
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

-define(C(K, L), proplists:get_value(K, L, 0)).

-define(TIMER_MSG, '#interval').

-record(state, {push_gateway, timer, interval}).

%%--------------------------------------------------------------------
%% update new config
update(Config) ->
    case
        emqx_conf:update(
            [prometheus],
            Config,
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, #{raw_config := NewConfigRows}} ->
            case maps:get(<<"enable">>, Config, true) of
                true ->
                    ok = restart();
                false ->
                    ok = stop()
            end,
            {ok, NewConfigRows};
        {error, Reason} ->
            {error, Reason}
    end.

start() ->
    {_, []} = emqx_prometheus_proto_v1:start(mria_mnesia:running_nodes()),
    ok.

stop() ->
    {_, []} = emqx_prometheus_proto_v1:stop(mria_mnesia:running_nodes()),
    ok.

restart() ->
    ok = stop(),
    ok = start().

do_start() ->
    emqx_prometheus_sup:start_child(?APP, emqx_conf:get([prometheus])).

do_stop() ->
    case emqx_prometheus_sup:stop_child(?APP) of
        ok ->
            ok;
        {error, not_found} ->
            ok
    end.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Opts]) ->
    Interval = maps:get(interval, Opts),
    PushGateway = maps:get(push_gateway_server, Opts),
    {ok, ensure_timer(#state{push_gateway = PushGateway, interval = Interval})}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, R, ?TIMER_MSG}, State = #state{timer = R, push_gateway = Uri}) ->
    [Name, Ip] = string:tokens(atom_to_list(node()), "@"),
    Url = lists:concat([Uri, "/metrics/job/", Name, "/instance/", Name, "~", Ip]),
    Data = prometheus_text_format:format(),
    httpc:request(post, {Url, [], "text/plain", Data}, [{autoredirect, true}], []),
    {noreply, ensure_timer(State)};
handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

ensure_timer(State = #state{interval = Interval}) ->
    State#state{timer = emqx_misc:start_timer(Interval, ?TIMER_MSG)}.
%%--------------------------------------------------------------------
%% prometheus callbacks
%%--------------------------------------------------------------------

deregister_cleanup(_Registry) ->
    ok.

collect_mf(_Registry, Callback) ->
    Metrics = emqx_metrics:all(),
    Stats = emqx_stats:getstats(),
    VMData = emqx_vm_data(),
    ClusterData = emqx_cluster_data(),
    _ = [add_collect_family(Name, Stats, Callback, gauge) || Name <- emqx_stats()],
    _ = [add_collect_family(Name, VMData, Callback, gauge) || Name <- emqx_vm()],
    _ = [add_collect_family(Name, ClusterData, Callback, gauge) || Name <- emqx_cluster()],
    _ = [add_collect_family(Name, Metrics, Callback, counter) || Name <- emqx_metrics_packets()],
    _ = [add_collect_family(Name, Metrics, Callback, counter) || Name <- emqx_metrics_messages()],
    _ = [add_collect_family(Name, Metrics, Callback, counter) || Name <- emqx_metrics_delivery()],
    _ = [add_collect_family(Name, Metrics, Callback, counter) || Name <- emqx_metrics_client()],
    _ = [add_collect_family(Name, Metrics, Callback, counter) || Name <- emqx_metrics_session()],
    ok.

%% @private
collect(<<"json">>) ->
    Metrics = emqx_metrics:all(),
    Stats = emqx_stats:getstats(),
    VMData = emqx_vm_data(),
    #{
        stats => maps:from_list([collect_stats(Name, Stats) || Name <- emqx_stats()]),
        metrics => maps:from_list([collect_stats(Name, VMData) || Name <- emqx_vm()]),
        packets => maps:from_list([collect_stats(Name, Metrics) || Name <- emqx_metrics_packets()]),
        messages => maps:from_list([collect_stats(Name, Metrics) || Name <- emqx_metrics_messages()]),
        delivery => maps:from_list([collect_stats(Name, Metrics) || Name <- emqx_metrics_delivery()]),
        client => maps:from_list([collect_stats(Name, Metrics) || Name <- emqx_metrics_client()]),
        session => maps:from_list([collect_stats(Name, Metrics) || Name <- emqx_metrics_session()])
    };
collect(<<"prometheus">>) ->
    prometheus_text_format:format().

%% @private
collect_stats(Name, Stats) ->
    R = collect_metrics(Name, Stats),
    case R#'Metric'.gauge of
        undefined ->
            {_, Val} = R#'Metric'.counter,
            {Name, Val};
        {_, Val} ->
            {Name, Val}
    end.

collect_metrics(Name, Metrics) ->
    emqx_collect(Name, Metrics).

add_collect_family(Name, Data, Callback, Type) ->
    Callback(create_schema(Name, <<"">>, Data, Type)).

create_schema(Name, Help, Data, Type) ->
    create_mf(Name, Help, Type, ?MODULE, Data).

%%--------------------------------------------------------------------
%% Collector
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Stats

%% connections
emqx_collect(emqx_connections_count, Stats) ->
    gauge_metric(?C('connections.count', Stats));
emqx_collect(emqx_connections_max, Stats) ->
    gauge_metric(?C('connections.max', Stats));
%% sessions
emqx_collect(emqx_sessions_count, Stats) ->
    gauge_metric(?C('sessions.count', Stats));
emqx_collect(emqx_sessions_max, Stats) ->
    gauge_metric(?C('sessions.max', Stats));
%% pub/sub stats
emqx_collect(emqx_topics_count, Stats) ->
    gauge_metric(?C('topics.count', Stats));
emqx_collect(emqx_topics_max, Stats) ->
    gauge_metric(?C('topics.max', Stats));
emqx_collect(emqx_suboptions_count, Stats) ->
    gauge_metric(?C('suboptions.count', Stats));
emqx_collect(emqx_suboptions_max, Stats) ->
    gauge_metric(?C('suboptions.max', Stats));
emqx_collect(emqx_subscribers_count, Stats) ->
    gauge_metric(?C('subscribers.count', Stats));
emqx_collect(emqx_subscribers_max, Stats) ->
    gauge_metric(?C('subscribers.max', Stats));
emqx_collect(emqx_subscriptions_count, Stats) ->
    gauge_metric(?C('subscriptions.count', Stats));
emqx_collect(emqx_subscriptions_max, Stats) ->
    gauge_metric(?C('subscriptions.max', Stats));
emqx_collect(emqx_subscriptions_shared_count, Stats) ->
    gauge_metric(?C('subscriptions.shared.count', Stats));
emqx_collect(emqx_subscriptions_shared_max, Stats) ->
    gauge_metric(?C('subscriptions.shared.max', Stats));
%% retained
emqx_collect(emqx_retained_count, Stats) ->
    gauge_metric(?C('retained.count', Stats));
emqx_collect(emqx_retained_max, Stats) ->
    gauge_metric(?C('retained.max', Stats));
%%--------------------------------------------------------------------
%% Metrics - packets & bytes

%% bytes
emqx_collect(emqx_bytes_received, Metrics) ->
    counter_metric(?C('bytes.received', Metrics));
emqx_collect(emqx_bytes_sent, Metrics) ->
    counter_metric(?C('bytes.sent', Metrics));
%% received.sent
emqx_collect(emqx_packets_received, Metrics) ->
    counter_metric(?C('packets.received', Metrics));
emqx_collect(emqx_packets_sent, Metrics) ->
    counter_metric(?C('packets.sent', Metrics));
%% connect
emqx_collect(emqx_packets_connect, Metrics) ->
    counter_metric(?C('packets.connect.received', Metrics));
emqx_collect(emqx_packets_connack_sent, Metrics) ->
    counter_metric(?C('packets.connack.sent', Metrics));
emqx_collect(emqx_packets_connack_error, Metrics) ->
    counter_metric(?C('packets.connack.error', Metrics));
emqx_collect(emqx_packets_connack_auth_error, Metrics) ->
    counter_metric(?C('packets.connack.auth_error', Metrics));
%% sub.unsub
emqx_collect(emqx_packets_subscribe_received, Metrics) ->
    counter_metric(?C('packets.subscribe.received', Metrics));
emqx_collect(emqx_packets_subscribe_auth_error, Metrics) ->
    counter_metric(?C('packets.subscribe.auth_error', Metrics));
emqx_collect(emqx_packets_subscribe_error, Metrics) ->
    counter_metric(?C('packets.subscribe.error', Metrics));
emqx_collect(emqx_packets_suback_sent, Metrics) ->
    counter_metric(?C('packets.suback.sent', Metrics));
emqx_collect(emqx_packets_unsubscribe_received, Metrics) ->
    counter_metric(?C('packets.unsubscribe.received', Metrics));
emqx_collect(emqx_packets_unsubscribe_error, Metrics) ->
    counter_metric(?C('packets.unsubscribe.error', Metrics));
emqx_collect(emqx_packets_unsuback_sent, Metrics) ->
    counter_metric(?C('packets.unsuback.sent', Metrics));
%% publish.puback
emqx_collect(emqx_packets_publish_received, Metrics) ->
    counter_metric(?C('packets.publish.received', Metrics));
emqx_collect(emqx_packets_publish_sent, Metrics) ->
    counter_metric(?C('packets.publish.sent', Metrics));
emqx_collect(emqx_packets_publish_inuse, Metrics) ->
    counter_metric(?C('packets.publish.inuse', Metrics));
emqx_collect(emqx_packets_publish_error, Metrics) ->
    counter_metric(?C('packets.publish.error', Metrics));
emqx_collect(emqx_packets_publish_auth_error, Metrics) ->
    counter_metric(?C('packets.publish.auth_error', Metrics));
emqx_collect(emqx_packets_publish_dropped, Metrics) ->
    counter_metric(?C('packets.publish.dropped', Metrics));
%% puback
emqx_collect(emqx_packets_puback_received, Metrics) ->
    counter_metric(?C('packets.puback.received', Metrics));
emqx_collect(emqx_packets_puback_sent, Metrics) ->
    counter_metric(?C('packets.puback.sent', Metrics));
emqx_collect(emqx_packets_puback_inuse, Metrics) ->
    counter_metric(?C('packets.puback.inuse', Metrics));
emqx_collect(emqx_packets_puback_missed, Metrics) ->
    counter_metric(?C('packets.puback.missed', Metrics));
%% pubrec
emqx_collect(emqx_packets_pubrec_received, Metrics) ->
    counter_metric(?C('packets.pubrec.received', Metrics));
emqx_collect(emqx_packets_pubrec_sent, Metrics) ->
    counter_metric(?C('packets.pubrec.sent', Metrics));
emqx_collect(emqx_packets_pubrec_inuse, Metrics) ->
    counter_metric(?C('packets.pubrec.inuse', Metrics));
emqx_collect(emqx_packets_pubrec_missed, Metrics) ->
    counter_metric(?C('packets.pubrec.missed', Metrics));
%% pubrel
emqx_collect(emqx_packets_pubrel_received, Metrics) ->
    counter_metric(?C('packets.pubrel.received', Metrics));
emqx_collect(emqx_packets_pubrel_sent, Metrics) ->
    counter_metric(?C('packets.pubrel.sent', Metrics));
emqx_collect(emqx_packets_pubrel_missed, Metrics) ->
    counter_metric(?C('packets.pubrel.missed', Metrics));
%% pubcomp
emqx_collect(emqx_packets_pubcomp_received, Metrics) ->
    counter_metric(?C('packets.pubcomp.received', Metrics));
emqx_collect(emqx_packets_pubcomp_sent, Metrics) ->
    counter_metric(?C('packets.pubcomp.sent', Metrics));
emqx_collect(emqx_packets_pubcomp_inuse, Metrics) ->
    counter_metric(?C('packets.pubcomp.inuse', Metrics));
emqx_collect(emqx_packets_pubcomp_missed, Metrics) ->
    counter_metric(?C('packets.pubcomp.missed', Metrics));
%% pingreq
emqx_collect(emqx_packets_pingreq_received, Metrics) ->
    counter_metric(?C('packets.pingreq.received', Metrics));
emqx_collect(emqx_packets_pingresp_sent, Metrics) ->
    counter_metric(?C('packets.pingresp.sent', Metrics));
%% disconnect
emqx_collect(emqx_packets_disconnect_received, Metrics) ->
    counter_metric(?C('packets.disconnect.received', Metrics));
emqx_collect(emqx_packets_disconnect_sent, Metrics) ->
    counter_metric(?C('packets.disconnect.sent', Metrics));
%% auth
emqx_collect(emqx_packets_auth_received, Metrics) ->
    counter_metric(?C('packets.auth.received', Metrics));
emqx_collect(emqx_packets_auth_sent, Metrics) ->
    counter_metric(?C('packets.auth.sent', Metrics));
%%--------------------------------------------------------------------
%% Metrics - messages

%% messages
emqx_collect(emqx_messages_received, Metrics) ->
    counter_metric(?C('messages.received', Metrics));
emqx_collect(emqx_messages_sent, Metrics) ->
    counter_metric(?C('messages.sent', Metrics));
emqx_collect(emqx_messages_qos0_received, Metrics) ->
    counter_metric(?C('messages.qos0.received', Metrics));
emqx_collect(emqx_messages_qos0_sent, Metrics) ->
    counter_metric(?C('messages.qos0.sent', Metrics));
emqx_collect(emqx_messages_qos1_received, Metrics) ->
    counter_metric(?C('messages.qos1.received', Metrics));
emqx_collect(emqx_messages_qos1_sent, Metrics) ->
    counter_metric(?C('messages.qos1.sent', Metrics));
emqx_collect(emqx_messages_qos2_received, Metrics) ->
    counter_metric(?C('messages.qos2.received', Metrics));
emqx_collect(emqx_messages_qos2_sent, Metrics) ->
    counter_metric(?C('messages.qos2.sent', Metrics));
emqx_collect(emqx_messages_publish, Metrics) ->
    counter_metric(?C('messages.publish', Metrics));
emqx_collect(emqx_messages_dropped, Metrics) ->
    counter_metric(?C('messages.dropped', Metrics));
emqx_collect(emqx_messages_dropped_expired, Metrics) ->
    counter_metric(?C('messages.dropped.await_pubrel_timeout', Metrics));
emqx_collect(emqx_messages_dropped_no_subscribers, Metrics) ->
    counter_metric(?C('messages.dropped.no_subscribers', Metrics));
emqx_collect(emqx_messages_forward, Metrics) ->
    counter_metric(?C('messages.forward', Metrics));
emqx_collect(emqx_messages_retained, Metrics) ->
    counter_metric(?C('messages.retained', Metrics));
emqx_collect(emqx_messages_delayed, Stats) ->
    counter_metric(?C('messages.delayed', Stats));
emqx_collect(emqx_messages_delivered, Stats) ->
    counter_metric(?C('messages.delivered', Stats));
emqx_collect(emqx_messages_acked, Stats) ->
    counter_metric(?C('messages.acked', Stats));
%%--------------------------------------------------------------------
%% Metrics - delivery

emqx_collect(emqx_delivery_dropped, Stats) ->
    counter_metric(?C('delivery.dropped', Stats));
emqx_collect(emqx_delivery_dropped_no_local, Stats) ->
    counter_metric(?C('delivery.dropped.no_local', Stats));
emqx_collect(emqx_delivery_dropped_too_large, Stats) ->
    counter_metric(?C('delivery.dropped.too_large', Stats));
emqx_collect(emqx_delivery_dropped_qos0_msg, Stats) ->
    counter_metric(?C('delivery.dropped.qos0_msg', Stats));
emqx_collect(emqx_delivery_dropped_queue_full, Stats) ->
    counter_metric(?C('delivery.dropped.queue_full', Stats));
emqx_collect(emqx_delivery_dropped_expired, Stats) ->
    counter_metric(?C('delivery.dropped.expired', Stats));
%%--------------------------------------------------------------------
%% Metrics - client

emqx_collect(emqx_client_connected, Stats) ->
    counter_metric(?C('client.connected', Stats));
emqx_collect(emqx_client_authenticate, Stats) ->
    counter_metric(?C('client.authenticate', Stats));
emqx_collect(emqx_client_auth_anonymous, Stats) ->
    counter_metric(?C('client.auth.anonymous', Stats));
emqx_collect(emqx_client_authorize, Stats) ->
    counter_metric(?C('client.authorize', Stats));
emqx_collect(emqx_client_subscribe, Stats) ->
    counter_metric(?C('client.subscribe', Stats));
emqx_collect(emqx_client_unsubscribe, Stats) ->
    counter_metric(?C('client.unsubscribe', Stats));
emqx_collect(emqx_client_disconnected, Stats) ->
    counter_metric(?C('client.disconnected', Stats));
%%--------------------------------------------------------------------
%% Metrics - session

emqx_collect(emqx_session_created, Stats) ->
    counter_metric(?C('session.created', Stats));
emqx_collect(emqx_session_resumed, Stats) ->
    counter_metric(?C('session.resumed', Stats));
emqx_collect(emqx_session_takenover, Stats) ->
    counter_metric(?C('session.takenover', Stats));
emqx_collect(emqx_session_discarded, Stats) ->
    counter_metric(?C('session.discarded', Stats));
emqx_collect(emqx_session_terminated, Stats) ->
    counter_metric(?C('session.terminated', Stats));
%%--------------------------------------------------------------------
%% VM

emqx_collect(emqx_vm_cpu_use, VMData) ->
    gauge_metric(?C(cpu_use, VMData));
emqx_collect(emqx_vm_cpu_idle, VMData) ->
    gauge_metric(?C(cpu_idle, VMData));
emqx_collect(emqx_vm_run_queue, VMData) ->
    gauge_metric(?C(run_queue, VMData));
emqx_collect(emqx_vm_process_messages_in_queues, VMData) ->
    gauge_metric(?C(process_total_messages, VMData));
emqx_collect(emqx_vm_total_memory, VMData) ->
    gauge_metric(?C(total_memory, VMData));
emqx_collect(emqx_vm_used_memory, VMData) ->
    gauge_metric(?C(used_memory, VMData));
emqx_collect(emqx_cluster_nodes_running, ClusterData) ->
    gauge_metric(?C(nodes_running, ClusterData));
emqx_collect(emqx_cluster_nodes_stopped, ClusterData) ->
    gauge_metric(?C(nodes_stopped, ClusterData)).

%%--------------------------------------------------------------------
%% Indicators
%%--------------------------------------------------------------------

emqx_stats() ->
    [
        emqx_connections_count,
        emqx_connections_max,
        emqx_sessions_count,
        emqx_sessions_max,
        emqx_topics_count,
        emqx_topics_max,
        emqx_suboptions_count,
        emqx_suboptions_max,
        emqx_subscribers_count,
        emqx_subscribers_max,
        emqx_subscriptions_count,
        emqx_subscriptions_max,
        emqx_subscriptions_shared_count,
        emqx_subscriptions_shared_max,
        emqx_retained_count,
        emqx_retained_max
    ].

emqx_metrics_packets() ->
    [
        emqx_bytes_received,
        emqx_bytes_sent,
        emqx_packets_received,
        emqx_packets_sent,
        emqx_packets_connect,
        emqx_packets_connack_sent,
        emqx_packets_connack_error,
        emqx_packets_connack_auth_error,
        emqx_packets_publish_received,
        emqx_packets_publish_sent,
        emqx_packets_publish_inuse,
        emqx_packets_publish_error,
        emqx_packets_publish_auth_error,
        emqx_packets_publish_dropped,
        emqx_packets_puback_received,
        emqx_packets_puback_sent,
        emqx_packets_puback_inuse,
        emqx_packets_puback_missed,
        emqx_packets_pubrec_received,
        emqx_packets_pubrec_sent,
        emqx_packets_pubrec_inuse,
        emqx_packets_pubrec_missed,
        emqx_packets_pubrel_received,
        emqx_packets_pubrel_sent,
        emqx_packets_pubrel_missed,
        emqx_packets_pubcomp_received,
        emqx_packets_pubcomp_sent,
        emqx_packets_pubcomp_inuse,
        emqx_packets_pubcomp_missed,
        emqx_packets_subscribe_received,
        emqx_packets_subscribe_error,
        emqx_packets_subscribe_auth_error,
        emqx_packets_suback_sent,
        emqx_packets_unsubscribe_received,
        emqx_packets_unsubscribe_error,
        emqx_packets_unsuback_sent,
        emqx_packets_pingreq_received,
        emqx_packets_pingresp_sent,
        emqx_packets_disconnect_received,
        emqx_packets_disconnect_sent,
        emqx_packets_auth_received,
        emqx_packets_auth_sent
    ].

emqx_metrics_messages() ->
    [
        emqx_messages_received,
        emqx_messages_sent,
        emqx_messages_qos0_received,
        emqx_messages_qos0_sent,
        emqx_messages_qos1_received,
        emqx_messages_qos1_sent,
        emqx_messages_qos2_received,
        emqx_messages_qos2_sent,
        emqx_messages_publish,
        emqx_messages_dropped,
        emqx_messages_dropped_expired,
        emqx_messages_dropped_no_subscribers,
        emqx_messages_forward,
        emqx_messages_retained,
        emqx_messages_delayed,
        emqx_messages_delivered,
        emqx_messages_acked
    ].

emqx_metrics_delivery() ->
    [
        emqx_delivery_dropped,
        emqx_delivery_dropped_no_local,
        emqx_delivery_dropped_too_large,
        emqx_delivery_dropped_qos0_msg,
        emqx_delivery_dropped_queue_full,
        emqx_delivery_dropped_expired
    ].

emqx_metrics_client() ->
    [
        emqx_client_connected,
        emqx_client_authenticate,
        emqx_client_auth_anonymous,
        emqx_client_authorize,
        emqx_client_subscribe,
        emqx_client_unsubscribe,
        emqx_client_disconnected
    ].

emqx_metrics_session() ->
    [
        emqx_session_created,
        emqx_session_resumed,
        emqx_session_takenover,
        emqx_session_discarded,
        emqx_session_terminated
    ].

emqx_vm() ->
    [
        emqx_vm_cpu_use,
        emqx_vm_cpu_idle,
        emqx_vm_run_queue,
        emqx_vm_process_messages_in_queues,
        emqx_vm_total_memory,
        emqx_vm_used_memory
    ].

emqx_vm_data() ->
    Idle =
        case cpu_sup:util([detailed]) of
            %% Not support for Windows
            {_, 0, 0, _} -> 0;
            {_Num, _Use, IdleList, _} -> ?C(idle, IdleList)
        end,
    RunQueue = erlang:statistics(run_queue),
    [
        {run_queue, RunQueue},
        %% XXX: Plan removed at v5.0
        {process_total_messages, 0},
        {cpu_idle, Idle},
        {cpu_use, 100 - Idle}
    ] ++ emqx_vm:mem_info().

emqx_cluster() ->
    [
        emqx_cluster_nodes_running,
        emqx_cluster_nodes_stopped
    ].

emqx_cluster_data() ->
    #{running_nodes := Running, stopped_nodes := Stopped} = mria_mnesia:cluster_info(),
    [
        {nodes_running, length(Running)},
        {nodes_stopped, length(Stopped)}
    ].
