%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_otel_sampler).

-behaviour(otel_sampler).

-include("emqx_otel_trace.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("opentelemetry/include/otel_sampler.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-define(CLUSTER_ID_KEY, 'cluster.id').

-export([
    init_tables/0,
    store_rule/2,
    store_rule/3,
    purge_rules/0,
    purge_rules/1,
    get_rules/1,
    get_rule/2,
    delete_rule/2,
    record_count/0
]).

%% OpenTelemetry Sampler Callback
-export([setup/1, description/1, should_sample/7]).

%% 2^64 - 1 =:= (2#1 bsl 64 -1)
-define(MAX_VALUE, 18446744073709551615).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

-spec create_tables() -> [mria:table()].
create_tables() ->
    ok = mria:create_table(
        ?EMQX_OTEL_SAMPLER,
        [
            {type, ordered_set},
            {rlog_shard, ?EMQX_OTEL_SAMPLER_SHARD},
            {storage, disc_copies},
            {record_name, ?EMQX_OTEL_SAMPLER},
            {attributes, record_info(fields, ?EMQX_OTEL_SAMPLER)}
        ]
    ),
    [?EMQX_OTEL_SAMPLER].

%% Init
-spec init_tables() -> ok.
init_tables() ->
    ok = mria:wait_for_tables(create_tables()).

%% @doc Update sample rule
-spec store_rule(clientid | topic, binary()) -> ok.
store_rule(clientid, ClientId) ->
    store_rule(clientid, ClientId, #{});
store_rule(topic, TopicName) ->
    store_rule(topic, TopicName, #{}).

-spec store_rule(clientid | topic, binary(), map()) -> ok.
store_rule(clientid, ClientId, Extra) ->
    Record = #?EMQX_OTEL_SAMPLER{
        type = {?EMQX_OTEL_SAMPLE_CLIENTID, ClientId},
        extra = Extra
    },
    mria:dirty_write(Record);
store_rule(topic, TopicName, Extra) ->
    Record = #?EMQX_OTEL_SAMPLER{
        type = {?EMQX_OTEL_SAMPLE_TOPIC, TopicName},
        extra = Extra
    },
    mria:dirty_write(Record).

-spec purge_rules() -> ok.
purge_rules() ->
    do_purge_rules(purge_func('_')).

-spec purge_rules(clientid | topic) -> ok.
purge_rules(clientid) ->
    do_purge_rules(purge_func(?EMQX_OTEL_SAMPLE_CLIENTID));
purge_rules(topic) ->
    do_purge_rules(purge_func(?EMQX_OTEL_SAMPLE_TOPIC)).

do_purge_rules(Func) when is_function(Func) ->
    ok = lists:foreach(Func, mnesia:dirty_all_keys(?EMQX_OTEL_SAMPLER)).

purge_func(K) ->
    fun
        ({I, _} = Key) when I =:= K ->
            ok = mria:dirty_delete(?EMQX_OTEL_SAMPLER, Key);
        (_) ->
            ok
    end.

get_rules(clientid) ->
    read_rules(?EMQX_OTEL_SAMPLE_CLIENTID);
get_rules(topic) ->
    read_rules(?EMQX_OTEL_SAMPLE_TOPIC).

read_rules(K) ->
    mnesia:dirty_match_object(#?EMQX_OTEL_SAMPLER{
        type = {K, '_'},
        _ = '_'
    }).

get_rule(clientid, ClientId) ->
    do_get_rule({?EMQX_OTEL_SAMPLE_CLIENTID, ClientId});
get_rule(topic, Topic) ->
    do_get_rule({?EMQX_OTEL_SAMPLE_TOPIC, Topic}).

do_get_rule(Key) ->
    case mnesia:dirty_read(?EMQX_OTEL_SAMPLER, Key) of
        [#?EMQX_OTEL_SAMPLER{} = R] -> {ok, R};
        [] -> not_found
    end.

delete_rule(clientid, ClientId) ->
    mria:dirty_delete(?EMQX_OTEL_SAMPLER, {?EMQX_OTEL_SAMPLE_CLIENTID, ClientId});
delete_rule(topic, Topic) ->
    mria:dirty_delete(?EMQX_OTEL_SAMPLER, {?EMQX_OTEL_SAMPLE_TOPIC, Topic}).

-spec record_count() -> non_neg_integer().
record_count() ->
    mnesia:table_info(?EMQX_OTEL_SAMPLER, size).

%%--------------------------------------------------------------------
%% OpenTelemetry Sampler Callback
%%--------------------------------------------------------------------

setup(#{sample_ratio := Ratio} = InitOpts) ->
    IdUpper =
        case Ratio of
            R when R =:= +0.0 ->
                0;
            R when R =:= 1.0 ->
                ?MAX_VALUE;
            R when R >= 0.0 andalso R =< 1.0 ->
                trunc(R * ?MAX_VALUE)
        end,

    Opts = (maps:with(
        [
            client_connect_disconnect,
            client_subscribe_unsubscribe,
            client_messaging,
            msg_trace_level,
            cluster_identifier
        ],
        InitOpts
    ))#{
        id_upper => IdUpper
    },

    ?SLOG(debug, #{
        msg => "emqx_otel_sampler_setup",
        opts => Opts
    }),

    Opts.

%% TODO: description
description(_Opts) ->
    <<"AttributeSampler">>.

%% TODO: remote sampled
should_sample(
    Ctx,
    TraceId,
    _Links,
    SpanName,
    _SpanKind,
    Attributes,
    Opts
) when
    SpanName =:= ?CLIENT_CONNECT_SPAN_NAME orelse
        SpanName =:= ?CLIENT_DISCONNECT_SPAN_NAME orelse
        SpanName =:= ?CLIENT_SUBSCRIBE_SPAN_NAME orelse
        SpanName =:= ?CLIENT_UNSUBSCRIBE_SPAN_NAME orelse
        SpanName =:= ?CLIENT_PUBLISH_SPAN_NAME orelse
        SpanName =:= ?BROKER_DISCONNECT_SPAN_NAME orelse
        SpanName =:= ?BROKER_SUBSCRIBE_SPAN_NAME orelse
        SpanName =:= ?BROKER_UNSUBSCRIBE_SPAN_NAME
->
    Desicion =
        decide_by_match_rule(Attributes, Opts) orelse
            decide_by_traceid_ratio(TraceId, SpanName, Opts),
    {
        decide(Desicion),
        with_cluster_id(Opts),
        otel_span:tracestate(otel_tracer:current_span_ctx(Ctx))
    };
%% None Root Span, decide by Parent or Publish Response Tracing Level
should_sample(
    Ctx,
    _TraceId,
    _Links,
    SpanName,
    _SpanKind,
    _Attributes,
    #{msg_trace_level := QoS} = Opts
) ->
    Desicion =
        parent_sampled(otel_tracer:current_span_ctx(Ctx)) andalso
            match_by_span_name(SpanName, QoS),
    {
        decide(Desicion),
        with_cluster_id(Opts),
        otel_span:tracestate(otel_tracer:current_span_ctx(Ctx))
    }.

-compile({inline, [match_by_span_name/2]}).
match_by_span_name(?BROKER_PUBACK_SPAN_NAME, TraceQoS) -> ?QOS_1 =< TraceQoS;
match_by_span_name(?CLIENT_PUBACK_SPAN_NAME, TraceQoS) -> ?QOS_1 =< TraceQoS;
match_by_span_name(?BROKER_PUBREC_SPAN_NAME, TraceQoS) -> ?QOS_1 =< TraceQoS;
match_by_span_name(?CLIENT_PUBREC_SPAN_NAME, TraceQoS) -> ?QOS_1 =< TraceQoS;
match_by_span_name(?BROKER_PUBREL_SPAN_NAME, TraceQoS) -> ?QOS_2 =< TraceQoS;
match_by_span_name(?CLIENT_PUBREL_SPAN_NAME, TraceQoS) -> ?QOS_2 =< TraceQoS;
match_by_span_name(?BROKER_PUBCOMP_SPAN_NAME, TraceQoS) -> ?QOS_2 =< TraceQoS;
match_by_span_name(?CLIENT_PUBCOMP_SPAN_NAME, TraceQoS) -> ?QOS_2 =< TraceQoS;
%% other sub spans, sample by parent, set mask as true
match_by_span_name(_, _) -> true.

decide_by_match_rule(Attributes, _) ->
    by_clientid(Attributes) orelse
        by_message_from(Attributes) orelse
        %% FIXME: external topic filters for AUTHZ
        by_topic(Attributes).

decide_by_traceid_ratio(_, _, #{id_upper := ?MAX_VALUE}) ->
    true;
decide_by_traceid_ratio(TraceId, SpanName, #{id_upper := IdUpperBound} = Opts) ->
    case maps:get(span_name_to_config_key(SpanName), Opts, false) of
        true ->
            Lower64Bits = TraceId band ?MAX_VALUE,
            Lower64Bits =< IdUpperBound;
        _ ->
            %% not configured, always dropped.
            false
    end.

span_name_to_config_key(SpanName) when
    SpanName =:= ?CLIENT_CONNECT_SPAN_NAME orelse
        SpanName =:= ?CLIENT_DISCONNECT_SPAN_NAME orelse
        SpanName =:= ?BROKER_DISCONNECT_SPAN_NAME
->
    client_connect_disconnect;
span_name_to_config_key(SpanName) when
    SpanName =:= ?CLIENT_SUBSCRIBE_SPAN_NAME orelse
        SpanName =:= ?CLIENT_UNSUBSCRIBE_SPAN_NAME orelse
        SpanName =:= ?BROKER_SUBSCRIBE_SPAN_NAME orelse
        SpanName =:= ?BROKER_UNSUBSCRIBE_SPAN_NAME
->
    client_subscribe_unsubscribe;
span_name_to_config_key(?CLIENT_PUBLISH_SPAN_NAME) ->
    client_messaging.

by_clientid(#{'client.clientid' := ClientId}) ->
    read_should_sample({?EMQX_OTEL_SAMPLE_CLIENTID, ClientId});
by_clientid(_) ->
    false.

by_message_from(#{'message.from' := ClientId}) ->
    read_should_sample({?EMQX_OTEL_SAMPLE_CLIENTID, ClientId});
by_message_from(_) ->
    false.

-dialyzer({nowarn_function, by_topic/1}).
by_topic(#{'message.topic' := Topic}) ->
    case
        mnesia:dirty_match_object(#?EMQX_OTEL_SAMPLER{
            type = {?EMQX_OTEL_SAMPLE_TOPIC, '_'},
            _ = '_'
        })
    of
        [] ->
            false;
        Rules ->
            lists:any(
                fun(Rule) -> match_topic_filter(Topic, Rule) end, Rules
            )
    end;
by_topic(_) ->
    false.

read_should_sample(Key) ->
    case mnesia:dirty_read(?EMQX_OTEL_SAMPLER, Key) of
        [] -> false;
        [#?EMQX_OTEL_SAMPLER{}] -> true
    end.

-dialyzer({nowarn_function, match_topic_filter/2}).
match_topic_filter(AttrTopic, #?EMQX_OTEL_SAMPLER{type = {?EMQX_OTEL_SAMPLE_TOPIC, Topic}}) ->
    emqx_topic:match(AttrTopic, Topic).

-compile({inline, [parent_sampled/1]}).
parent_sampled(#span_ctx{trace_flags = TraceFlags}) when
    ?IS_SAMPLED(TraceFlags)
->
    true;
parent_sampled(_) ->
    false.

-compile({inline, [decide/1]}).
decide(true) ->
    ?RECORD_AND_SAMPLE;
decide(false) ->
    ?DROP.

with_cluster_id(#{cluster_identifier := IdValue}) ->
    #{?CLUSTER_ID_KEY => IdValue};
with_cluster_id(_) ->
    #{}.
