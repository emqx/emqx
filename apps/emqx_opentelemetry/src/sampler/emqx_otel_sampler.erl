%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_otel_sampler).

-behaviour(otel_sampler).

-include("emqx_otel_trace.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("opentelemetry/include/otel_sampler.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-define(EMQX_OTEL_SAMPLER_RULE, emqx_otel_sampler_rule).

-define(EMQX_OTEL_SAMPLE_CLIENTID, 1).
-define(EMQX_OTEL_SAMPLE_USERNAME, 2).
-define(EMQX_OTEL_SAMPLE_TOPIC_NAME, 3).
-define(EMQX_OTEL_SAMPLE_TOPIC_MATCHING, 4).

-record(?EMQX_OTEL_SAMPLER_RULE, {
    type ::
        {?EMQX_OTEL_SAMPLE_CLIENTID, binary()}
        | {?EMQX_OTEL_SAMPLE_USERNAME, binary()}
        | {?EMQX_OTEL_SAMPLE_TOPIC_NAME, binary()}
        | {?EMQX_OTEL_SAMPLE_TOPIC_MATCHING, binary()},
    should_sample :: boolean(),
    extra :: map()
}).

-export([
    init_tables/0,
    store_rules/3,
    purge_rules/0,
    get_rules/1,
    delete_rules/1
]).

%% OpenTelemetry Sampler Callback
-export([setup/1, description/1, should_sample/7]).

%% 2^64 - 1
-define(MAX_VALUE, 18446744073709551615).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

-spec create_tables() -> [mria:table()].
create_tables() ->
    ok = mria:create_table(
        ?EMQX_OTEL_SAMPLER_RULE,
        [
            {type, ordered_set},
            {storage, disc_copies},
            {local_content, true},
            {record_name, ?EMQX_OTEL_SAMPLER_RULE},
            {attributes, record_info(fields, ?EMQX_OTEL_SAMPLER_RULE)}
        ]
    ),
    [?EMQX_OTEL_SAMPLER_RULE].

%% Init
-spec init_tables() -> ok.
init_tables() ->
    ok = mria:wait_for_tables(create_tables()).

%% @doc Update sample rule
%% -spec store_rules(who(), rules()) -> ok.
store_rules({clientid, ClientId}, ShouldSample, Extra) ->
    Record = #?EMQX_OTEL_SAMPLER_RULE{
        type = {?EMQX_OTEL_SAMPLE_CLIENTID, ClientId},
        should_sample = ShouldSample,
        extra = Extra
    },
    mria:dirty_write(Record);
store_rules({username, Username}, ShouldSample, Extra) ->
    Record = #?EMQX_OTEL_SAMPLER_RULE{
        type = {?EMQX_OTEL_SAMPLE_USERNAME, Username},
        should_sample = ShouldSample,
        extra = Extra
    },
    mria:dirty_write(Record);
store_rules({topic_name, TopicName}, ShouldSample, Extra) ->
    Record = #?EMQX_OTEL_SAMPLER_RULE{
        type = {?EMQX_OTEL_SAMPLE_TOPIC_NAME, TopicName},
        should_sample = ShouldSample,
        extra = Extra
    },
    mria:dirty_write(Record);
store_rules({topic_matching, TopicFilter}, ShouldSample, Extra) ->
    Record = #?EMQX_OTEL_SAMPLER_RULE{
        type = {?EMQX_OTEL_SAMPLE_TOPIC_MATCHING, TopicFilter},
        should_sample = ShouldSample,
        extra = Extra
    },
    mria:dirty_write(Record).

-spec purge_rules() -> ok.
purge_rules() ->
    ok = lists:foreach(
        fun(Key) ->
            ok = mria:dirty_delete(?EMQX_OTEL_SAMPLER_RULE, Key)
        end,
        mnesia:dirty_all_keys(?EMQX_OTEL_SAMPLER_RULE)
    ).

get_rules({clientid, ClientId}) ->
    do_get_rules({?EMQX_OTEL_SAMPLE_CLIENTID, ClientId});
get_rules({username, Username}) ->
    do_get_rules({?EMQX_OTEL_SAMPLE_USERNAME, Username});
get_rules({topic_name, TopicName}) ->
    do_get_rules({?EMQX_OTEL_SAMPLE_TOPIC_NAME, TopicName});
get_rules({topic_matching, TopicFilter}) ->
    do_get_rules({?EMQX_OTEL_SAMPLE_TOPIC_MATCHING, TopicFilter}).

do_get_rules(Key) ->
    case mnesia:dirty_read(?EMQX_OTEL_SAMPLER_RULE, Key) of
        [#?EMQX_OTEL_SAMPLER_RULE{should_sample = ShouldSample}] -> {ok, ShouldSample};
        [] -> not_found
    end.

delete_rules({clientid, ClientId}) ->
    mria:dirty_delete(?EMQX_OTEL_SAMPLER_RULE, {?EMQX_OTEL_SAMPLE_CLIENTID, ClientId});
delete_rules({username, Username}) ->
    mria:dirty_delete(?EMQX_OTEL_SAMPLER_RULE, {?EMQX_OTEL_SAMPLE_USERNAME, Username});
delete_rules({topic_name, TopicName}) ->
    mria:dirty_delete(?EMQX_OTEL_SAMPLER_RULE, {?EMQX_OTEL_SAMPLE_TOPIC_NAME, TopicName});
delete_rules({topic_matching, TopicFilter}) ->
    mria:dirty_delete(?EMQX_OTEL_SAMPLER_RULE, {?EMQX_OTEL_SAMPLE_TOPIC_MATCHING, TopicFilter}).

%%--------------------------------------------------------------------
%% OpenTelemetry Sampler Callback
%%--------------------------------------------------------------------

setup(
    #{
        attribute_meta := MetaValue,
        samplers :=
            #{
                event_based_samplers := EventBasedSamplers,
                whitelist_based_sampler := WhiteListEnabled
            },
        publish_response_trace_level := QoS
    } = _Opts
) ->
    EventBasedRatio = lists:foldl(
        %% Name might not appears
        fun(#{name := Name, ratio := Ratio}, AccIn) ->
            case Ratio of
                R when R =:= +0.0 ->
                    AccIn#{Name => #{ratio => 0, id_upper => 0}};
                R when R =:= 1.0 ->
                    AccIn#{Name => #{ratio => 1, id_upper => ?MAX_VALUE}};
                R when R >= 0.0 andalso R =< 1.0 ->
                    IdUpperBound = R * ?MAX_VALUE,
                    AccIn#{Name => #{ratio => R, id_upper => IdUpperBound}}
            end
        end,
        #{},
        EventBasedSamplers
    ),
    Config = #{
        event_based_samplers => EventBasedRatio,
        whitelist_based_sampler => WhiteListEnabled,
        publish_response_trace_level => QoS,
        attribute_meta => MetaValue
    },
    ?SLOG(debug, #{
        msg => "emqx_otel_sampler_setup",
        config => Config
    }),
    Config.

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
    #{
        whitelist_based_sampler := WhiteListEnabled,
        event_based_samplers := EventBasedRatio,
        attribute_meta := MetaValue
    } = _Opts
) when
    SpanName =:= ?CLIENT_CONNECT_SPAN_NAME orelse
        SpanName =:= ?CLIENT_DISCONNECT_SPAN_NAME orelse
        SpanName =:= ?CLIENT_SUBSCRIBE_SPAN_NAME orelse
        SpanName =:= ?CLIENT_UNSUBSCRIBE_SPAN_NAME orelse
        SpanName =:= ?CLIENT_PUBLISH_SPAN_NAME
->
    Desicion =
        decide_by_whitelist(WhiteListEnabled, Attributes) orelse
            decide_by_traceid_ratio(TraceId, SpanName, EventBasedRatio),
    {
        decide(Desicion),
        #{attribute_meta => MetaValue},
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
    #{publish_response_trace_level := QoS, attribute_meta := MetaValue} = _Opts
) ->
    Desicion =
        parent_sampled(otel_tracer:current_span_ctx(Ctx)) andalso
            match_by_span_name(SpanName, QoS),
    {
        decide(Desicion),
        #{attribute_meta => MetaValue},
        otel_span:tracestate(otel_tracer:current_span_ctx(Ctx))
    }.

%% -compile({inline, [match_sample/2]}).
match_by_span_name(?BROKER_PUBACK_SPAN_NAME, L) -> ?QOS_1 =< L;
match_by_span_name(?CLIENT_PUBACK_SPAN_NAME, L) -> ?QOS_1 =< L;
match_by_span_name(?BROKER_PUBREC_SPAN_NAME, L) -> ?QOS_1 =< L;
match_by_span_name(?CLIENT_PUBREC_SPAN_NAME, L) -> ?QOS_1 =< L;
match_by_span_name(?BROKER_PUBREL_SPAN_NAME, L) -> ?QOS_2 =< L;
match_by_span_name(?CLIENT_PUBREL_SPAN_NAME, L) -> ?QOS_2 =< L;
match_by_span_name(?BROKER_PUBCOMP_SPAN_NAME, L) -> ?QOS_2 =< L;
match_by_span_name(?CLIENT_PUBCOMP_SPAN_NAME, L) -> ?QOS_2 =< L;
%% other spans, always sample
match_by_span_name(_, _) -> true.

decide_by_whitelist(true, Attributes) ->
    sample_by_clientid(Attributes) orelse
        sample_by_message_from(Attributes) orelse
        sample_by_username(Attributes) orelse
        sample_by_topic_name(Attributes) orelse
        sample_by_topic_filter(Attributes);
decide_by_whitelist(false, _Attributes) ->
    false.

decide_by_traceid_ratio(TraceId, SpanName, _EventBasedRatio) ->
    case _EventBasedRatio of
        #{SpanName := #{id_upper := IdUpperBound}} ->
            Lower64Bits = TraceId band ?MAX_VALUE,
            %% XXX: really need abs?
            erlang:abs(Lower64Bits) =< IdUpperBound;
        _ ->
            %% not configured, always dropped.
            false
    end.

sample_by_clientid(#{'client.clientid' := ClientId}) ->
    read_should_sample({?EMQX_OTEL_SAMPLE_CLIENTID, ClientId});
sample_by_clientid(_) ->
    false.

sample_by_message_from(#{'message.from' := ClientId}) ->
    read_should_sample({?EMQX_OTEL_SAMPLE_CLIENTID, ClientId});
sample_by_message_from(_) ->
    false.

sample_by_username(#{'client.username' := Username}) ->
    read_should_sample({?EMQX_OTEL_SAMPLE_USERNAME, Username});
sample_by_username(_) ->
    false.

sample_by_topic_name(#{'message.topic' := TopicName}) ->
    read_should_sample({?EMQX_OTEL_SAMPLE_TOPIC_NAME, TopicName});
sample_by_topic_name(_) ->
    false.

sample_by_topic_filter(#{'message.topic' := TopicName}) ->
    case
        mnesia:dirty_match_object(#?EMQX_OTEL_SAMPLER_RULE{
            type = {?EMQX_OTEL_SAMPLE_TOPIC_MATCHING, '_'},
            should_sample = '_',
            extra = '_'
        })
    of
        [] ->
            false;
        Rules ->
            lists:any(
                fun(Rule) -> match_topic_filter(TopicName, Rule) end, Rules
            )
    end;
sample_by_topic_filter(_) ->
    false.

read_should_sample(Key) ->
    case mnesia:dirty_read(?EMQX_OTEL_SAMPLER_RULE, Key) of
        [] -> false;
        [#?EMQX_OTEL_SAMPLER_RULE{should_sample = ShouldSample}] -> ShouldSample
    end.

match_topic_filter(TopicName, #?EMQX_OTEL_SAMPLER_RULE{
    type = {?EMQX_OTEL_SAMPLE_TOPIC_MATCHING, TopicFilter},
    should_sample = ShouldSample
}) ->
    emqx_topic:match(TopicName, TopicFilter) andalso ShouldSample.

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
