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
%% -include_lib("opentelemetry_api/include/opentelemetry.hrl").

-define(EMQX_OTEL_SAMPLER_RULE, emqx_otel_sampler_rule).

-define(EMQX_OTEL_SAMPLE_CLIENTID, 1).
-define(EMQX_OTEL_SAMPLE_USERNAME, 2).
-define(EMQX_OTEL_SAMPLE_TOPICNAME, 3).

-record(?EMQX_OTEL_SAMPLER_RULE, {
    type ::
        {?EMQX_OTEL_SAMPLE_CLIENTID, binary()}
        | {?EMQX_OTEL_SAMPLE_USERNAME, binary()}
        | {?EMQX_OTEL_SAMPLE_TOPICNAME, binary()},
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

-define(QOS1_RESPONSES_SPANS_PUBACK, [
    ?BROKER_PUBACK_SPAN_NAME,
    ?CLIENT_PUBACK_SPAN_NAME
]).

%% First response in QoS2
-define(QOS2_RESPONSES_SPANS_PUBREC, [
    ?BROKER_PUBREC_SPAN_NAME,
    ?CLIENT_PUBREC_SPAN_NAME
]).

%% Rest responses in QoS2
-define(QOS2_RESPONSES_SPANS_PUBREL, [
    ?BROKER_PUBREL_SPAN_NAME,
    ?CLIENT_PUBREL_SPAN_NAME
]).

-define(QOS2_RESPONSES_SPANS_PUBCOMP, [
    ?BROKER_PUBCOMP_SPAN_NAME,
    ?CLIENT_PUBCOMP_SPAN_NAME
]).

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
store_rules({topicname, TopicName}, ShouldSample, Extra) ->
    Record = #?EMQX_OTEL_SAMPLER_RULE{
        type = {?EMQX_OTEL_SAMPLE_TOPICNAME, TopicName},
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
get_rules({topicname, TopicName}) ->
    do_get_rules({?EMQX_OTEL_SAMPLE_TOPICNAME, TopicName}).

do_get_rules(Key) ->
    case mnesia:dirty_read(?EMQX_OTEL_SAMPLER_RULE, Key) of
        [#?EMQX_OTEL_SAMPLER_RULE{should_sample = ShouldSample}] -> {ok, ShouldSample};
        [] -> not_found
    end.

delete_rules({clientid, ClientId}) ->
    mria:dirty_delete(?EMQX_OTEL_SAMPLER_RULE, {?EMQX_OTEL_SAMPLE_CLIENTID, ClientId});
delete_rules({username, Username}) ->
    mria:dirty_delete(?EMQX_OTEL_SAMPLER_RULE, {?EMQX_OTEL_SAMPLE_USERNAME, Username});
delete_rules({topicname, TopicName}) ->
    mria:dirty_delete(?EMQX_OTEL_SAMPLER_RULE, {?EMQX_OTEL_SAMPLE_TOPICNAME, TopicName}).

%%--------------------------------------------------------------------
%% OpenTelemetry Sampler Callback
%%--------------------------------------------------------------------

setup(Opts) ->
    ?SLOG(debug, #{
        msg => "emqx_otel_sampler_setup",
        opts => Opts
    }),
    %% do nothing for now
    Opts.

description(_Opts) ->
    <<"AttributeSampler">>.

should_sample(Ctx, _TraceId, _Links, SpanName, _SpanKind, Attributes, _Opts) ->
    ?SLOG(debug, #{
        msg => "call_emqx_otel_sampler_should_sample",
        span_name => SpanName,
        span_kind => _SpanKind,
        opts => _Opts,
        attributes => Attributes
    }),
    SpanCtx = otel_tracer:current_span_ctx(Ctx),
    case match_sample(SpanName, Attributes) of
        true -> {?RECORD_AND_SAMPLE, [], otel_span:tracestate(SpanCtx)};
        false -> {?DROP, [], otel_span:tracestate(SpanCtx)}
    end.

%% TODO: ignore publish qos1/qos2 responses spans by user configured
match_sample(?BROKER_PUBACK_SPAN_NAME, Attributes) ->
    publish_response_should_sample(?QOS_1) andalso
        match_sample_attrs(Attributes);
match_sample(?CLIENT_PUBACK_SPAN_NAME, Attributes) ->
    publish_response_should_sample(?QOS_1) andalso
        match_sample_attrs(Attributes);
match_sample(?BROKER_PUBREC_SPAN_NAME, Attributes) ->
    publish_response_should_sample(?QOS_2) andalso
        match_sample_attrs(Attributes);
match_sample(?CLIENT_PUBREC_SPAN_NAME, Attributes) ->
    publish_response_should_sample(?QOS_2) andalso
        match_sample_attrs(Attributes);
match_sample(?BROKER_PUBREL_SPAN_NAME, Attributes) ->
    publish_response_should_sample(?QOS_2) andalso
        match_sample_attrs(Attributes);
match_sample(?CLIENT_PUBREL_SPAN_NAME, Attributes) ->
    publish_response_should_sample(?QOS_2) andalso
        match_sample_attrs(Attributes);
match_sample(?BROKER_PUBCOMP_SPAN_NAME, Attributes) ->
    publish_response_should_sample(?QOS_2) andalso
        match_sample_attrs(Attributes);
match_sample(?CLIENT_PUBCOMP_SPAN_NAME, Attributes) ->
    publish_response_should_sample(?QOS_2) andalso
        match_sample_attrs(Attributes);
match_sample(?MSG_ROUTE_SPAN_NAME, _) ->
    true;
match_sample(_, Attributes) ->
    match_sample_attrs(Attributes).

publish_response_should_sample(QoS) ->
    QoS =< persistent_term:get(pub_resp_level, ?QOS_2).

match_sample_attrs(Attributes) ->
    sample_by_clientid(Attributes) orelse
        sample_by_message_from(Attributes) orelse
        sample_by_username(Attributes) orelse
        sample_by_topicname(Attributes).

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

sample_by_topicname(#{'message.topic' := TopicName}) ->
    read_should_sample({?EMQX_OTEL_SAMPLE_TOPICNAME, TopicName});
sample_by_topicname(_) ->
    false.

read_should_sample(Key) ->
    case mnesia:dirty_read(?EMQX_OTEL_SAMPLER_RULE, Key) of
        [] -> false;
        [#?EMQX_OTEL_SAMPLER_RULE{should_sample = ShouldSample}] -> ShouldSample
    end.
