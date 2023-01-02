%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_pubsub_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("gen_coap/include/coap.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(LOGT(Format, Args), ct:pal(Format, Args)).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_coap], fun set_special_cfg/1),
    Config.

set_special_cfg(emqx_coap) ->
    application:set_env(emqx_coap, enable_stats, true);
set_special_cfg(_) ->
    ok.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_coap]),
    Config.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_update_max_age(_Config) ->
    TopicInPayload = <<"topic1">>,
    Payload = <<"<topic1>;ct=42">>,
    Payload1 = <<"<topic1>;ct=50">>,
    URI = "coap://127.0.0.1/ps/"++"?c=client1&u=tom&p=secret",
    URI2 = "coap://127.0.0.1/ps/topic1"++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, URI, #coap_content{format = <<"application/link-format">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    TopicInfo = [{TopicInPayload, MaxAge1, CT1, _ResPayload1, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(TopicInPayload),
    ?LOGT("lookup topic info=~p", [TopicInfo]),
    ?assertEqual(60, MaxAge1),
    ?assertEqual(<<"42">>, CT1),

    timer:sleep(50),

    %% post to create the same topic but with different max age and ct value in payload
    Reply1 = er_coap_client:request(post, URI, #coap_content{max_age = 70, format = <<"application/link-format">>, payload = Payload1}),
    {ok,created, #coap_content{location_path = LocPath}} = Reply1,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    [{TopicInPayload, MaxAge2, CT2, _ResPayload2, _TimeStamp1}] = emqx_coap_pubsub_topics:lookup_topic_info(TopicInPayload),
    ?assertEqual(70, MaxAge2),
    ?assertEqual(<<"50">>, CT2),

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI2).

t_create_subtopic(_Config) ->
    TopicInPayload = <<"topic1">>,
    TopicInPayloadStr = "topic1",
    Payload = <<"<topic1>;ct=42">>,
    URI = "coap://127.0.0.1/ps/"++"?c=client1&u=tom&p=secret",
    RealURI = "coap://127.0.0.1/ps/topic1"++"?c=client1&u=tom&p=secret",

    Reply = er_coap_client:request(post, URI, #coap_content{format = <<"application/link-format">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    TopicInfo = [{TopicInPayload, MaxAge1, CT1, _ResPayload1, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(TopicInPayload),
    ?LOGT("lookup topic info=~p", [TopicInfo]),
    ?assertEqual(60, MaxAge1),
    ?assertEqual(<<"42">>, CT1),

    timer:sleep(50),

    %% post to create the a sub topic
    SubPayload = <<"<subtopic>;ct=42">>,
    SubTopicInPayloadStr = "subtopic",
    SubURI = "coap://127.0.0.1/ps/"++TopicInPayloadStr++"?c=client1&u=tom&p=secret",
    SubRealURI = "coap://127.0.0.1/ps/"++TopicInPayloadStr++"/"++SubTopicInPayloadStr++"?c=client1&u=tom&p=secret",
    FullTopic = list_to_binary(TopicInPayloadStr++"/"++SubTopicInPayloadStr),
    Reply1 = er_coap_client:request(post, SubURI, #coap_content{format = <<"application/link-format">>, payload = SubPayload}),
    ?LOGT("Reply =~p", [Reply1]),
    {ok,created, #coap_content{location_path = LocPath1}} = Reply1,
    ?assertEqual([<<"/ps/topic1/subtopic">>] ,LocPath1),
    [{FullTopic, MaxAge2, CT2, _ResPayload2, _}] = emqx_coap_pubsub_topics:lookup_topic_info(FullTopic),
    ?assertEqual(60, MaxAge2),
    ?assertEqual(<<"42">>, CT2),

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, SubRealURI),
    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, RealURI).

t_over_max_age(_Config) ->
    TopicInPayload = <<"topic1">>,
    Payload = <<"<topic1>;ct=42">>,
    URI = "coap://127.0.0.1/ps/"++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, URI, #coap_content{max_age = 2, format = <<"application/link-format">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    TopicInfo = [{TopicInPayload, MaxAge1, CT1, _ResPayload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(TopicInPayload),
    ?LOGT("lookup topic info=~p", [TopicInfo]),
    ?assertEqual(2, MaxAge1),
    ?assertEqual(<<"42">>, CT1),

    timer:sleep(3000),
    ?assertEqual(true, emqx_coap_pubsub_topics:is_topic_timeout(TopicInPayload)).

t_refreash_max_age(_Config) ->
    TopicInPayload = <<"topic1">>,
    Payload = <<"<topic1>;ct=42">>,
    Payload1 = <<"<topic1>;ct=50">>,
    URI = "coap://127.0.0.1/ps/"++"?c=client1&u=tom&p=secret",
    RealURI = "coap://127.0.0.1/ps/topic1"++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, URI, #coap_content{max_age = 5, format = <<"application/link-format">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    TopicInfo = [{TopicInPayload, MaxAge1, CT1, _ResPayload1, TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(TopicInPayload),
    ?LOGT("lookup topic info=~p", [TopicInfo]),
    ?LOGT("TimeStamp=~p", [TimeStamp]),
    ?assertEqual(5, MaxAge1),
    ?assertEqual(<<"42">>, CT1),

    timer:sleep(3000),

    %% post to create the same topic, the max age timer will be restarted with the new max age value
    Reply1 = er_coap_client:request(post, URI, #coap_content{max_age = 5, format = <<"application/link-format">>, payload = Payload1}),
    {ok,created, #coap_content{location_path = LocPath}} = Reply1,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    [{TopicInPayload, MaxAge2, CT2, _ResPayload2, TimeStamp1}] = emqx_coap_pubsub_topics:lookup_topic_info(TopicInPayload),
    ?LOGT("TimeStamp1=~p", [TimeStamp1]),
    ?assertEqual(5, MaxAge2),
    ?assertEqual(<<"50">>, CT2),

    timer:sleep(3000),
    ?assertEqual(false, emqx_coap_pubsub_topics:is_topic_timeout(TopicInPayload)),

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, RealURI).

t_case01_publish_post(_Config) ->
    timer:sleep(100),
    MainTopic = <<"maintopic">>,
    TopicInPayload = <<"topic1">>,
    Payload = <<"<topic1>;ct=42">>,
    MainTopicStr = binary_to_list(MainTopic),

    %% post to create topic maintopic/topic1
    URI1 = "coap://127.0.0.1/ps/"++MainTopicStr++"?c=client1&u=tom&p=secret",
    FullTopic = list_to_binary(MainTopicStr++"/"++binary_to_list(TopicInPayload)),
    Reply1 = er_coap_client:request(post, URI1, #coap_content{format = <<"application/link-format">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply1]),
    {ok,created, #coap_content{location_path = LocPath1}} = Reply1,
    ?assertEqual([<<"/ps/maintopic/topic1">>] ,LocPath1),
    [{FullTopic, MaxAge, CT2, <<>>, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(FullTopic),
    ?assertEqual(60, MaxAge),
    ?assertEqual(<<"42">>, CT2),

    %% post to publish message to topic maintopic/topic1
    FullTopicStr = emqx_http_lib:uri_encode(binary_to_list(FullTopic)),
    URI2 = "coap://127.0.0.1/ps/"++FullTopicStr++"?c=client1&u=tom&p=secret",
    PubPayload = <<"PUBLISH">>,

    %% Sub topic first
    emqx:subscribe(FullTopic),

    Reply2 = er_coap_client:request(post, URI2, #coap_content{format = <<"application/octet-stream">>, payload = PubPayload}),
    ?LOGT("Reply =~p", [Reply2]),
    {ok,changed, _} = Reply2,
    TopicInfo = [{FullTopic, MaxAge, CT2, PubPayload, _TimeStamp1}] = emqx_coap_pubsub_topics:lookup_topic_info(FullTopic),
    ?LOGT("the topic info =~p", [TopicInfo]),

    assert_recv(FullTopic, PubPayload),
    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI2).

t_case02_publish_post(_Config) ->
    Topic = <<"topic1">>,
    TopicStr = binary_to_list(Topic),
    Payload = <<"payload">>,

    %% Sub topic first
    emqx:subscribe(Topic),

    %% post to publish a new topic "topic1", and the topic is created
    URI = "coap://127.0.0.1/ps/"++TopicStr++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, URI, #coap_content{format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    [{Topic, MaxAge, CT, Payload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?assertEqual(60, MaxAge),
    ?assertEqual(<<"42">>, CT),

    assert_recv(Topic, Payload),

    %% post to publish a new message to the same topic "topic1" with different payload
    NewPayload = <<"newpayload">>,
    Reply1 = er_coap_client:request(post, URI, #coap_content{format = <<"application/octet-stream">>, payload = NewPayload}),
    ?LOGT("Reply =~p", [Reply1]),
    {ok,changed, _} = Reply1,
    [{Topic, MaxAge, CT, NewPayload, _TimeStamp1}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),

    assert_recv(Topic, NewPayload),
    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI).

t_case03_publish_post(_Config) ->
    Topic = <<"topic1">>,
    TopicStr = binary_to_list(Topic),
    Payload = <<"payload">>,

    %% Sub topic first
    emqx:subscribe(Topic),

    %% post to publish a new topic "topic1", and the topic is created
    URI = "coap://127.0.0.1/ps/"++TopicStr++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, URI, #coap_content{format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    [{Topic, MaxAge, CT, Payload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?assertEqual(60, MaxAge),
    ?assertEqual(<<"42">>, CT),

    assert_recv(Topic, Payload),

    %% post to publish a new message to the same topic "topic1", but the ct is not same as created
    NewPayload = <<"newpayload">>,
    Reply1 = er_coap_client:request(post, URI, #coap_content{format = <<"application/exi">>, payload = NewPayload}),
    ?LOGT("Reply =~p", [Reply1]),
    ?assertEqual({error,bad_request}, Reply1),

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI).

t_case04_publish_post(_Config) ->
    Topic = <<"topic1">>,
    TopicStr = binary_to_list(Topic),
    Payload = <<"payload">>,

    %% post to publish a new topic "topic1", and the topic is created
    URI = "coap://127.0.0.1/ps/"++TopicStr++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, URI, #coap_content{max_age = 5, format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    [{Topic, MaxAge, CT, Payload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?assertEqual(5, MaxAge),
    ?assertEqual(<<"42">>, CT),

    %% after max age timeout, the topic still exists but the status is timeout
    timer:sleep(6000),
    ?assertEqual(true, emqx_coap_pubsub_topics:is_topic_timeout(Topic)),

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI).

t_case01_publish_put(_Config) ->
    MainTopic = <<"maintopic">>,
    TopicInPayload = <<"topic1">>,
    Payload = <<"<topic1>;ct=42">>,
    MainTopicStr = binary_to_list(MainTopic),

    %% post to create topic maintopic/topic1
    URI1 = "coap://127.0.0.1/ps/"++MainTopicStr++"?c=client1&u=tom&p=secret",
    FullTopic = list_to_binary(MainTopicStr++"/"++binary_to_list(TopicInPayload)),
    Reply1 = er_coap_client:request(post, URI1, #coap_content{format = <<"application/link-format">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply1]),
    {ok,created, #coap_content{location_path = LocPath1}} = Reply1,
    ?assertEqual([<<"/ps/maintopic/topic1">>] ,LocPath1),
    [{FullTopic, MaxAge, CT2, <<>>, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(FullTopic),
    ?assertEqual(60, MaxAge),
    ?assertEqual(<<"42">>, CT2),

    %% put to publish message to topic maintopic/topic1
    FullTopicStr = emqx_http_lib:uri_encode(binary_to_list(FullTopic)),
    URI2 = "coap://127.0.0.1/ps/"++FullTopicStr++"?c=client1&u=tom&p=secret",
    PubPayload = <<"PUBLISH">>,

    %% Sub topic first
    emqx:subscribe(FullTopic),

    Reply2 = er_coap_client:request(put, URI2, #coap_content{format = <<"application/octet-stream">>, payload = PubPayload}),
    ?LOGT("Reply =~p", [Reply2]),
    {ok,changed, _} = Reply2,
    [{FullTopic, MaxAge, CT2, PubPayload, _TimeStamp1}] = emqx_coap_pubsub_topics:lookup_topic_info(FullTopic),

    assert_recv(FullTopic, PubPayload),

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI2).

t_case02_publish_put(_Config) ->
    Topic = <<"topic1">>,
    TopicStr = binary_to_list(Topic),
    Payload = <<"payload">>,

    %% Sub topic first
    emqx:subscribe(Topic),

    %% put to publish a new topic "topic1", and the topic is created
    URI = "coap://127.0.0.1/ps/"++TopicStr++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(put, URI, #coap_content{format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    [{Topic, MaxAge, CT, Payload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?assertEqual(60, MaxAge),
    ?assertEqual(<<"42">>, CT),

    assert_recv(Topic, Payload),

    %% put to publish a new message to the same topic "topic1" with different payload
    NewPayload = <<"newpayload">>,
    Reply1 = er_coap_client:request(put, URI, #coap_content{format = <<"application/octet-stream">>, payload = NewPayload}),
    ?LOGT("Reply =~p", [Reply1]),
    {ok,changed, _} = Reply1,
    [{Topic, MaxAge, CT, NewPayload, _TimeStamp1}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),

    assert_recv(Topic, NewPayload),

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI).

t_case03_publish_put(_Config) ->
    Topic = <<"topic1">>,
    TopicStr = binary_to_list(Topic),
    Payload = <<"payload">>,

    %% Sub topic first
    emqx:subscribe(Topic),

    %% put to publish a new topic "topic1", and the topic is created
    URI = "coap://127.0.0.1/ps/"++TopicStr++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(put, URI, #coap_content{format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    [{Topic, MaxAge, CT, Payload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?assertEqual(60, MaxAge),
    ?assertEqual(<<"42">>, CT),

    assert_recv(Topic, Payload),

    %% put to publish a new message to the same topic "topic1", but the ct is not same as created
    NewPayload = <<"newpayload">>,
    Reply1 = er_coap_client:request(put, URI, #coap_content{format = <<"application/exi">>, payload = NewPayload}),
    ?LOGT("Reply =~p", [Reply1]),
    ?assertEqual({error,bad_request}, Reply1),

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI).

t_case04_publish_put(_Config) ->
    Topic = <<"topic1">>,
    TopicStr = binary_to_list(Topic),
    Payload = <<"payload">>,

    %% put to publish a new topic "topic1", and the topic is created
    URI = "coap://127.0.0.1/ps/"++TopicStr++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(put, URI, #coap_content{max_age = 5, format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/topic1">>] ,LocPath),
    [{Topic, MaxAge, CT, Payload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?assertEqual(5, MaxAge),
    ?assertEqual(<<"42">>, CT),

    %% after max age timeout, no publish message to the same topic, the topic info will be deleted
    %%%%%%%%%%%%%%%%%%%%%%%%%%
    % but there is one thing to do is we don't count in the publish message received from emqx(from other node).TBD!!!!!!!!!!!!!
    %%%%%%%%%%%%%%%%%%%%%%%%%%
    timer:sleep(6000),
    ?assertEqual(true, emqx_coap_pubsub_topics:is_topic_timeout(Topic)),

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI).

t_case01_subscribe(_Config) ->
    Topic = <<"topic1">>,
    Payload1 = <<"<topic1>;ct=42">>,
    timer:sleep(100),

    %% First post to create a topic "topic1"
    Uri = "coap://127.0.0.1/ps/"++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, Uri, #coap_content{format = <<"application/link-format">>, payload = Payload1}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = [LocPath]}} = Reply,
    ?assertEqual(<<"/ps/topic1">> ,LocPath),
    TopicInfo = [{Topic, MaxAge1, CT1, _ResPayload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?LOGT("lookup topic info=~p", [TopicInfo]),
    ?assertEqual(60, MaxAge1),
    ?assertEqual(<<"42">>, CT1),

    %% Subscribe the topic
    Uri1 = "coap://127.0.0.1"++binary_to_list(LocPath)++"?c=client1&u=tom&p=secret",
    {ok, Pid, N, Code, Content} = er_coap_observer:observe(Uri1),
    ?LOGT("observer Pid=~p, N=~p, Code=~p, Content=~p", [Pid, N, Code, Content]),

    [SubPid] = emqx:subscribers(Topic),
    ?assert(is_pid(SubPid)),

    %% Publish a message
    Payload = <<"123">>,
    emqx:publish(emqx_message:make(Topic, Payload)),

    Notif = receive_notification(),
    ?LOGT("observer get Notif=~p", [Notif]),
    {coap_notify, _, _, {ok,content}, #coap_content{payload = PayloadRecv}} = Notif,

    ?assertEqual(Payload, PayloadRecv),

    %% GET to read the publish message of the topic
    Reply1 = er_coap_client:request(get, Uri1),
    ?LOGT("Reply=~p", [Reply1]),
    {ok,content, #coap_content{payload = <<"123">>}} = Reply1,

    er_coap_observer:stop(Pid),
    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, Uri1).

t_case02_subscribe(_Config) ->
    Topic = <<"a/b">>,
    TopicStr = binary_to_list(Topic),
    PercentEncodedTopic = emqx_http_lib:uri_encode(TopicStr),
    Payload = <<"payload">>,

    %% post to publish a new topic "a/b", and the topic is created
    URI = "coap://127.0.0.1/ps/"++PercentEncodedTopic++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, URI, #coap_content{max_age = 5, format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/a/b">>] ,LocPath),
    [{Topic, MaxAge, CT, Payload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?assertEqual(5, MaxAge),
    ?assertEqual(<<"42">>, CT),

    %% Wait for the max age of the timer expires
    timer:sleep(6000),
    ?assertEqual(true, emqx_coap_pubsub_topics:is_topic_timeout(Topic)),

    %% Subscribe to the timeout topic "a/b", still successfully，got {ok, nocontent} Method
    Uri = "coap://127.0.0.1/ps/"++PercentEncodedTopic++"?c=client1&u=tom&p=secret",
    Reply1 = {ok, Pid, _N, nocontent, _} = er_coap_observer:observe(Uri),
    ?LOGT("Subscribe Reply=~p", [Reply1]), 

    [SubPid] = emqx:subscribers(Topic),
    ?assert(is_pid(SubPid)),

    %% put to publish to topic "a/b"
    Reply2 = er_coap_client:request(put, URI, #coap_content{format = <<"application/octet-stream">>, payload = Payload}),
    {ok,changed, #coap_content{}} = Reply2,
    [{Topic, MaxAge1, CT, Payload, TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?assertEqual(60, MaxAge1),
    ?assertEqual(<<"42">>, CT),
    ?assertEqual(false, TimeStamp =:= timeout),

    %% Publish a message
    emqx:publish(emqx_message:make(Topic, Payload)),

    Notif = receive_notification(),
    ?LOGT("observer get Notif=~p", [Notif]),
    {coap_notify, _, _, {ok,content}, #coap_content{payload = Payload}} = Notif,

    er_coap_observer:stop(Pid),
    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI).

t_case03_subscribe(_Config) ->
    %% Subscribe to the unexisted topic "a/b", got not_found
    Topic = <<"a/b">>,
    TopicStr = binary_to_list(Topic),
    PercentEncodedTopic = emqx_http_lib:uri_encode(TopicStr),
    Uri = "coap://127.0.0.1/ps/"++PercentEncodedTopic++"?c=client1&u=tom&p=secret",
    {error, not_found} = er_coap_observer:observe(Uri),

    [] = emqx:subscribers(Topic).

t_case04_subscribe(_Config) ->
    %% Subscribe to the wildcad topic "+/b", got bad_request
    Topic = <<"+/b">>,
    TopicStr = binary_to_list(Topic),
    PercentEncodedTopic = emqx_http_lib:uri_encode(TopicStr),
    Uri = "coap://127.0.0.1/ps/"++PercentEncodedTopic++"?c=client1&u=tom&p=secret",
    {error, bad_request} = er_coap_observer:observe(Uri),

    [] = emqx:subscribers(Topic).

t_case01_read(_Config) ->
    Topic = <<"topic1">>,
    TopicStr = binary_to_list(Topic),
    Payload = <<"PubPayload">>,
    timer:sleep(100),

    %% First post to create a topic "topic1"
    Uri = "coap://127.0.0.1/ps/"++TopicStr++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, Uri, #coap_content{format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = [LocPath]}} = Reply,
    ?assertEqual(<<"/ps/topic1">> ,LocPath),
    TopicInfo = [{Topic, MaxAge1, CT1, _ResPayload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?LOGT("lookup topic info=~p", [TopicInfo]),
    ?assertEqual(60, MaxAge1),
    ?assertEqual(<<"42">>, CT1),

    %% GET to read the publish message of the topic
    timer:sleep(1000),
    Reply1 = er_coap_client:request(get, Uri),
    ?LOGT("Reply=~p", [Reply1]),
    {ok,content, #coap_content{payload = Payload}} = Reply1,

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, Uri).

t_case02_read(_Config) ->
    Topic = <<"topic1">>,
    TopicStr = binary_to_list(Topic),
    Payload = <<"PubPayload">>,
    timer:sleep(100),

    %% First post to publish a topic "topic1"
    Uri = "coap://127.0.0.1/ps/"++TopicStr++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, Uri, #coap_content{format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = [LocPath]}} = Reply,
    ?assertEqual(<<"/ps/topic1">> ,LocPath),
    TopicInfo = [{Topic, MaxAge1, CT1, _ResPayload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?LOGT("lookup topic info=~p", [TopicInfo]),
    ?assertEqual(60, MaxAge1),
    ?assertEqual(<<"42">>, CT1),

    %% GET to read the publish message of unmatched format, got bad_request
    Reply1 = er_coap_client:request(get, Uri, #coap_content{format = <<"application/json">>}),
    ?LOGT("Reply=~p", [Reply1]),
    {error, bad_request} = Reply1,

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, Uri).

t_case03_read(_Config) ->
    Topic = <<"topic1">>,
    TopicStr = binary_to_list(Topic),
    Uri = "coap://127.0.0.1/ps/"++TopicStr++"?c=client1&u=tom&p=secret",
    timer:sleep(100),

    %% GET to read the nexisted topic "topic1", got not_found
    Reply = er_coap_client:request(get, Uri),
    ?LOGT("Reply=~p", [Reply]),
    {error, not_found} = Reply.

t_case04_read(_Config) ->
    Topic = <<"topic1">>,
    TopicStr = binary_to_list(Topic),
    Payload = <<"PubPayload">>,
    timer:sleep(100),

    %% First post to publish a topic "topic1"
    Uri = "coap://127.0.0.1/ps/"++TopicStr++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, Uri, #coap_content{format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = [LocPath]}} = Reply,
    ?assertEqual(<<"/ps/topic1">> ,LocPath),
    TopicInfo = [{Topic, MaxAge1, CT1, _ResPayload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?LOGT("lookup topic info=~p", [TopicInfo]),
    ?assertEqual(60, MaxAge1),
    ?assertEqual(<<"42">>, CT1),

    %% GET to read the publish message of wildcard topic, got bad_request
    WildTopic = binary_to_list(<<"+/topic1">>),
    Uri1 = "coap://127.0.0.1/ps/"++WildTopic++"?c=client1&u=tom&p=secret",
    Reply1 = er_coap_client:request(get, Uri1, #coap_content{format = <<"application/json">>}),
    ?LOGT("Reply=~p", [Reply1]),
    {error, bad_request} = Reply1,

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, Uri).

t_case05_read(_Config) ->
    Topic = <<"a/b">>,
    TopicStr = binary_to_list(Topic),
    PercentEncodedTopic = emqx_http_lib:uri_encode(TopicStr),
    Payload = <<"payload">>,

    %% post to publish a new topic "a/b", and the topic is created
    URI = "coap://127.0.0.1/ps/"++PercentEncodedTopic++"?c=client1&u=tom&p=secret",
    Reply = er_coap_client:request(post, URI, #coap_content{max_age = 5, format = <<"application/octet-stream">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/a/b">>] ,LocPath),
    [{Topic, MaxAge, CT, Payload, _TimeStamp}] = emqx_coap_pubsub_topics:lookup_topic_info(Topic),
    ?assertEqual(5, MaxAge),
    ?assertEqual(<<"42">>, CT),

    %% Wait for the max age of the timer expires
    timer:sleep(6000),
    ?assertEqual(true, emqx_coap_pubsub_topics:is_topic_timeout(Topic)),

    %% GET to read the expired publish message, supposed to get {ok, nocontent}, but now got {ok, content}
    Reply1 = er_coap_client:request(get, URI),
    ?LOGT("Reply=~p", [Reply1]),
    {ok, content, #coap_content{payload = <<>>}}= Reply1,

    {ok, deleted, #coap_content{}} = er_coap_client:request(delete, URI).

t_case01_delete(_Config) ->
    TopicInPayload = <<"a/b">>,
    TopicStr = binary_to_list(TopicInPayload),
    PercentEncodedTopic = emqx_http_lib:uri_encode(TopicStr),
    Payload = list_to_binary("<"++PercentEncodedTopic++">;ct=42"),
    URI = "coap://127.0.0.1/ps/"++"?c=client1&u=tom&p=secret",

    %% Client post to CREATE topic "a/b"
    Reply = er_coap_client:request(post, URI, #coap_content{format = <<"application/link-format">>, payload = Payload}),
    ?LOGT("Reply =~p", [Reply]),
    {ok,created, #coap_content{location_path = LocPath}} = Reply,
    ?assertEqual([<<"/ps/a/b">>] ,LocPath),

    %% Client post to CREATE topic "a/b/c"
    TopicInPayload1 = <<"a/b/c">>,
    PercentEncodedTopic1 = emqx_http_lib:uri_encode(binary_to_list(TopicInPayload1)),
    Payload1 = list_to_binary("<"++PercentEncodedTopic1++">;ct=42"),
    Reply1 = er_coap_client:request(post, URI, #coap_content{format = <<"application/link-format">>, payload = Payload1}),
    ?LOGT("Reply =~p", [Reply1]),
    {ok,created, #coap_content{location_path = LocPath1}} = Reply1,
    ?assertEqual([<<"/ps/a/b/c">>] ,LocPath1),

    timer:sleep(50),

    %% DELETE the topic "a/b"
    UriD = "coap://127.0.0.1/ps/"++PercentEncodedTopic++"?c=client1&u=tom&p=secret",
    ReplyD = er_coap_client:request(delete, UriD),
    ?LOGT("Reply=~p", [ReplyD]),
    {ok, deleted, #coap_content{}}= ReplyD,

    timer:sleep(300), %% Waiting gen_server:cast/2 for deleting operation
    ?assertEqual(false, emqx_coap_pubsub_topics:is_topic_existed(TopicInPayload)),
    ?assertEqual(false, emqx_coap_pubsub_topics:is_topic_existed(TopicInPayload1)).

t_case02_delete(_Config) ->
    TopicInPayload = <<"a/b">>,
    TopicStr = binary_to_list(TopicInPayload),
    PercentEncodedTopic = emqx_http_lib:uri_encode(TopicStr),

    %% DELETE the unexisted topic "a/b"
    Uri1 = "coap://127.0.0.1/ps/"++PercentEncodedTopic++"?c=client1&u=tom&p=secret",
    Reply1 = er_coap_client:request(delete, Uri1),
    ?LOGT("Reply=~p", [Reply1]),
    {error, not_found} = Reply1.

t_case13_emit_stats_test(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions

receive_notification() ->
    receive
        {coap_notify, Pid, N2, Code2, Content2} ->
            {coap_notify, Pid, N2, Code2, Content2}
    after 2000 ->
        receive_notification_timeout
    end.

assert_recv(Topic, Payload) ->
    receive
        {deliver, _, Msg} ->
            ?assertEqual(Topic, Msg#message.topic),
            ?assertEqual(Payload, Msg#message.payload)
    after
        500 ->
            ?assert(false)
    end.

