%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_publish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("emqx_cluster_link.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    ok = emqx_cluster_link:put_hook(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cluster_link:delete_hook(),
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    ok = meck:new(emqx_cluster_link_config, [passthrough, no_link]),
    ok = emqx_broker:subscribe(?TOPIC_PREFIX_WILDCARD),
    ok = emqx_broker:subscribe(<<"forwarded/topic">>),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_broker:unsubscribe(?TOPIC_PREFIX_WILDCARD),
    ok = emqx_broker:unsubscribe(<<"forwarded/topic">>),
    ok = meck:unload(emqx_cluster_link_config).

%% Check that route-control messages are consumed without subscriber delivery.
t_route_control(_Config) ->
    Payload = term_to_binary(#{'$op' => <<"heartbeat">>, 10 => test_actor, 11 => 1}),
    Msg = emqx_message:make(<<"remote">>, 1, ?ROUTE_TOPIC(<<"remote">>), Payload),
    assert_consumed(Msg).

%% Check that invalid route-control messages return a disconnect error without delivery.
t_invalid_route_control(_Config) ->
    %% Build an invalid route operation and its expected hook-modified message.
    ok = meck:expect(emqx_cluster_link_config, cluster, fun() -> <<"local">> end),
    Msg = emqx_message:make(
        <<"remote">>, 1, ?ROUTE_TOPIC(<<"remote">>), term_to_binary(invalid_route_operation)
    ),
    Expected = emqx_message:set_headers(
        #{allow_publish => false, should_disconnect => true}, emqx_message:clean_dup(Msg)
    ),
    ?assertEqual({error, disconnect, Expected}, emqx_broker:publish(Msg)),
    %% Check that disconnect takes precedence over the blocked-message option.
    ?assertEqual(
        {error, disconnect, Expected},
        emqx_broker:publish(Msg, #{hook_prohibition_as_error => true})
    ),
    ?assertNotReceive({deliver, _, _}).

%% Check that invalid forwarded payloads are consumed without subscriber delivery.
t_invalid_forwarded_message(_Config) ->
    Msg = emqx_message:make(
        <<"remote">>, 1, ?MSG_FWD_TOPIC(<<"remote">>), term_to_binary(invalid_message)
    ),
    assert_consumed(Msg).

%% Check that disabled links and nonmatching topic filters suppress forwarded messages.
t_filtered_forwarded_message(_Config) ->
    lists:foreach(
        fun(LinkConfig) ->
            ok = meck:expect(emqx_cluster_link_config, link, fun(<<"remote">>) -> LinkConfig end),
            assert_consumed(transport_message())
        end,
        [
            #{enable => true, topics => [<<"other/#">>]},
            #{enable => false, topics => [<<"forwarded/#">>]}
        ]
    ).

%% Check that an accepted forwarded message is decoded, returned, and delivered exactly once.
t_accepted_forwarded_message(_Config) ->
    %% Enable the link for the forwarded topic.
    ok = meck:expect(emqx_cluster_link_config, link, fun(<<"remote">>) ->
        #{enable => true, topics => [<<"forwarded/#">>]}
    end),
    %% Publish the transport message and check the decoded result.
    {ok, [{_, <<"forwarded/topic">>, _}], PublishedMsg} =
        emqx_broker:publish(transport_message()),
    ?assertMatch(
        #message{
            topic = <<"forwarded/topic">>,
            payload = <<"payload">>,
            extra = #{link_origin := <<"remote">>}
        },
        PublishedMsg
    ),
    %% Check that only the decoded message reaches the subscriber.
    ?assertReceive({deliver, <<"forwarded/topic">>, PublishedMsg}),
    ?assertNotReceive({deliver, _, _}).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

transport_message() ->
    ForwardedMsg = emqx_message:make(<<"publisher">>, 1, <<"forwarded/topic">>, <<"payload">>),
    emqx_message:make(<<"remote">>, 1, ?MSG_FWD_TOPIC(<<"remote">>), term_to_binary(ForwardedMsg)).

assert_consumed(Msg) ->
    Expected = emqx_message:set_header(allow_publish, false, emqx_message:clean_dup(Msg)),
    ?assertEqual({ok, [], Expected}, emqx_broker:publish(Msg)),
    ?assertEqual({ok, [], Expected}, emqx_broker:publish(Msg, #{})),
    ?assertEqual({ok, [], Expected}, emqx:publish(Msg)),
    ?assertEqual({ok, [], Expected}, emqx_broker:safe_publish(Msg)),
    ?assertEqual(
        {error, blocked, Expected},
        emqx_broker:publish(Msg, #{hook_prohibition_as_error => true})
    ),
    ?assertEqual(
        {error, blocked, Expected},
        emqx_broker:safe_publish(Msg, #{hook_prohibition_as_error => true})
    ),
    ?assertNotReceive({deliver, _, _}).
