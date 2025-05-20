%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_nats_topic_SUITE).

-include_lib("eunit/include/eunit.hrl").

-export([all/0, groups/0]).
-export([
    t_nats_to_mqtt/1,
    t_mqtt_to_nats/1,
    t_validate_nats_subject/1
]).

all() ->
    [
        {group, conversion},
        {group, validation}
    ].

groups() ->
    [
        {conversion, [sequence], [
            t_nats_to_mqtt,
            t_mqtt_to_nats
        ]},
        {validation, [sequence], [
            t_validate_nats_subject
        ]}
    ].

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_nats_to_mqtt(_) ->
    %% Basic conversion
    ?assertEqual(<<"foo/bar/baz">>, emqx_nats_topic:nats_to_mqtt(<<"foo.bar.baz">>)),
    ?assertEqual(<<"foo/bar">>, emqx_nats_topic:nats_to_mqtt(<<"foo.bar">>)),
    ?assertEqual(<<"foo">>, emqx_nats_topic:nats_to_mqtt(<<"foo">>)),

    %% Wildcard conversion
    ?assertEqual(<<"foo/+/bar">>, emqx_nats_topic:nats_to_mqtt(<<"foo.*.bar">>)),
    ?assertEqual(<<"+/bar/baz">>, emqx_nats_topic:nats_to_mqtt(<<"*.bar.baz">>)),
    ?assertEqual(<<"foo/bar/+">>, emqx_nats_topic:nats_to_mqtt(<<"foo.bar.*">>)),
    ?assertEqual(<<"foo/#">>, emqx_nats_topic:nats_to_mqtt(<<"foo.>">>)),
    ?assertEqual(<<"#">>, emqx_nats_topic:nats_to_mqtt(<<">">>)),

    %% Multiple wildcards
    ?assertEqual(<<"+/+/+">>, emqx_nats_topic:nats_to_mqtt(<<"*.*.*">>)),
    ?assertEqual(<<"foo/+/bar/+">>, emqx_nats_topic:nats_to_mqtt(<<"foo.*.bar.*">>)),
    ?assertEqual(<<"foo/+/bar/#">>, emqx_nats_topic:nats_to_mqtt(<<"foo.*.bar.>">>)),

    %% Edge cases
    ?assertEqual(<<>>, emqx_nats_topic:nats_to_mqtt(<<"">>)).

t_mqtt_to_nats(_) ->
    %% Basic conversion
    ?assertEqual(<<"foo.bar.baz">>, emqx_nats_topic:mqtt_to_nats(<<"foo/bar/baz">>)),
    ?assertEqual(<<"foo.bar">>, emqx_nats_topic:mqtt_to_nats(<<"foo/bar">>)),
    ?assertEqual(<<"foo">>, emqx_nats_topic:mqtt_to_nats(<<"foo">>)),

    %% Wildcard conversion
    ?assertEqual(<<"foo.*.bar">>, emqx_nats_topic:mqtt_to_nats(<<"foo/+/bar">>)),
    ?assertEqual(<<"*.bar.baz">>, emqx_nats_topic:mqtt_to_nats(<<"+/bar/baz">>)),
    ?assertEqual(<<"foo.bar.*">>, emqx_nats_topic:mqtt_to_nats(<<"foo/bar/+">>)),
    ?assertEqual(<<"foo.>">>, emqx_nats_topic:mqtt_to_nats(<<"foo/#">>)),
    ?assertEqual(<<">">>, emqx_nats_topic:mqtt_to_nats(<<"#">>)),

    %% Multiple wildcards
    ?assertEqual(<<"*.*.*">>, emqx_nats_topic:mqtt_to_nats(<<"+/+/+">>)),
    ?assertEqual(<<"foo.*.bar.*">>, emqx_nats_topic:mqtt_to_nats(<<"foo/+/bar/+">>)),
    ?assertEqual(<<"foo.*.bar.>">>, emqx_nats_topic:mqtt_to_nats(<<"foo/+/bar/#">>)),

    %% Edge cases
    ?assertEqual(<<>>, emqx_nats_topic:mqtt_to_nats(<<"">>)).

t_validate_nats_subject(_) ->
    %% Valid subjects without wildcards
    ?assertEqual({ok, false}, emqx_nats_topic:validate_nats_subject(<<"foo">>)),
    ?assertEqual({ok, false}, emqx_nats_topic:validate_nats_subject(<<"foo.bar">>)),
    ?assertEqual({ok, false}, emqx_nats_topic:validate_nats_subject(<<"foo.bar.baz">>)),
    ?assertEqual({ok, false}, emqx_nats_topic:validate_nats_subject(<<"foo-bar">>)),
    ?assertEqual({ok, false}, emqx_nats_topic:validate_nats_subject(<<"foo_bar">>)),
    ?assertEqual({ok, false}, emqx_nats_topic:validate_nats_subject(<<"foo@bar">>)),
    ?assertEqual({ok, false}, emqx_nats_topic:validate_nats_subject(<<"中文"/utf8>>)),

    %% Valid subjects with wildcards
    ?assertEqual({ok, true}, emqx_nats_topic:validate_nats_subject(<<"foo.*">>)),
    ?assertEqual({ok, true}, emqx_nats_topic:validate_nats_subject(<<"*.bar">>)),
    ?assertEqual({ok, true}, emqx_nats_topic:validate_nats_subject(<<"foo.*.baz">>)),
    ?assertEqual({ok, true}, emqx_nats_topic:validate_nats_subject(<<"foo.>">>)),
    ?assertEqual({ok, true}, emqx_nats_topic:validate_nats_subject(<<"*.>">>)),
    ?assertEqual({ok, true}, emqx_nats_topic:validate_nats_subject(<<"中文.*.>"/utf8>>)),

    %% Invalid subjects
    ?assertEqual({error, empty_subject}, emqx_nats_topic:validate_nats_subject(<<"">>)),
    ?assertEqual({error, starts_with_dot}, emqx_nats_topic:validate_nats_subject(<<".foo">>)),
    ?assertEqual({error, ends_with_dot}, emqx_nats_topic:validate_nats_subject(<<"foo.">>)),
    ?assertEqual({error, consecutive_dots}, emqx_nats_topic:validate_nats_subject(<<"foo..bar">>)),
    ?assertEqual(
        {error, special_chars_in_middle}, emqx_nats_topic:validate_nats_subject(<<"foo bar">>)
    ),
    ?assertEqual(
        {error, special_chars_in_middle}, emqx_nats_topic:validate_nats_subject(<<"foo.>.bar">>)
    ),
    ?assertEqual(
        {error, special_chars_in_middle}, emqx_nats_topic:validate_nats_subject(<<"foo.>bar">>)
    ).
