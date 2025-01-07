%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_topic_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-import(
    emqx_topic,
    [
        wildcard/1,
        match/2,
        intersection/2,
        validate/1,
        prepend/2,
        join/1,
        words/1,
        systop/1,
        feed_var/3,
        parse/1,
        parse/2
    ]
).

-define(N, 100000).

all() -> emqx_common_test_helpers:all(?MODULE).

t_wildcard(_) ->
    true = wildcard(<<"a/b/#">>),
    true = wildcard(<<"a/+/#">>),
    false = wildcard(<<"">>),
    false = wildcard(<<"a/b/c">>).

t_match1(_) ->
    true = match(<<"a/b/c">>, <<"a/b/+">>),
    true = match(<<"a/b/c">>, <<"a/#">>),
    true = match(<<"abcd/ef/g">>, <<"#">>),
    true = match(<<"abc/de/f">>, <<"abc/de/f">>),
    true = match(<<"abc">>, <<"+">>),
    true = match(<<"a/b/c">>, <<"a/b/c">>),
    false = match(<<"a/b/c">>, <<"a/c/d">>),
    false = match(<<"$share/x/y">>, <<"+">>),
    false = match(<<"$share/x/y">>, <<"+/x/y">>),
    false = match(<<"$share/x/y">>, <<"#">>),
    false = match(<<"$share/x/y">>, <<"+/+/#">>),
    false = match(<<"house/1/sensor/0">>, <<"house/+">>),
    false = match(<<"house">>, <<"house/+">>).

t_match2(_) ->
    true = match(<<"sport/tennis/player1">>, <<"sport/tennis/player1/#">>),
    true = match(<<"sport/tennis/player1/ranking">>, <<"sport/tennis/player1/#">>),
    true = match(<<"sport/tennis/player1/score/wimbledon">>, <<"sport/tennis/player1/#">>),
    true = match(<<"sport">>, <<"sport/#">>),
    true = match(<<"sport">>, <<"#">>),
    true = match(<<"/sport/football/score/1">>, <<"#">>),
    true = match(<<"Topic/C">>, <<"+/+">>),
    true = match(<<"TopicA/B">>, <<"+/+">>),
    true = match(<<"TopicA/C">>, <<"+/+">>),
    true = match(<<"abc">>, <<"+">>),
    true = match(<<"a/b/c">>, <<"a/b/c">>),
    false = match(<<"a/b/c">>, <<"a/c/d">>),
    false = match(<<"$share/x/y">>, <<"+">>),
    false = match(<<"$share/x/y">>, <<"+/x/y">>),
    false = match(<<"$share/x/y">>, <<"#">>),
    false = match(<<"$share/x/y">>, <<"+/+/#">>),
    false = match(<<"house/1/sensor/0">>, <<"house/+">>).

t_match3(_) ->
    true = match(<<"device/60019423a83c/fw">>, <<"device/60019423a83c/#">>),
    true = match(<<"device/60019423a83c/$fw">>, <<"device/60019423a83c/#">>),
    true = match(<<"device/60019423a83c/$fw/fw">>, <<"device/60019423a83c/$fw/#">>),
    true = match(<<"device/60019423a83c/fw/checksum">>, <<"device/60019423a83c/#">>),
    true = match(<<"device/60019423a83c/$fw/checksum">>, <<"device/60019423a83c/#">>),
    true = match(<<"device/60019423a83c/dust/type">>, <<"device/60019423a83c/#">>).

t_sigle_level_match(_) ->
    true = match(<<"sport/tennis/player1">>, <<"sport/tennis/+">>),
    false = match(<<"sport/tennis/player1/ranking">>, <<"sport/tennis/+">>),
    false = match(<<"sport">>, <<"sport/+">>),
    true = match(<<"sport/">>, <<"sport/+">>),
    true = match(<<"/finance">>, <<"+/+">>),
    true = match(<<"/finance">>, <<"/+">>),
    false = match(<<"/finance">>, <<"+">>),
    true = match(<<"/devices/$dev1">>, <<"/devices/+">>),
    true = match(<<"/devices/$dev1/online">>, <<"/devices/+/online">>).

t_sys_match(_) ->
    true = match(<<"$SYS/broker/clients/testclient">>, <<"$SYS/#">>),
    true = match(<<"$SYS/broker">>, <<"$SYS/+">>),
    false = match(<<"$SYS/broker">>, <<"+/+">>),
    false = match(<<"$SYS/broker">>, <<"#">>).

't_#_match'(_) ->
    true = match(<<"a/b/c">>, <<"#">>),
    true = match(<<"a/b/c">>, <<"+/#">>),
    false = match(<<"$SYS/brokers">>, <<"#">>),
    true = match(<<"a/b/$c">>, <<"a/b/#">>),
    true = match(<<"a/b/$c">>, <<"a/#">>).

t_match_tokens(_) ->
    true = match(emqx_topic:tokens(<<"a/b/c">>), words(<<"a/+/c">>)),
    true = match(emqx_topic:tokens(<<"a//c">>), words(<<"a/+/c">>)),
    false = match(emqx_topic:tokens(<<"a//c/">>), words(<<"a/+/c">>)),
    true = match(emqx_topic:tokens(<<"a//c/">>), words(<<"a/+/c/#">>)).

t_match_perf(_) ->
    true = match(<<"a/b/ccc">>, <<"a/#">>),
    Name = <<"/abkc/19383/192939/akakdkkdkak/xxxyyuya/akakak">>,
    Filter = <<"/abkc/19383/+/akakdkkdkak/#">>,
    true = match(Name, Filter),
    ok = bench('match/2', fun emqx_topic:match/2, [Name, Filter]).

t_intersect(_) ->
    <<"t/global/1/+">> = intersection(<<"t/global/#">>, <<"t/+/1/+">>),
    <<"t/global/#">> = intersection(<<"t/global/#">>, <<"#">>),
    <<"t/global/#">> = intersection(<<"t/global/#">>, <<"t/global/#">>),
    <<"1/2/3/4/5">> = intersection(<<"1/+/3/+/5/#">>, <<"+/2/+/4/+">>),
    <<"t/local/1">> = intersection(<<"t/local/1/#">>, <<"t/local/+">>),
    false = intersection(<<"t/global/#">>, <<"t/local/+">>),
    false = intersection(<<"t/local/1/+">>, <<"t/local/+">>).

t_intersect_topic_wildcard(_) ->
    <<"t/test/1">> = intersection(<<"t/test/#">>, <<"t/test/1">>),
    <<"t/test/1/1">> = intersection(<<"t/test/1/1">>, <<"t/test/#">>),
    false = intersection(<<"t/test/1/1">>, <<"t/test/+">>),
    <<"t/test/1/1">> = intersection(<<"t/test/1/1">>, <<"t/test/1/1">>),
    false = intersection(<<"t/test/1">>, <<"t/test/2">>),
    false = intersection(<<"t/test/1">>, <<"t/test/1/2">>).

t_intersect_commutes(_) ->
    ?assertEqual(
        intersection(<<"t/+/1/+">>, <<"t/global/#">>),
        intersection(<<"t/global/#">>, <<"t/+/1/+">>)
    ),
    ?assertEqual(
        intersection(<<"#">>, <<"t/global/#">>),
        intersection(<<"t/global/#">>, <<"#">>)
    ),
    ?assertEqual(
        intersection(<<"+/2/+/4/+">>, <<"1/+/3/+/5/#">>),
        intersection(<<"1/+/3/+/5/#">>, <<"+/2/+/4/+">>)
    ),
    ?assertEqual(
        intersection(<<"t/local/+">>, <<"t/local/1/#">>),
        intersection(<<"t/local/1/#">>, <<"t/local/+">>)
    ),
    ?assertEqual(
        intersection(<<"t/local/+">>, <<"t/global/#">>),
        intersection(<<"t/global/#">>, <<"t/local/+">>)
    ),
    ?assertEqual(
        intersection(<<"t/local/+">>, <<"t/local/1/+">>),
        intersection(<<"t/local/1/+">>, <<"t/local/+">>)
    ),
    ?assertEqual(
        intersection(<<"t/test/#">>, <<"t/test/1/1">>),
        intersection(<<"t/test/1/1">>, <<"t/test/#">>)
    ),
    ?assertEqual(
        intersection(<<"t/test/+">>, <<"t/test/1/1">>),
        intersection(<<"t/test/1/1">>, <<"t/test/+">>)
    ).

t_sys_intersect(_) ->
    <<"$SYS/broker/+">> = intersection(<<"$SYS/broker/#">>, <<"$SYS/+/+">>),
    <<"$SYS/broker">> = intersection(<<"$SYS/broker">>, <<"$SYS/+">>),
    false = intersection(<<"$SYS/broker">>, <<"+/+">>),
    false = intersection(<<"$SYS/broker">>, <<"#">>).

t_union(_) ->
    Union = fun(Topics) -> lists:sort(emqx_topic:union(Topics)) end,
    ?assertEqual([], Union([])),
    ?assertEqual([<<"t/1/2/+">>], Union([<<"t/1/2/+">>])),
    ?assertEqual([<<"t/1/2/#">>], Union([<<"t/1/2/+">>, <<"t/1/2/#">>])),
    ?assertEqual([<<"t/+/1/#">>, <<"t/1/+/#">>], Union([<<"t/+/1/#">>, <<"t/1/+/#">>])),
    ?assertEqual(
        [<<"g/1">>, <<"t/+/+/#">>, <<"t/1">>],
        Union([<<"t/1">>, <<"t/1">>, <<"t/2/+">>, <<"t/+/+/#">>, <<"t/+/+">>, <<"g/1">>])
    ),
    ?assertEqual(
        [<<"#">>],
        Union([<<"t/1">>, <<"t/1">>, <<"t/2/+">>, <<"t/+/+/#">>, <<"#">>, <<"g/1">>])
    ).

t_union_sys(_) ->
    Union = fun(Topics) -> lists:sort(emqx_topic:union(Topics)) end,
    ?assertEqual(
        [<<"$SYS/cluster">>, <<"+/#">>],
        Union([<<"+/#">>, <<"t/1/2/#">>, <<"$SYS/cluster">>])
    ),
    ?assertEqual(
        [<<"$SYS/+">>, <<"$SYS/cluster/#">>, <<"+/#">>],
        Union([<<"+/#">>, <<"t/1/2">>, <<"$SYS/cluster">>, <<"$SYS/cluster/#">>, <<"$SYS/+">>])
    ).

t_validate(_) ->
    true = validate(<<"a/+/#">>),
    true = validate(<<"a/b/c/d">>),
    true = validate({name, <<"abc/de/f">>}),
    true = validate({filter, <<"abc/+/f">>}),
    true = validate({filter, <<"abc/#">>}),
    true = validate({filter, <<"x">>}),
    true = validate({name, <<"x//y">>}),
    true = validate({filter, <<"sport/tennis/#">>}),
    %% MQTT-5.0 [MQTT-4.7.3-1]
    ?assertError(empty_topic, validate({name, <<>>})),
    ?assertError(empty_topic, validate({filter, <<>>})),
    ?assertError(topic_name_error, validate({name, <<"abc/#">>})),
    ?assertError(topic_too_long, validate({name, long_topic()})),
    ?assertError(topic_invalid_char, validate({filter, <<"abc/#xzy/+">>})),
    ?assertError(topic_invalid_char, validate({filter, <<"abc/xzy/+9827">>})),
    ?assertError(topic_invalid_char, validate({filter, <<"sport/tennis#">>})),
    %% MQTT-5.0 [MQTT-4.7.1-1]
    ?assertError('topic_invalid_#', validate({filter, <<"abc/#/1">>})),
    ?assertError('topic_invalid_#', validate({filter, <<"sport/tennis/#/ranking">>})),
    %% MQTT-5.0 [MQTT-4.8.2-1]
    ?assertError(?SHARE_EMPTY_FILTER, validate({filter, <<"$share/">>})),
    ?assertError(?SHARE_EMPTY_FILTER, validate({filter, <<"$share//">>})),
    ?assertError(?SHARE_EMPTY_GROUP, validate({filter, <<"$share//t">>})),
    ?assertError(?SHARE_EMPTY_GROUP, validate({filter, <<"$share//test">>})),
    %% MQTT-5.0 [MQTT-4.7.3-1] for shared-sub
    ?assertError(?SHARE_EMPTY_FILTER, validate({filter, <<"$share/g/">>})),
    ?assertError(?SHARE_EMPTY_FILTER, validate({filter, <<"$share/g2/">>})),
    %% MQTT-5.0 [MQTT-4.8.2-2]
    ?assertError(?SHARE_NAME_INVALID_CHAR, validate({filter, <<"$share/p+q/1">>})),
    ?assertError(?SHARE_NAME_INVALID_CHAR, validate({filter, <<"$share/m+/1">>})),
    ?assertError(?SHARE_NAME_INVALID_CHAR, validate({filter, <<"$share/+n/1">>})),
    ?assertError(?SHARE_NAME_INVALID_CHAR, validate({filter, <<"$share/x#y/1">>})),
    ?assertError(?SHARE_NAME_INVALID_CHAR, validate({filter, <<"$share/x#/1">>})),
    ?assertError(?SHARE_NAME_INVALID_CHAR, validate({filter, <<"$share/#y/1">>})),
    %% share recursively
    ?assertError(?SHARE_RECURSIVELY, validate({filter, <<"$share/g1/$share/t">>})),
    true = validate({filter, <<"$share/g1/topic/$share">>}).

t_sigle_level_validate(_) ->
    true = validate({filter, <<"+">>}),
    true = validate({filter, <<"+/tennis/#">>}),
    true = validate({filter, <<"sport/+/player1">>}),
    ?assertError(topic_invalid_char, validate({filter, <<"sport+">>})).

t_prepend(_) ->
    ?assertEqual(<<"ab">>, prepend(undefined, <<"ab">>)),
    ?assertEqual(<<"a/b">>, prepend(<<>>, <<"a/b">>)),
    ?assertEqual(<<"x/a/b">>, prepend("x/", <<"a/b">>)),
    ?assertEqual(<<"x/y/a/b">>, prepend(<<"x/y">>, <<"a/b">>)),
    ?assertEqual(<<"+/a/b">>, prepend('+', <<"a/b">>)).

t_levels(_) ->
    ?assertEqual(3, emqx_topic:levels(<<"a/+/#">>)),
    ?assertEqual(4, emqx_topic:levels(<<"a/b/c/d">>)).

t_tokens(_) ->
    ?assertEqual(
        [<<"a">>, <<"b">>, <<"+">>, <<"#">>],
        emqx_topic:tokens(<<"a/b/+/#">>)
    ).

t_words(_) ->
    Topic = <<"/abkc/19383/+/akakdkkdkak/#">>,
    ?assertEqual(['', <<"a">>, '+', '#'], words(<<"/a/+/#">>)),
    ?assertEqual(['', <<"abkc">>, <<"19383">>, '+', <<"akakdkkdkak">>, '#'], words(Topic)),
    ok = bench('words/1', fun emqx_topic:words/1, [Topic]),
    BSplit = fun(Bin) -> binary:split(Bin, <<"/">>, [global]) end,
    ok = bench('binary:split/3', BSplit, [Topic]).

t_join(_) ->
    ?assertEqual(<<>>, join([])),
    ?assertEqual(<<"x">>, join([<<"x">>])),
    ?assertEqual(<<"#">>, join(['#'])),
    ?assertEqual(<<"+//#">>, join(['+', '', '#'])),
    ?assertEqual(<<"x/y/z/+">>, join([<<"x">>, <<"y">>, <<"z">>, '+'])),
    ?assertEqual(<<"/ab/cd/ef/">>, join(words(<<"/ab/cd/ef/">>))),
    ?assertEqual(<<"ab/+/#">>, join(words(<<"ab/+/#">>))),
    %% MQTT-5.0 [MQTT-4.7.1-1]
    ?assertError('topic_invalid_#', join(['+', <<"a">>, '#', <<"b">>, '', '+'])),
    ?assertError('topic_invalid_#', join(['+', <<"c">>, <<"#">>, <<"d">>, '', '+'])).

t_systop(_) ->
    SysTop1 = iolist_to_binary(["$SYS/brokers/", atom_to_list(node()), "/xyz"]),
    ?assertEqual(SysTop1, systop('xyz')),
    SysTop2 = iolist_to_binary(["$SYS/brokers/", atom_to_list(node()), "/abc"]),
    ?assertEqual(SysTop2, systop(<<"abc">>)).

t_feed_var(_) ->
    ?assertEqual(
        <<"$queue/client/clientId">>,
        feed_var(<<"$c">>, <<"clientId">>, <<"$queue/client/$c">>)
    ),
    ?assertEqual(
        <<"username/test/client/x">>,
        feed_var(
            ?PH_USERNAME,
            <<"test">>,
            <<"username/", ?PH_USERNAME/binary, "/client/x">>
        )
    ),
    ?assertEqual(
        <<"username/test/client/clientId">>,
        feed_var(
            ?PH_CLIENTID,
            <<"clientId">>,
            <<"username/test/client/", ?PH_CLIENTID/binary>>
        )
    ).

long_topic() ->
    iolist_to_binary([[integer_to_list(I), "/"] || I <- lists:seq(0, 66666)]).

t_parse(_) ->
    ?assertError(
        {invalid_topic_filter, <<"$queue/t">>},
        parse(#share{group = <<"$queue">>, topic = <<"$queue/t">>}, #{})
    ),
    ?assertError(
        {invalid_topic_filter, <<"$share/g/t">>},
        parse(#share{group = <<"g">>, topic = <<"$share/g/t">>}, #{})
    ),
    ?assertError(
        {invalid_topic_filter, <<"$share/t">>},
        parse(<<"$share/t">>)
    ),
    ?assertError(
        {invalid_topic_filter, <<"$share/+/t">>},
        parse(<<"$share/+/t">>)
    ),
    ?assertEqual({<<"a/b/+/#">>, #{}}, parse(<<"a/b/+/#">>)),
    ?assertEqual({<<"a/b/+/#">>, #{qos => 1}}, parse({<<"a/b/+/#">>, #{qos => 1}})),
    ?assertEqual(
        {#share{group = <<"$queue">>, topic = <<"topic">>}, #{}}, parse(<<"$queue/topic">>)
    ),
    ?assertEqual(
        {#share{group = <<"group">>, topic = <<"topic">>}, #{}}, parse(<<"$share/group/topic">>)
    ),
    %% The '$local' and '$fastlane' topics have been deprecated.
    ?assertEqual({<<"$local/topic">>, #{}}, parse(<<"$local/topic">>)),
    ?assertEqual({<<"$local/$queue/topic">>, #{}}, parse(<<"$local/$queue/topic">>)),
    ?assertEqual({<<"$local/$share/group/a/b/c">>, #{}}, parse(<<"$local/$share/group/a/b/c">>)),
    ?assertEqual({<<"$fastlane/topic">>, #{}}, parse(<<"$fastlane/topic">>)).

bench(Case, Fun, Args) ->
    {Time, ok} = timer:tc(
        fun lists:foreach/2,
        [
            fun(_) -> apply(Fun, Args) end,
            lists:seq(1, ?N)
        ]
    ),
    ct:pal(
        "Time consumed by ~ts: ~.3f(us)~nCall ~ts per second: ~w",
        [Case, Time / ?N, Case, (?N * 1000000) div Time]
    ).
