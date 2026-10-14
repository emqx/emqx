%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-doc """
`wildcard/1` on a binary is true when a level is `+` or `#`. The cases put the wildcard level
at the start, in the middle, at the end and across the 7-byte words of the scan.
""".
t_wildcard_binary_levels(_) ->
    Cases = [
        {<<"+">>, true},
        {<<"#">>, true},
        {<<"+/a">>, true},
        {<<"a/+">>, true},
        {<<"a/#">>, true},
        {<<"a/+/b">>, true},
        {<<"/+">>, true},
        {<<"+/">>, true},
        {<<"/#">>, true},
        {<<"abcdef/+/x">>, true},
        {<<"abcdefg/+">>, true},
        {<<"abcdefghijklmn/+/o">>, true},
        {<<"abcdefghijklmn/opq/#">>, true},
        {<<>>, false},
        {<<"a">>, false},
        {<<"/">>, false},
        {<<"//">>, false},
        {<<"abcdefghijklmn/opq">>, false}
    ],
    lists:foreach(
        fun({Topic, Expected}) ->
            ?assertEqual({Topic, Expected}, {Topic, wildcard(Topic)}),
            ?assertEqual({Topic, Expected}, {Topic, wildcard(words(Topic))})
        end,
        Cases
    ).

-doc """
`match/2` on two binaries gives the same results as on word lists for valid topics. The cases
put wildcard levels before, at and after the end of the 7-byte words that are compared at once.
""".
t_match_binary_levels(_) ->
    Cases = [
        {<<"a">>, <<"a/#">>, true},
        {<<"a/">>, <<"a/+">>, true},
        {<<"a">>, <<"a/+">>, false},
        {<<"a/b">>, <<"a/">>, false},
        {<<"#/x">>, <<"#">>, false},
        {<<"#">>, <<"#">>, true},
        {<<"$SYS/x">>, <<"#">>, false},
        {<<"$SYS/x">>, <<"+/x">>, false},
        {<<"$SYS/x">>, <<"$SYS/+">>, true},
        {<<"abcdef/xyz">>, <<"abcdef/#">>, true},
        {<<"abcdef/xyz/q">>, <<"abcdef/+/q">>, true},
        {<<"abcde/xa/b">>, <<"abcde/+/b">>, true},
        {<<"abcde/xa/b">>, <<"abcde/xa/c">>, false},
        {<<"abcdefghijklmn/opq">>, <<"abcdefghijklmn/opq">>, true},
        {<<"abcdefghijklmn/opq">>, <<"abcdefghijklmn/opr">>, false},
        {<<"abcdefghijklmn">>, <<"abcdefghijklmn/#">>, true},
        {<<"abcdefghijklmn/">>, <<"abcdefghijklmn/+">>, true},
        %% A filter as the name: + and # at the end of the 7 bytes compared at once.
        {<<"abcde/+">>, <<"abcde/+">>, true},
        {<<"abcde/+/x">>, <<"abcde/+/x">>, true},
        {<<"abcde/+/x">>, <<"abcde/+/y">>, false},
        {<<"abcde/+/x">>, <<"abcde/+/+">>, true},
        {<<"abcde/#">>, <<"abcde/#">>, true},
        {<<"abcde/#">>, <<"abcde/+">>, true},
        {<<"abcde/+">>, <<"abcde/#">>, true}
    ],
    lists:foreach(
        fun({Name, Filter, Expected}) ->
            ?assertEqual({Name, Filter, Expected}, {Name, Filter, match(Name, Filter)}),
            ?assertEqual(
                {Name, Filter, Expected}, {Name, Filter, match(words(Name), words(Filter))}
            )
        end,
        Cases
    ).

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

-doc """
`validate/2` checks a topic in one scan when the topic is valid: `+` as a whole level, `#` as
the whole last level, no NUL, valid UTF-8. An invalid topic gives the same error as the full
check. The cases put the special byte before, inside and after the first 7-byte word.
""".
t_validate_fast_path(_) ->
    lists:foreach(
        fun(Topic) ->
            ?assertEqual({Topic, true}, {Topic, validate({name, Topic})}),
            ?assertEqual({Topic, true}, {Topic, validate({filter, Topic})})
        end,
        [
            <<"a">>,
            <<"/">>,
            <<"//">>,
            <<"abcdefg">>,
            <<"abcdefgh/ijklmno/p">>,
            <<"$SYS/x">>,
            <<"a", 1, "b">>,
            <<"abcdefg", 16#C3, 16#A9>>,
            <<"abcdefghijklmn/", 16#E2, 16#82, 16#AC>>
        ]
    ),
    lists:foreach(
        fun(Filter) -> ?assertEqual({Filter, true}, {Filter, validate({filter, Filter})}) end,
        [
            <<"+">>,
            <<"#">>,
            <<"+/+">>,
            <<"+/#">>,
            <<"/#">>,
            <<"abcdefgh/#">>,
            <<"abcdefg/+/hijklmn/+">>,
            <<"caf", 16#C3, 16#A9, "/+/#">>,
            <<"$share/g/abcdefg/+/#">>
        ]
    ),
    ?assertError(topic_name_error, validate({name, <<"abcdefgh/#">>})),
    ?assertError(topic_name_error, validate({name, <<"abcdefgh/+">>})),
    ?assertError(topic_invalid_char, validate({name, <<"abcdefg", 0, "h">>})),
    ?assertError(topic_invalid_char, validate({filter, <<"abcdefgh#">>})),
    ?assertError(topic_invalid_char, validate({filter, <<"abcdefg/+h">>})),
    ?assertError(topic_invalid_char, validate({filter, <<"abc#/#">>})),
    ?assertError('topic_invalid_#', validate({filter, <<"abcdefg/#/h">>})),
    ?assertError('topic_invalid_#', validate({filter, <<"#/+">>})),
    ?assertError('topic_invalid_#', validate({filter, <<"$share/g/a/#/b">>})),
    ?assertError(function_clause, validate({name, <<"abcdefg", 16#FF>>})),
    ?assertError(function_clause, validate({filter, <<"abcdefg/+/", 16#C3>>})).

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
    true = validate({filter, <<"$share/g1/topic/$share">>}),

    true = validate(#share{group = <<"g1">>, topic = <<"topic/$share">>}),
    ?assertError(?SHARE_EMPTY_GROUP, validate(#share{group = <<>>, topic = <<"t">>})),
    ?assertError(?SHARE_EMPTY_FILTER, validate(#share{group = <<"g">>, topic = <<>>})),
    ?assertError(topic_too_long, validate(#share{group = <<"g">>, topic = long_topic()})).

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
    ?assertEqual(4, emqx_topic:levels(<<"a/b/c/d">>)),
    ?assertEqual(1, emqx_topic:levels(<<"a">>)),
    ?assertEqual(2, emqx_topic:levels(<<"/">>)),
    ?assertEqual(3, emqx_topic:levels(<<"a//">>)).

-doc """
Check that levels/1 agrees with binary:split/3 when $/ is at each position, and with
the bytes 16#2E, 16#30 and 16#AF next to it. The positions cover the 7-byte count
and the byte-by-byte count after it.
""".
t_levels_byte_positions(_) ->
    Size = 23,
    Fillers = [<<"a">>, <<16#2E>>, <<16#30>>, <<16#AF>>, <<16#FF>>],
    lists:foreach(
        fun({Pos, Filler}) ->
            Body = binary:copy(Filler, Size),
            OneSlash = <<
                (binary:copy(Filler, Pos))/binary, $/, (binary:copy(Filler, Size - Pos - 1))/binary
            >>,
            EveryOther = binary:copy(<<Filler/binary, $/>>, Pos),
            lists:foreach(
                fun(Topic) ->
                    Expected = length(binary:split(Topic, <<"/">>, [global])),
                    ?assertEqual(Expected, emqx_topic:levels(Topic), Topic)
                end,
                [Body, OneSlash, EveryOther]
            )
        end,
        [{Pos, F} || Pos <- lists:seq(0, Size - 1), F <- Fillers]
    ).

t_tokens(_) ->
    ?assertEqual(
        [<<"a">>, <<"b">>, <<"+">>, <<"#">>],
        emqx_topic:tokens(<<"a/b/+/#">>)
    ).

-doc "Check that tokens/1 and levels/1 agree with binary:split/3 on empty levels.".
t_tokens_empty_levels(_) ->
    lists:foreach(
        fun(Topic) ->
            Expected = binary:split(Topic, <<"/">>, [global]),
            ?assertEqual(Expected, emqx_topic:tokens(Topic), Topic),
            ?assertEqual(length(Expected), emqx_topic:levels(Topic), Topic)
        end,
        [
            <<>>,
            <<"/">>,
            <<"//">>,
            <<"a/">>,
            <<"/a">>,
            <<"a//b">>,
            <<"/a//b/">>,
            <<"中/文/"/utf8>>
        ]
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
    ?assertEqual(<<"li/x">>, join(["li", <<"x">>])),
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
