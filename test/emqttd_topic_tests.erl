%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_topic_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-import(emqttd_topic, [validate/1, wildcard/1, match/2, triples/1, words/1]).

-define(N, 100000).

validate_test() ->
	?assert( validate({filter, <<"sport/tennis/#">>}) ),
	?assert( validate({filter, <<"a/b/c">>}) ),
	?assert( validate({filter, <<"/a/b">>}) ),
	?assert( validate({filter, <<"/+/x">>}) ),
	?assert( validate({filter, <<"/a/b/c/#">>}) ),
	?assertNot( validate({filter, <<"a/#/c">>}) ),
	?assertNot( validate({filter, <<"sport/tennis#">>}) ),
	?assertNot( validate({filter, <<"sport/tennis/#/ranking">>}) ).

sigle_level_validate_test() ->
    ?assert( validate({filter, <<"+">>}) ),
    ?assert( validate({filter, <<"+/tennis/#">>}) ),
    ?assertNot( validate({filter, <<"sport+">>}) ),
    ?assert( validate({filter, <<"sport/+/player1">>}) ).

match_test() ->
    ?assert( match(<<"sport/tennis/player1">>, <<"sport/tennis/player1/#">>) ),
    ?assert( match(<<"sport/tennis/player1/ranking">>, <<"sport/tennis/player1/#">>) ),
    ?assert( match(<<"sport/tennis/player1/score/wimbledon">>, <<"sport/tennis/player1/#">>) ),

    ?assert( match(<<"sport">>, <<"sport/#">>) ),
    ?assert( match(<<"sport">>, <<"#">>) ),
    ?assert( match(<<"/sport/football/score/1">>, <<"#">>) ),
    %% paho test
    ?assert( match(<<"Topic/C">>, <<"+/+">>) ),
    ?assert( match(<<"TopicA/B">>, <<"+/+">>) ),
    ?assert( match(<<"TopicA/C">>, <<"+/+">>) ).

sigle_level_match_test() ->
    ?assert( match(<<"sport/tennis/player1">>, <<"sport/tennis/+">>) ),
    ?assertNot( match(<<"sport/tennis/player1/ranking">>, <<"sport/tennis/+">>) ),
    ?assertNot( match(<<"sport">>, <<"sport/+">>) ),
    ?assert( match(<<"sport/">>, <<"sport/+">>) ),
    ?assert( match(<<"/finance">>, <<"+/+">>) ),
    ?assert( match(<<"/finance">>, <<"/+">>) ),
    ?assertNot( match(<<"/finance">>, <<"+">>) ).

sys_match_test() ->
    ?assert( match(<<"$SYS/broker/clients/testclient">>, <<"$SYS/#">>) ),
    ?assert( match(<<"$SYS/broker">>, <<"$SYS/+">>) ),
    ?assertNot( match(<<"$SYS/broker">>, <<"+/+">>) ),
    ?assertNot( match(<<"$SYS/broker">>, <<"#">>) ).

'#_match_test'() ->
    ?assert( match(<<"a/b/c">>, <<"#">>) ),
    ?assert( match(<<"a/b/c">>, <<"+/#">>) ),
    ?assertNot( match(<<"$SYS/brokers">>, <<"#">>) ).

match_perf_test() ->
    ?assert( match(<<"a/b/ccc">>, <<"a/#">>) ),
    Name = <<"/abkc/19383/192939/akakdkkdkak/xxxyyuya/akakak">>,
    Filter = <<"/abkc/19383/+/akakdkkdkak/#">>,
    ?assert( match(Name, Filter) ),
    %?debugFmt("Match ~p with ~p", [Name, Filter]),
    {Time, _} = timer:tc(fun() ->
                [match(Name, Filter) || _I <- lists:seq(1, ?N)]
        end),
    ?debugFmt("Time for match: ~p(micro)", [Time/?N]),
    ok.

triples_test() ->
    Triples = [{root, <<"a">>, <<"a">>}, {<<"a">>, <<"b">>, <<"a/b">>}],
    ?assertMatch(Triples, triples(<<"a/b">>) ).

triples_perf_test() ->
    Topic = <<"/abkc/19383/192939/akakdkkdkak/xxxyyuya/akakak">>,
    {Time, _} = timer:tc(fun() ->
                [triples(Topic) || _I <- lists:seq(1, ?N)]
        end),
    ?debugFmt("Time for triples: ~p(micro)", [Time/?N]),
    ok.

type_test() ->
	?assertEqual(false, wildcard(<<"/a/b/cdkd">>)),
	?assertEqual(true, wildcard(<<"/a/+/d">>)),
	?assertEqual(true, wildcard(<<"/a/b/#">>)).

words_test() ->
    ?assertMatch(['', <<"abkc">>, <<"19383">>, '+', <<"akakdkkdkak">>, '#'],  words(<<"/abkc/19383/+/akakdkkdkak/#">>)),
    {Time, _} = timer:tc(fun() ->
                [words(<<"/abkc/19383/+/akakdkkdkak/#">>) || _I <- lists:seq(1, ?N)]
        end),
    ?debugFmt("Time for words: ~p(micro)", [Time/?N]),
    {Time2, _} = timer:tc(fun() ->
                [binary:split(<<"/abkc/19383/+/akakdkkdkak/#">>, <<"/">>, [global]) || _I <- lists:seq(1, ?N)]
        end),
    ?debugFmt("Time for binary:split: ~p(micro)", [Time2/?N]),
    ok.

feed_var_test() ->
    ?assertEqual(<<"$Q/client/clientId">>, emqttd_topic:feed_var(<<"$c">>, <<"clientId">>, <<"$Q/client/$c">>)).

join_test() ->
    ?assertEqual(<<"/ab/cd/ef/">>, emqttd_topic:join(words(<<"/ab/cd/ef/">>))),
    ?assertEqual(<<"ab/+/#">>, emqttd_topic:join(words(<<"ab/+/#">>))).

-endif.
