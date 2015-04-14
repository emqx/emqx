%%-----------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------
-module(emqtt_topic_tests).

-import(emqtt_topic, [validate/1, wildcard/1, match/2, triples/1, words/1]).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

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
    ?assert( match(<<"/sport/football/score/1">>, <<"#">>) ).

sigle_level_match_test() ->
    ?assert( match(<<"sport/tennis/player1">>, <<"sport/tennis/+">>) ),
    ?assertNot( match(<<"sport/tennis/player1/ranking">>, <<"sport/tennis/+">>) ),
    ?assertNot( match(<<"sport">>, <<"sport/+">>) ),
    ?assert( match(<<"sport/">>, <<"sport/+">>) ),
    ?assert( match(<<"/finance">>, <<"+/+">>) ),
    ?assert( match(<<"/finance">>, <<"/+">>) ),
    ?assertNot( match(<<"/finance">>, <<"+">>) ).

sys_match_test() ->
    ?assert( match(<<"$SYS/borker/clients/testclient">>, <<"$SYS/#">>) ),
    ?assert( match(<<"$SYS/borker">>, <<"$SYS/+">>) ),
    ?assertNot( match(<<"$SYS/borker">>, <<"+/+">>) ),
    ?assertNot( match(<<"$SYS/borker">>, <<"#">>) ).

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

-endif.

