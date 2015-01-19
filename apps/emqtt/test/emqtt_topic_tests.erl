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

-include("emqtt_topic.hrl").

-import(emqtt_topic, [validate/1, type/1, match/2, triples/1, words/1]).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(N, 100000).

validate_test() ->
	?assert( validate({filter, <<"a/b/c">>}) ),
	?assert( validate({filter, <<"/a/b">>}) ),
	?assert( validate({filter, <<"/+/x">>}) ),
	?assert( validate({filter, <<"/a/b/c/#">>}) ),
	?assertNot( validate({filter, <<"a/#/c">>}) ).

match_test() ->
    ?assert( match(<<"a/b/ccc">>, <<"a/#">>) ),
    Name = <<"/abkc/19383/192939/akakdkkdkak/xxxyyuya/akakak">>,
    Filter = <<"/abkc/19383/+/akakdkkdkak/#">>,
    ?assert( match(Name, Filter) ),
    ?debugFmt("Match ~p with ~p", [Name, Filter]),
    {Time, _} = timer:tc(fun() -> 
                [match(Name, Filter) || _I <- lists:seq(1, ?N)]
        end),
    ?debugFmt("Time for match: ~p(micro)", [Time/?N]),
    ok.

triples_test() ->
    Topic = <<"/abkc/19383/192939/akakdkkdkak/xxxyyuya/akakak">>,
    {Time, _} = timer:tc(fun() -> 
                [triples(Topic) || _I <- lists:seq(1, ?N)]
        end),
    ?debugFmt("Time for triples: ~p(micro)", [Time/?N]),
    ok.

type_test() ->
	?assertEqual(direct, type(#topic{name = <<"/a/b/cdkd">>})),
	?assertEqual(wildcard, type(#topic{name = <<"/a/+/d">>})),
	?assertEqual(wildcard, type(#topic{name = <<"/a/b/#">>})).

words_test() ->
    ?debugFmt("Words: ~p", [words(<<"/abkc/19383/+/akakdkkdkak/#">>)]),
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

