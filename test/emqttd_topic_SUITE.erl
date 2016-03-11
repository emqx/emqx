%%--------------------------------------------------------------------
%% Copyright (c) 2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_topic_SUITE).

%% CT
-compile(export_all).

-import(emqttd_topic, [wildcard/1, match/2, validate/1, triples/1, join/1,
                       words/1, systop/1, is_queue/1, feed_var/3]).

-define(N, 10000).

all() -> [t_wildcard, t_match, t_match2, t_validate, t_triples, t_join,
          t_words, t_systop, t_is_queue, t_feed_var, t_sys_match, 't_#_match',
          t_sigle_level_validate, t_sigle_level_match, t_match_perf,
          t_triples_perf].

t_wildcard(_) ->
    true  = wildcard(<<"a/b/#">>),
    true  = wildcard(<<"a/+/#">>),
    false = wildcard(<<"">>),
    false = wildcard(<<"a/b/c">>).

t_match(_) ->
    true  = match(<<"a/b/c">>, <<"a/b/+">>),
    true  = match(<<"a/b/c">>, <<"a/#">>),
    true  = match(<<"abcd/ef/g">>, <<"#">>),
    true  = match(<<"abc/de/f">>, <<"abc/de/f">>),
    true  = match(<<"abc">>, <<"+">>),
    true  = match(<<"a/b/c">>, <<"a/b/c">>),
    false = match(<<"a/b/c">>, <<"a/c/d">>),
    false = match(<<"$shared/x/y">>, <<"+">>),
    false = match(<<"$shared/x/y">>, <<"+/x/y">>),
    false = match(<<"$shared/x/y">>, <<"#">>),
    false = match(<<"$shared/x/y">>, <<"+/+/#">>),
    false = match(<<"house/1/sensor/0">>, <<"house/+">>),
    false = match(<<"house">>, <<"house/+">>).

t_match2(_) ->
    true  = match(<<"sport/tennis/player1">>, <<"sport/tennis/player1/#">>),
    true  = match(<<"sport/tennis/player1/ranking">>, <<"sport/tennis/player1/#">>),
    true  = match(<<"sport/tennis/player1/score/wimbledon">>, <<"sport/tennis/player1/#">>),
    true  = match(<<"sport">>, <<"sport/#">>),
    true  = match(<<"sport">>, <<"#">>),
    true  = match(<<"/sport/football/score/1">>, <<"#">>),
    true  = match(<<"Topic/C">>, <<"+/+">>),
    true  = match(<<"TopicA/B">>, <<"+/+">>),
    true  = match(<<"TopicA/C">>, <<"+/+">>),
    true  = match(<<"abc">>, <<"+">>),
    true  = match(<<"a/b/c">>, <<"a/b/c">>),
    false = match(<<"a/b/c">>, <<"a/c/d">>),
    false = match(<<"$shared/x/y">>, <<"+">>),
    false = match(<<"$shared/x/y">>, <<"+/x/y">>),
    false = match(<<"$shared/x/y">>, <<"#">>),
    false = match(<<"$shared/x/y">>, <<"+/+/#">>),
    false = match(<<"house/1/sensor/0">>, <<"house/+">>).

t_sigle_level_match(_) ->
    true  = match(<<"sport/tennis/player1">>, <<"sport/tennis/+">>),
    false = match(<<"sport/tennis/player1/ranking">>, <<"sport/tennis/+">>),
    false = match(<<"sport">>, <<"sport/+">>),
    true  = match(<<"sport/">>, <<"sport/+">>),
    true  = match(<<"/finance">>, <<"+/+">>),
    true  = match(<<"/finance">>, <<"/+">>),
    false = match(<<"/finance">>, <<"+">>).

t_sys_match(_) ->
    true  = match(<<"$SYS/broker/clients/testclient">>, <<"$SYS/#">>),
    true  = match(<<"$SYS/broker">>, <<"$SYS/+">>),
    false = match(<<"$SYS/broker">>, <<"+/+">>),
    false = match(<<"$SYS/broker">>, <<"#">>).

't_#_match'(_) ->
    true = match(<<"a/b/c">>, <<"#">>),
    true = match(<<"a/b/c">>, <<"+/#">>),
    false = match(<<"$SYS/brokers">>, <<"#">>).

t_match_perf(_) ->
    true = match(<<"a/b/ccc">>, <<"a/#">>),
    Name = <<"/abkc/19383/192939/akakdkkdkak/xxxyyuya/akakak">>,
    Filter = <<"/abkc/19383/+/akakdkkdkak/#">>,
    true = match(Name, Filter),
    {Time, _} = timer:tc(fun() ->
                [match(Name, Filter) || _I <- lists:seq(1, ?N)]
        end),
    io:format("Time for match: ~p(micro)", [Time/?N]).

t_validate(_) ->
    true  = validate({name, <<"abc/de/f">>}),
    true  = validate({filter, <<"abc/+/f">>}),
    true  = validate({filter, <<"abc/#">>}),
    true  = validate({filter, <<"x">>}),
    true  = validate({name, <<"x//y">>}),
	true  = validate({filter, <<"sport/tennis/#">>}),
    false = validate({name, <<>>}),
    false = validate({name, long_topic()}),
    false = validate({name, <<"abc/#">>}),
    false = validate({filter, <<"abc/#/1">>}),
    false = validate({filter, <<"abc/#xzy/+">>}),
    false = validate({filter, <<"abc/xzy/+9827">>}),
	false = validate({filter, <<"sport/tennis#">>}),
    false = validate({filter, <<"sport/tennis/#/ranking">>}).

t_sigle_level_validate(_) ->
    true  = validate({filter, <<"+">>}),
    true  = validate({filter, <<"+/tennis/#">>}),
    true  = validate({filter, <<"sport/+/player1">>}),
    false = validate({filter, <<"sport+">>}).

t_triples(_) ->
    Triples = [{root,<<"a">>,<<"a">>},
               {<<"a">>,<<"b">>,<<"a/b">>},
               {<<"a/b">>,<<"c">>,<<"a/b/c">>}],
    Triples = triples(<<"a/b/c">>).

t_triples_perf(_) ->
    Topic = <<"/abkc/19383/192939/akakdkkdkak/xxxyyuya/akakak">>,
    {Time, _} = timer:tc(fun() ->
                [triples(Topic) || _I <- lists:seq(1, ?N)]
        end),
    io:format("Time for triples: ~p(micro)", [Time/?N]).

t_words(_) ->
    ['', <<"a">>, '+', '#'] = words(<<"/a/+/#">>),
    ['', <<"abkc">>, <<"19383">>, '+', <<"akakdkkdkak">>, '#'] = words(<<"/abkc/19383/+/akakdkkdkak/#">>),
    {Time, _} = timer:tc(fun() ->
                [words(<<"/abkc/19383/+/akakdkkdkak/#">>) || _I <- lists:seq(1, ?N)]
        end),
    io:format("Time for words: ~p(micro)", [Time/?N]),
    {Time2, _} = timer:tc(fun() ->
                [binary:split(<<"/abkc/19383/+/akakdkkdkak/#">>, <<"/">>, [global]) || _I <- lists:seq(1, ?N)]
        end),
    io:format("Time for binary:split: ~p(micro)", [Time2/?N]).

t_join(_) ->
    <<>>       = join([]),
    <<"x">>    = join([<<"x">>]),
    <<"#">>    = join(['#']),
    <<"+//#">> = join(['+', '', '#']),
    <<"x/y/z/+">> = join([<<"x">>, <<"y">>, <<"z">>, '+']),
    <<"/ab/cd/ef/">> = join(words(<<"/ab/cd/ef/">>)),
    <<"ab/+/#">> = join(words(<<"ab/+/#">>)).

t_is_queue(_) ->
    true  = is_queue(<<"$queue/queue">>),
    false = is_queue(<<"xyz/queue">>).

t_systop(_) ->
    SysTop1 = iolist_to_binary(["$SYS/brokers/", atom_to_list(node()), "/xyz"]),
    SysTop1 = systop('xyz'),
    SysTop2 = iolist_to_binary(["$SYS/brokers/", atom_to_list(node()), "/abc"]),
    SysTop2 = systop(<<"abc">>).

t_feed_var(_) ->
    <<"$queue/client/clientId">> = feed_var(<<"$c">>, <<"clientId">>, <<"$queue/client/$c">>),
    <<"username/test/client/x">> = feed_var(<<"%u">>, <<"test">>, <<"username/%u/client/x">>),
    <<"username/test/client/clientId">> = feed_var(<<"%c">>, <<"clientId">>, <<"username/test/client/%c">>).

long_topic() ->
    iolist_to_binary([[integer_to_list(I), "/"] || I <- lists:seq(0, 10000)]).

