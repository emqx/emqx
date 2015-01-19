-module(emqtt_oldtopic).

-export([triples/1, test/0]).

triples(B) when is_binary(B) ->
	triples(binary_to_list(B), []).

triples(S, Acc) ->
	triples(string:rchr(S, $/), S, Acc).

triples(0, S, Acc) ->
	[{root, l2b(S), l2b(S)}|Acc];

triples(I, S, Acc) ->
	S1 = string:substr(S, 1, I-1),
	S2 = string:substr(S, I+1),
	triples(S1, [{l2b(S1), l2b(S2), l2b(S)}|Acc]).

l2b(L) -> list_to_binary(L).

test() ->
    N = 100000,
    Topic = <<"/abkc/19383/192939/akakdkkdkak/xxxyyuya/akakak">>,
    {Time, _} = timer:tc(fun() -> 
                [triples(Topic) || _I <- lists:seq(1, N)]
        end),
    io:format("Time for triples: ~p(micro)", [Time/N]),
    ok.
