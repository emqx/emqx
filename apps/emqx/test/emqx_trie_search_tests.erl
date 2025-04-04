%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_trie_search_tests).

-include_lib("eunit/include/eunit.hrl").

-import(emqx_trie_search, [filter/1]).

filter_test_() ->
    [
        ?_assertEqual(
            [<<"sensor">>, '+', <<"metric">>, <<>>, '#'],
            filter(<<"sensor/+/metric//#">>)
        ),
        ?_assertEqual(
            false,
            filter(<<"sensor/1/metric//42">>)
        )
    ].

topic_validation_test_() ->
    NextF = fun(_) -> '$end_of_table' end,
    Call = fun(Topic) ->
        emqx_trie_search:match(Topic, NextF)
    end,
    [
        ?_assertError(badarg, Call(<<"+">>)),
        ?_assertError(badarg, Call(<<"#">>)),
        ?_assertError(badarg, Call(<<"a/+/b">>)),
        ?_assertError(badarg, Call(<<"a/b/#">>)),
        ?_assertEqual(false, Call(<<"a/b/b+">>)),
        ?_assertEqual(false, Call(<<"a/b/c#">>))
    ].
