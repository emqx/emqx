%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_trie_search_tests).

-include_lib("eunit/include/eunit.hrl").

topic_validation_test() ->
    NextF = fun(_) -> '$end_of_table' end,
    Call = fun(Topic) ->
        emqx_trie_search:match(Topic, NextF)
    end,
    ?assertError(badarg, Call(<<"+">>)),
    ?assertError(badarg, Call(<<"#">>)),
    ?assertError(badarg, Call(<<"a/+/b">>)),
    ?assertError(badarg, Call(<<"a/b/#">>)),
    ?assertEqual(false, Call(<<"a/b/b+">>)),
    ?assertEqual(false, Call(<<"a/b/c#">>)),
    ok.
