%%--------------------------------------------------------------------
%% Copyright (c) 2016 Feng Lee <feng@emqtt.io>. All Rights Reserved.
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

-module(emqttd_trie_tests).

-include("emqttd.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

match_test() ->
    mnesia:start(),
    emqttd_trie:mnesia(boot),
    Topics = [<<"d/#">>, <<"a/b/c">>, <<"a/b/+">>, <<"a/#">>, <<"#">>, <<"$SYS/#">>],
    mnesia:transaction(fun() -> [emqttd_trie:insert(Topic) || Topic <- Topics] end),
    Matched = mnesia:async_dirty(fun emqttd_trie:match/1, [<<"a/b/c">>]),
    ?assertEqual(4, length(Matched)),
    SysMatched = mnesia:async_dirty(fun emqttd_trie:match/1, [<<"$SYS/a/b/c">>]),
    ?assertEqual([<<"$SYS/#">>], SysMatched).

-endif.
