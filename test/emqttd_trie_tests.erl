%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2016, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------

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
