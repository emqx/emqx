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

-module(emqttd_node_tests).

-author("Feng Lee <feng@emqtt.io>").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

is_aliving_test() ->
    ?debugFmt("Node: ~p~n", [node()]),
    ?assert(emqttd_node:is_aliving(node())),
    ?assertNot(emqttd_node:is_aliving('x@127.0.0.1')).

parse_name_test() ->
    ?assertEqual('a@127.0.0.1', emqttd_node:parse_name("a@127.0.0.1")),
    ?assertEqual('b@127.0.0.1', emqttd_node:parse_name("b")).

-endif.

