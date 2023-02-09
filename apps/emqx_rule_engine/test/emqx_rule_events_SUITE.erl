%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_events_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_mod_hook_fun(_) ->
    Funcs = emqx_rule_events:module_info(exports),
    [?assert(lists:keymember(emqx_rule_events:hook_fun(Event), 1, Funcs)) ||
     Event <- ['client.connected',
               'client.disconnected',
               'session.subscribed',
               'session.unsubscribed',
               'message.acked',
               'message.dropped',
               'message.delivered'
              ]].

t_printable_maps(_) ->
    TestMap = #{
      peerhost => {127,0,0,1},
      peername => {{127,0,0,1}, 9980},
      sockname => {{127,0,0,1}, 1883},
      redispatch_to => {<<"group">>, <<"sub/topic/+">>},
      shared_dispatch_ack => {self(), ref},
      string => <<"abc">>,
      atom => abc,
      integer => 1,
      float => 1.0,
      simple_list => [1, 1.0, a, "abc", <<"abc">>, {a,b}]
    },
    Headers = TestMap#{
      map => TestMap,
      map_list => [
        TestMap#{
          map => TestMap
        }
      ]
    },
    Converted = emqx_rule_events:printable_maps(Headers),
    Verify = fun(Result) ->
      ?assertMatch(
          #{peerhost := <<"127.0.0.1">>,
            peername := <<"127.0.0.1:9980">>,
            sockname := <<"127.0.0.1:1883">>,
            string := <<"abc">>,
            atom := abc,
            integer := 1,
            float := 1.0,
            simple_list := [1, 1.0, a, "abc", <<"abc">>] %% {a,b} is removed
          }, Result),
      ?assertNot(maps:is_key(redispatch_to, Result)),
      ?assertNot(maps:is_key(shared_dispatch_ack, Result)),
      %% make sure the result is jsonable
      _ = emqx_json:encode(Result)
    end,
    Verify(maps:get(map, Converted)),
    Verify(maps:get(map, lists:nth(1, maps:get(map_list, Converted)))),
    Verify(Converted),
    ok.
