%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_eviction_agent_test_helpers).

-export([emqtt_connect/0,
         emqtt_connect/2,
         emqtt_connect_many/1,
         emqtt_try_connect/0]).

emqtt_connect() ->
    emqtt_connect(<<"client1">>, true).

emqtt_connect(ClientId, CleanStart) ->
    {ok, C} = emqtt:start_link(
                [{clientid, ClientId},
                 {clean_start, CleanStart},
                 {proto_ver, v5},
                 {properties, #{'Session-Expiry-Interval' => 600}}
                ]),
    case emqtt:connect(C) of
        {ok, _} -> {ok, C};
        {error, _} = Error -> Error
    end.

emqtt_connect_many(Count) ->
    lists:map(
      fun(N) ->
              NBin = integer_to_binary(N),
              ClientId = <<"client-", NBin/binary>>,
              {ok, C} = emqtt_connect(ClientId, false),
              C
      end,
      lists:seq(1, Count)).

emqtt_try_connect() ->
    case emqtt_connect() of
        {ok, C} ->
            emqtt:disconnect(C),
            ok;
        {error, _} = Error -> Error
    end.
