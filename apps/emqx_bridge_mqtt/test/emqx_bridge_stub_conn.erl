%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_stub_conn).

-behaviour(emqx_bridge_connect).

%% behaviour callbacks
-export([ start/1
        , send/2
        , stop/1
        ]).

-type ack_ref() :: emqx_bridge_worker:ack_ref().
-type batch() :: emqx_bridge_worker:batch().

start(#{client_pid := Pid} = Cfg) ->
    Pid ! {self(), ?MODULE, ready},
    {ok, Cfg}.

stop(_) -> ok.

%% @doc Callback for `emqx_bridge_connect' behaviour
-spec send(_, batch()) -> {ok, ack_ref()} | {error, any()}.
send(#{client_pid := Pid}, Batch) ->
    Ref = make_ref(),
    Pid ! {stub_message, self(), Ref, Batch},
    {ok, Ref}.
