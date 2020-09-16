%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_sup).

-behaviour(supervisor).

-export([ start_link/0
        , init/1
        ]).

-export([ start_driver_pool/1
        , stop_driver_pool/1
        ]).

%%--------------------------------------------------------------------
%%  Supervisor APIs & Callbacks
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 10, 100}, []}}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_driver_pool(map()) -> {ok, pid()} | {error, term()}.
start_driver_pool(Spec) ->
    supervisor:start_child(?MODULE, Spec).

-spec stop_driver_pool(atom()) -> ok.
stop_driver_pool(Name) ->
    ok = supervisor:terminate_child(?MODULE, Name),
    ok = supervisor:delete_child(?MODULE, Name).
