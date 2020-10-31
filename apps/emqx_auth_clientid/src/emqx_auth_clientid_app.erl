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

-module(emqx_auth_clientid_app).

-include("emqx_auth_clientid.hrl").

-behaviour(application).

-emqx_plugin(auth).

-export([ start/2
        , stop/1
        ]).

-behaviour(supervisor).

-export([init/1]).

start(_Type, _Args) ->
    emqx_ctl:register_command(clientid, {?APP, cli}, []),
    emqx_auth_clientid:register_metrics(),
    HashType = application:get_env(?APP, password_hash, sha256),
    Params = #{hash_type => HashType},
    emqx:hook('client.authenticate', fun emqx_auth_clientid:check/3, [Params]),
    DefaultIds = application:get_env(?APP, client_list, []),
    ok = emqx_auth_clientid:init(DefaultIds),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    emqx:unhook('client.authenticate', fun emqx_auth_clientid:check/3),
    emqx_ctl:unregister_command(clientid).

%%--------------------------------------------------------------------
%% Dummy supervisor
%%--------------------------------------------------------------------

init([]) ->
    {ok, { {one_for_all, 1, 10}, []} }.

