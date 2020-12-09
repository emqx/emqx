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

-module(emqx_auth_jwt_app).

-behaviour(application).

-behaviour(supervisor).

-emqx_plugin(auth).

-export([start/2, stop/1]).

-export([init/1]).

-define(APP, emqx_auth_jwt).

start(_Type, _Args) ->
    {ok, Sup} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),

    {ok, Pid} = start_auth_server(jwks_svr_options()),
    ok = emqx_auth_jwt:register_metrics(),

    AuthEnv0 = auth_env(),
    AuthEnv1 = AuthEnv0#{pid => Pid},

    emqx:hook('client.authenticate', {emqx_auth_jwt, check, [AuthEnv1]}),
    {ok, Sup, AuthEnv1}.

stop(AuthEnv) ->
    emqx:unhook('client.authenticate', {emqx_auth_jwt, check, [AuthEnv]}).

%%--------------------------------------------------------------------
%% Dummy supervisor
%%--------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 1, 10}, []}}.

start_auth_server(Options) ->
    Spec = #{id => jwt_svr,
             start => {emqx_auth_jwt_svr, start_link, [Options]},
             restart => permanent,
             shutdown => brutal_kill,
             type => worker,
             modules => [emqx_auth_jwt_svr]},
    supervisor:start_child(?MODULE, Spec).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

auth_env() ->
    Checklists = [{atom_to_binary(K, utf8), V}
                  || {K, V} <- env(verify_claims, [])],
    #{ from => env(from, password)
     , checklists => Checklists
     }.

jwks_svr_options() ->
    [{K, V} || {K, V}
             <- [{secret, env(secret, undefined)},
                 {pubkey, env(pubkey, undefined)},
                 {jwks_addr, env(jwks, undefined)},
                 {interval, env(refresh_interval, undefined)}],
             V /= undefined].

env(Key, Default) ->
    application:get_env(?APP, Key, Default).
