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

-module(emqx_auth_mnesia_app).

-behaviour(application).

-emqx_plugin(auth).

-include("emqx_auth_mnesia.hrl").

%% Application callbacks
-export([ start/2
        , prep_stop/1
        , stop/1
        ]).

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_auth_mnesia_sup:start_link(),
    emqx_ctl:register_command('mqtt-user', {emqx_auth_mnesia_cli, auth_cli}, []),
    emqx_ctl:register_command('mqtt-acl', {emqx_auth_mnesia_cli, acl_cli}, []),
    load_auth_hook(),
    load_acl_hook(),
    {ok, Sup}.

prep_stop(State) ->
    emqx:unhook('client.authenticate', fun emqx_auth_mnesia:check/3),
    emqx:unhook('client.check_acl', fun emqx_acl_mnesia:check_acl/5),
    emqx_ctl:unregister_command('mqtt-user'),
    emqx_ctl:unregister_command('mqtt-acl'),
    State.

stop(_State) ->
    ok.

load_auth_hook() ->
    DefaultUsers = application:get_env(?APP, userlist, []),
    ok = emqx_auth_mnesia:init(DefaultUsers),
    ok = emqx_auth_mnesia:register_metrics(),
    Params = #{
            hash_type => application:get_env(emqx_auth_mnesia, hash_type, sha256),
            key_as => application:get_env(emqx_auth_mnesia, as, username)
            },
    emqx:hook('client.authenticate', fun emqx_auth_mnesia:check/3, [Params]).

load_acl_hook() ->
    ok = emqx_acl_mnesia:init(),
    ok = emqx_acl_mnesia:register_metrics(),
    Params = #{
            key_as => application:get_env(emqx_auth_mnesia, as, username)
            },
    emqx:hook('client.check_acl', fun emqx_acl_mnesia:check_acl/5, [Params]).

