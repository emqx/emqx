%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_app).

-include("emqx_authn.hrl").

-behaviour(application).

%% Application callbacks
-export([ start/2
        , stop/1
        ]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    ok = mria_rlog:wait_for_shards([?AUTH_SHARD], infinity),
    {ok, Sup} = emqx_authn_sup:start_link(),
    ok = ?AUTHN:register_providers(providers()),
    ok = initialize(),
    {ok, Sup}.

stop(_State) ->
    ok = ?AUTHN:deregister_providers(provider_types()),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

initialize() ->
    ?AUTHN:initialize_authentication(?GLOBAL, emqx:get_raw_config([authentication], [])),
    lists:foreach(fun({ListenerID, ListenerConfig}) ->
                      ?AUTHN:initialize_authentication(ListenerID, maps:get(authentication, ListenerConfig, []))
                  end, emqx_listeners:list()).

provider_types() ->
    lists:map(fun({Type, _Module}) -> Type end, providers()).

providers() ->
    [ {{'password-based', 'built-in-database'}, emqx_authn_mnesia}
    , {{'password-based', mysql}, emqx_authn_mysql}
    , {{'password-based', postgresql}, emqx_authn_pgsql}
    , {{'password-based', mongodb}, emqx_authn_mongodb}
    , {{'password-based', redis}, emqx_authn_redis}
    , {{'password-based', 'http-server'}, emqx_authn_http}
    , {jwt, emqx_authn_jwt}
    , {{scram, 'built-in-database'}, emqx_enhanced_authn_scram_mnesia}
    ].
