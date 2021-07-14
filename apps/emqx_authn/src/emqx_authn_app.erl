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
-include_lib("emqx/include/logger.hrl").

-behaviour(application).

-emqx_plugin(?MODULE).

%% Application callbacks
-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_authn_sup:start_link(),
    ok = ekka_rlog:wait_for_shards([?AUTH_SHARD], infinity),
    initialize(),
    {ok, Sup}.

stop(_State) ->
    ok.

initialize() ->
    #{authenticators := Authenticators} = emqx_config:get([emqx_authn], #{authenticators => []}),
    initialize(Authenticators).

initialize(Authenticators) ->
    {ok, _} = emqx_authn:create_chain(#{id => ?CHAIN}),
    initialize_authenticators(Authenticators).

initialize_authenticators([]) ->
    ok;
initialize_authenticators([#{name := Name} = Authenticator | More]) ->
    case emqx_authn:create_authenticator(?CHAIN, Authenticator) of
        {ok, _} ->
            initialize_authenticators(More);
        {error, Reason} ->
            ?LOG(error, "Failed to create authenticator '~s': ~p", [Name, Reason])
    end.