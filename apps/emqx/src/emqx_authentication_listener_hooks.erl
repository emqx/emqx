%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authentication_listener_hooks).

-include_lib("emqx/include/emqx_hooks.hrl").

-export([
    on_listener_started/4,
    on_listener_stopped/4,
    on_listener_updated/4
]).

-export([
    load/0,
    unload/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

load() ->
    ok = emqx_hook:put('listener.started', {?MODULE, on_listener_started, []}, ?HP_AUTHN),
    ok = emqx_hook:put('listener.stopped', {?MODULE, on_listener_stopped, []}, ?HP_AUTHN),
    ok = emqx_hook:put('listener.updated', {?MODULE, on_listener_updated, []}, ?HP_AUTHN),
    ok.

unload() ->
    ok = emqx_hooks:del('listener.started', {?MODULE, authenticate, []}),
    ok = emqx_hooks:del('listener.stopped', {?MODULE, authenticate, []}),
    ok = emqx_hooks:del('listener.updated', {?MODULE, authenticate, []}),
    ok.

%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------

on_listener_started(Type, Name, Conf, ok) ->
    recreate_authenticators(Type, Name, Conf);
on_listener_started(_Type, _Name, _Conf, _Error) ->
    ok.

on_listener_updated(Type, Name, {_OldConf, NewConf}, ok) ->
    recreate_authenticators(Type, Name, NewConf);
on_listener_updated(_Type, _Name, _Conf, _Error) ->
    ok.

on_listener_stopped(Type, Name, _OldConf, ok) ->
    _ = emqx_authentication:delete_chain(emqx_listeners:listener_id(Type, Name)),
    ok;
on_listener_stopped(_Type, _Name, _Conf, _Error) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

recreate_authenticators(Type, Name, Conf) ->
    Chain = emqx_listeners:listener_id(Type, Name),
    _ = emqx_authentication:delete_chain(Chain),
    do_create_authneticators(Chain, maps:get(authentication, Conf, [])).

do_create_authneticators(Chain, [AuthN | T]) ->
    case emqx_authentication:create_authenticator(Chain, AuthN) of
        {ok, _} ->
            do_create_authneticators(Chain, T);
        Error ->
            _ = emqx_authentication:delete_chain(Chain),
            {ok, Error}
    end;
do_create_authneticators(_Chain, []) ->
    ok.
