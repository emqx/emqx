%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_psk).

-include("logger.hrl").

%% API Functions
-export([init/0,
         add_handler/3,
         activate_handler/1,
         list_handlers/0]).

%% SSL PSK Callbacks
-export([lookup/3]).

-define(TAB, ?MODULE).

-type psk_identity() :: string().
-type psk_user_state() :: term().

-callback handle_lookup(psk_identity(), Args::term()) ->
    {ok, SharedSecret::binary()} | {error, Reason::term()}.

-spec init() -> ok.
init() ->
    ok = emqx_tables:new(?TAB, [bag, public, {read_concurrency,true}]).

-spec lookup(psk, psk_identity(), psk_user_state()) ->
      {ok, SharedSecret :: binary()} | error.
lookup(psk, ClientPSKID, _UserState) ->
    {Module, Args} = list_handlers(),
    try Module:handle_lookup(ClientPSKID, Args) of
        {ok, SharedSecret} -> {ok, SharedSecret};
        {error, Reason} ->
            ?LOG(error, "Lookup PSK failed, ~p: ~p", [Module, Reason]),
            error
    catch
        Except:Error ->
          ?LOG(error, "Lookup PSK failed, ~p: ~p", [Module, {Except,Error}]),
          error
    end.

-spec add_handler(Name::string(), Module::module(), Args::term()) -> ok | {error, Reason::term()}.
add_handler(Name, Module, Args) ->
    ets:insert(?TAB, {Name, Module, Args}), ok.

-spec activate_handler(Name::string()) -> ok | {error, Reason::term()}.
activate_handler(Name) ->
    ets:insert(?TAB, {active_handler, Name}).

-spec list_handlers() -> [{Name::string(), Module::module(), Args::term()}].
list_handlers() ->
    ets:lookup(?TAB, handler).