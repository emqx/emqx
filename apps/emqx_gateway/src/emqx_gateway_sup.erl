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

-module(emqx_gateway_sup).

-behaviour(supervisor).

-include("include/emqx_gateway.hrl").

-export([start_link/0]).

%% Gateway Instance APIs
-export([ create_gateway_insta/1
        , remove_gateway_insta/2
        , update_gateway_insta/2
        , start_gateway_insta/2
        , stop_gateway_insta/2
        , list_gateway_insta/1
        , list_gateway_insta/0
        ]).

%% Gateway APs
-export([ list_started_gateway/0
        , stop_all_suptree/1
        ]).

%% supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec create_gateway_insta(instance()) -> {ok, pid()} | {error, any()}.
create_gateway_insta(Insta = #instance{type = Type}) ->
    case emqx_gateway_registry:lookup(Type) of
        undefined -> {error, {unknown_type, Type}};
        GwDscrptr ->
            {ok, GwSup} = ensure_gateway_suptree_ready(gatewayid(Insta)),
            emqx_gateway_gw_sup:create_insta(GwSup, Insta, GwDscrptr)
    end.

-spec remove_gateway_insta(atom(), atom()) -> ok | {error, any()}.
remove_gateway_insta(InstaId, Type) ->
    case emqx_gateway_utils:find_sup_child(?MODULE, Type) of
        {ok, GwSup} ->
            emqx_gateway_gw_sup:remove_insta(GwSup, InstaId);
        _ -> ok
    end.

-spec update_gateway_insta(instance(), atom())
    -> ok
     | {error, any()}.
update_gateway_insta(NewInsta, Type) ->
    case emqx_gateway_utils:find_sup_child(?MODULE, Type) of
        {ok, GwSup} ->
            emqx_gateway_gw_sup:update_insta(GwSup, NewInsta);
        _ -> {error, not_found}
    end.

start_gateway_insta(_InstaId, _Type) ->
    todo.

stop_gateway_insta(InstaId, Type) ->
    case emqx_gateway_utils:find_sup_child(?MODULE, Type) of
        {ok, GwSup} ->
            emqx_gateway_gw_sup:stop_insta(GwSup, InstaId);
        _ -> {error, not_found}
    end.

-spec list_gateway_insta(Type :: atom()) -> {ok, [instance()]} | {error, any()}.
list_gateway_insta(Type) ->
    case emqx_gateway_utils:find_sup_child(?MODULE, Type) of
        {ok, GwSup} ->
            {ok, emqx_gateway_gw_sup:list_insta(GwSup)};
        _ -> {error, not_found}
    end.

-spec list_gateway_insta() -> [{atom(), instance()}].
list_gateway_insta() ->
    lists:map(
      fun(Type) ->
        {ok, Instas} = list_gateway_insta(Type),
        {Type, Instas}
      end, list_started_gateway()).

-spec list_started_gateway() -> [GwId :: atom()].
list_started_gateway() ->
    lists:filtermap(
      fun({Id, _Pid, _Type, _Mods}) ->
        is_a_gateway_id(Id) andalso {true, Id}
      end, supervisor:which_children(?MODULE)).

-spec stop_all_suptree(atom()) -> ok.
stop_all_suptree(GwId) ->
    case lists:keyfind(GwId, 1, supervisor:which_children(?MODULE)) of
        false -> ok;
        _ ->
            _ = supervisor:terminate_child(?MODULE, GwId),
            _ = supervisor:delete_child(?MODULE, GwId),
            ok
    end.

%% Supervisor callback

init([]) ->
    SupFlags = #{ strategy => one_for_one
                , intensity => 10
                , period => 60
                },
    ChildSpecs = [ emqx_gateway_utils:childspec(worker, emqx_gateway_registry)
                 ],
    {ok, {SupFlags, ChildSpecs}}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

gatewayid(#instance{type = Type}) ->
    list_to_atom(lists:concat(["gateway#", Type])).

is_a_gateway_id(Id) when is_atom(Id) ->
    is_a_gateway_id(atom_to_list(Id));
is_a_gateway_id("gateway#" ++ _) ->
    true;
is_a_gateway_id(Id) when is_list(Id) ->
    false.

ensure_gateway_suptree_ready(GwId) ->
    case lists:keyfind(GwId, 1, supervisor:which_children(?MODULE)) of
        false ->
            ChildSpec = emqx_gateway_utils:childspec(
                          GwId,
                          supervisor,
                          emqx_gateway_gw_sup,
                          [GwId]
                         ),
            emqx_gateway_utils:supervisor_ret(
              supervisor:start_child(?MODULE, ChildSpec)
             );
        {_Id, Pid, _Type, _Mods} ->
            {ok, Pid}
    end.
