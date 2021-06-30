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
        , remove_gateway_insta/1
        , lookup_gateway_insta/1
        , update_gateway_insta/1
        , start_gateway_insta/1
        , stop_gateway_insta/1
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
create_gateway_insta(Insta = #{gwid := GwId}) ->
    case emqx_gateway_registry:lookup(GwId) of
        undefined -> {error, {unknown_gateway_id, GwId}};
        GwDscrptr ->
            {ok, GwSup} = ensure_gateway_suptree_ready(gatewayid(GwId)),
            emqx_gateway_gw_sup:create_insta(GwSup, Insta, GwDscrptr)
    end.

-spec remove_gateway_insta(instance_id()) -> ok | {error, any()}.
remove_gateway_insta(InstaId) ->
    case search_gateway_insta_proc(InstaId) of
        {ok, {GwSup, _}} ->
            emqx_gateway_gw_sup:remove_insta(GwSup, InstaId);
        _ ->
            ok
    end.

-spec lookup_gateway_insta(instance_id()) -> instance() | undefined.
lookup_gateway_insta(InstaId) ->
    case search_gateway_insta_proc(InstaId) of
        {ok, {_, GwInstaPid}} ->
            emqx_gateway_insta_sup:info(GwInstaPid);
        _ ->
            undefined
    end.

-spec update_gateway_insta(instance())
    -> ok
     | {error, any()}.
update_gateway_insta(NewInsta = #{gwid := GwId}) ->
    case emqx_gateway_utils:find_sup_child(?MODULE, gatewayid(GwId)) of
        {ok, GwSup} ->
            emqx_gateway_gw_sup:update_insta(GwSup, NewInsta);
        _ -> {error, not_found}
    end.

start_gateway_insta(InstaId) ->
    case search_gateway_insta_proc(InstaId) of
        {ok, {GwSup, _}} ->
            emqx_gateway_gw_sup:start_insta(GwSup, InstaId);
        _ -> {error, not_found}
    end.

-spec stop_gateway_insta(instance_id()) -> ok | {error, any()}.
stop_gateway_insta(InstaId) ->
    case search_gateway_insta_proc(InstaId) of
        {ok, {GwSup, _}} ->
            emqx_gateway_gw_sup:stop_insta(GwSup, InstaId);
        _ -> {error, not_found}
    end.

-spec list_gateway_insta(gateway_id()) -> {ok, [instance()]} | {error, any()}.
list_gateway_insta(GwId) ->
    case emqx_gateway_utils:find_sup_child(?MODULE, gatewayid(GwId)) of
        {ok, GwSup} ->
            {ok, emqx_gateway_gw_sup:list_insta(GwSup)};
        _ -> {error, not_found}
    end.

-spec list_gateway_insta() -> [{gateway_id(), instance()}].
list_gateway_insta() ->
    lists:map(
      fun(SupId) ->
        Instas = emqx_gateway_gw_sup:list_insta(SupId),
        {SupId, Instas}
      end, list_started_gateway()).

-spec list_started_gateway() -> [gateway_id()].
list_started_gateway() ->
    started_gateway_id().

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

gatewayid(GwId) ->
    list_to_atom(lists:concat([GwId])).

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
        {_Id, Pid, _GwId, _Mods} ->
            {ok, Pid}
    end.

search_gateway_insta_proc(InstaId) ->
    search_gateway_insta_proc(InstaId, started_gateway_pid()).

search_gateway_insta_proc(_InstaId, []) ->
    {error, not_found};
search_gateway_insta_proc(InstaId, [SupPid|More]) ->
    case emqx_gateway_utils:find_sup_child(SupPid, InstaId) of
        {ok, InstaPid} -> {ok, {SupPid, InstaPid}};
        _ ->
            search_gateway_insta_proc(InstaId, More)
    end.

started_gateway_id() ->
    lists:filtermap(
        fun({Id, _, _, _}) ->
            is_a_gateway_id(Id) andalso {true, Id}
        end, supervisor:which_children(?MODULE)).

started_gateway_pid() ->
    lists:filtermap(
        fun({Id, Pid, _, _}) ->
            is_a_gateway_id(Id) andalso {true, Pid}
        end, supervisor:which_children(?MODULE)).

is_a_gateway_id(Id) ->
    Id /= emqx_gateway_registry.


