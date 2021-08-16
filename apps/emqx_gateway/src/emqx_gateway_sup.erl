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

%% Gateway APIs
-export([ load_gateway/1
        , unload_gateway/1
        , lookup_gateway/1
        , update_gateway/1
        , start_gateway_insta/1
        , stop_gateway_insta/1
        , list_gateway_insta/0
        ]).

%% supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


-spec load_gateway(gateway()) -> {ok, pid()} | {error, any()}.
load_gateway(Gateway = #{type := GwType}) ->
    case emqx_gateway_registry:lookup(GwType) of
        undefined -> {error, {unknown_gateway_type, GwType}};
        GwDscrptr ->
            {ok, GwSup} = ensure_gateway_suptree_ready(GwType),
            emqx_gateway_gw_sup:create_insta(GwSup, Gateway, GwDscrptr)
    end.

-spec unload_gateway(gateway_type()) -> ok | {error, not_found}.
unload_gateway(GwType) ->
    case lists:keyfind(GwType, 1, supervisor:which_children(?MODULE)) of
        false -> {error, not_found};
        _ ->
            _ = supervisor:terminate_child(?MODULE, GwType),
            _ = supervisor:delete_child(?MODULE, GwType),
            ok
    end.

-spec lookup_gateway(gateway_type()) -> gateway() | undefined.
lookup_gateway(GwType) ->
    case search_gateway_insta_proc(GwType) of
        {ok, {_, GwInstaPid}} ->
            emqx_gateway_insta_sup:info(GwInstaPid);
        _ ->
            undefined
    end.

-spec update_gateway(gateway())
    -> ok
     | {error, any()}.
update_gateway(NewGateway = #{type := GwType}) ->
    case emqx_gateway_utils:find_sup_child(?MODULE, GwType) of
        {ok, GwSup} ->
            emqx_gateway_gw_sup:update_insta(GwSup, NewGateway);
        _ -> {error, not_found}
    end.

start_gateway_insta(GwType) ->
    case search_gateway_insta_proc(GwType) of
        {ok, {GwSup, _}} ->
            emqx_gateway_gw_sup:start_insta(GwSup, GwType);
        _ -> {error, not_found}
    end.

-spec stop_gateway_insta(gateway_type()) -> ok | {error, any()}.
stop_gateway_insta(GwType) ->
    case search_gateway_insta_proc(GwType) of
        {ok, {GwSup, _}} ->
            emqx_gateway_gw_sup:stop_insta(GwSup, GwType);
        _ -> {error, not_found}
    end.

-spec list_gateway_insta() -> [gateway()].
list_gateway_insta() ->
    lists:append(lists:map(
      fun(SupId) ->
        emqx_gateway_gw_sup:list_insta(SupId)
      end, list_started_gateway())).

-spec list_started_gateway() -> [gateway_type()].
list_started_gateway() ->
    started_gateway_type().

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

ensure_gateway_suptree_ready(GwType) ->
    case lists:keyfind(GwType, 1, supervisor:which_children(?MODULE)) of
        false ->
            ChildSpec = emqx_gateway_utils:childspec(
                          GwType,
                          supervisor,
                          emqx_gateway_gw_sup,
                          [GwType]
                         ),
            emqx_gateway_utils:supervisor_ret(
              supervisor:start_child(?MODULE, ChildSpec)
             );
        {_Id, Pid, _Type, _Mods} ->
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

started_gateway_type() ->
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
