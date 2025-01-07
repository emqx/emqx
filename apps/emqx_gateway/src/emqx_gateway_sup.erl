%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_gateway.hrl").

-export([start_link/0]).

%% Gateway APIs
-export([
    load_gateway/1,
    unload_gateway/1,
    lookup_gateway/1,
    update_gateway/2,
    start_gateway_insta/1,
    stop_gateway_insta/1,
    list_gateway_insta/0
]).

%% supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?GATEWAY_SUP_NAME}, ?MODULE, []).

-spec load_gateway(gateway()) -> {ok, pid()} | {error, any()}.
load_gateway(Gateway = #{name := GwName}) ->
    case emqx_gateway_registry:lookup(GwName) of
        undefined ->
            {error, {unknown_gateway_name, GwName}};
        GwDscrptr ->
            {ok, GwSup} = ensure_gateway_suptree_ready(GwName),
            emqx_gateway_gw_sup:create_insta(GwSup, Gateway, GwDscrptr)
    end.

-spec unload_gateway(gateway_name()) ->
    ok
    | {error, not_found}
    | {error, any()}.
unload_gateway(GwName) ->
    case lists:keyfind(GwName, 1, supervisor:which_children(?GATEWAY_SUP_NAME)) of
        false ->
            {error, not_found};
        {_Id, Pid, _Type, _Mods} ->
            _ = emqx_gateway_gw_sup:remove_insta(Pid, GwName),
            _ = supervisor:terminate_child(?GATEWAY_SUP_NAME, GwName),
            _ = supervisor:delete_child(?GATEWAY_SUP_NAME, GwName),
            ok
    end.

-spec lookup_gateway(gateway_name()) -> gateway() | undefined.
lookup_gateway(GwName) ->
    case search_gateway_insta_proc(GwName) of
        {ok, {_, GwInstaPid}} ->
            emqx_gateway_insta_sup:info(GwInstaPid);
        _ ->
            undefined
    end.

-spec update_gateway(gateway_name(), emqx_config:config()) ->
    ok
    | {error, any()}.
update_gateway(GwName, Config) ->
    case emqx_gateway_utils:find_sup_child(?GATEWAY_SUP_NAME, GwName) of
        {ok, GwSup} ->
            emqx_gateway_gw_sup:update_insta(GwSup, GwName, Config);
        _ ->
            {error, not_found}
    end.

start_gateway_insta(GwName) ->
    case search_gateway_insta_proc(GwName) of
        {ok, {GwSup, _}} ->
            emqx_gateway_gw_sup:start_insta(GwSup, GwName);
        _ ->
            {error, not_found}
    end.

-spec stop_gateway_insta(gateway_name()) -> ok | {error, any()}.
stop_gateway_insta(GwName) ->
    case search_gateway_insta_proc(GwName) of
        {ok, {GwSup, _}} ->
            emqx_gateway_gw_sup:stop_insta(GwSup, GwName);
        _ ->
            {error, not_found}
    end.

-spec list_gateway_insta() -> [gateway()].
list_gateway_insta() ->
    lists:append(
        lists:map(
            fun(SupId) ->
                emqx_gateway_gw_sup:list_insta(SupId)
            end,
            list_started_gateway()
        )
    ).

-spec list_started_gateway() -> [gateway_name()].
list_started_gateway() ->
    started_gateway().

%% Supervisor callback

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    ChildSpecs = [emqx_gateway_utils:childspec(worker, emqx_gateway_registry)],
    {ok, {SupFlags, ChildSpecs}}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

ensure_gateway_suptree_ready(GwName) ->
    case lists:keyfind(GwName, 1, supervisor:which_children(?GATEWAY_SUP_NAME)) of
        false ->
            ChildSpec = emqx_gateway_utils:childspec(
                GwName,
                supervisor,
                emqx_gateway_gw_sup,
                [GwName]
            ),
            emqx_gateway_utils:supervisor_ret(
                supervisor:start_child(?GATEWAY_SUP_NAME, ChildSpec)
            );
        {_Id, Pid, _Type, _Mods} ->
            {ok, Pid}
    end.

search_gateway_insta_proc(InstaId) ->
    search_gateway_insta_proc(InstaId, started_gateway_pid()).

search_gateway_insta_proc(_InstaId, []) ->
    {error, not_found};
search_gateway_insta_proc(InstaId, [SupPid | More]) ->
    case emqx_gateway_utils:find_sup_child(SupPid, InstaId) of
        {ok, InstaPid} -> {ok, {SupPid, InstaPid}};
        _ -> search_gateway_insta_proc(InstaId, More)
    end.

started_gateway() ->
    lists:filtermap(
        fun({Id, _, _, _}) ->
            is_a_gateway_id(Id) andalso {true, Id}
        end,
        supervisor:which_children(?GATEWAY_SUP_NAME)
    ).

started_gateway_pid() ->
    lists:filtermap(
        fun({Id, Pid, _, _}) ->
            is_a_gateway_id(Id) andalso {true, Pid}
        end,
        supervisor:which_children(?GATEWAY_SUP_NAME)
    ).

is_a_gateway_id(Id) ->
    Id /= emqx_gateway_registry.
