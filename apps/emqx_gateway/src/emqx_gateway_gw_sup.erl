%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The Gateway Top supervisor.
%%
%%  This supervisor has monitor a bunch of process/resources depended by
%%  gateway runtime
%%
-module(emqx_gateway_gw_sup).

-behaviour(supervisor).

-include("include/emqx_gateway.hrl").

-export([start_link/1]).

-export([
    create_insta/3,
    remove_insta/2,
    update_insta/3,
    start_insta/2,
    stop_insta/2,
    list_insta/1
]).

%% Supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(GwName) ->
    supervisor:start_link({local, GwName}, ?MODULE, [GwName]).

-spec create_insta(pid(), gateway(), map()) -> {ok, GwInstaPid :: pid()} | {error, any()}.
create_insta(Sup, Gateway = #{name := Name}, GwDscrptr) ->
    case emqx_gateway_utils:find_sup_child(Sup, Name) of
        {ok, _GwInstaPid} ->
            {error, alredy_existed};
        false ->
            Ctx = ctx(Sup, Name),
            ChildSpec = emqx_gateway_utils:childspec(
                Name,
                worker,
                emqx_gateway_insta_sup,
                [Gateway, Ctx, GwDscrptr]
            ),
            emqx_gateway_utils:supervisor_ret(
                supervisor:start_child(Sup, ChildSpec)
            )
    end.

-spec remove_insta(pid(), Name :: gateway_name()) -> ok | {error, any()}.
remove_insta(Sup, Name) ->
    case emqx_gateway_utils:find_sup_child(Sup, Name) of
        false ->
            ok;
        {ok, _GwInstaPid} ->
            ok = supervisor:terminate_child(Sup, Name),
            ok = supervisor:delete_child(Sup, Name)
    end.

-spec update_insta(pid(), gateway_name(), emqx_config:config()) ->
    ok | {error, any()}.
update_insta(Sup, Name, Config) ->
    case emqx_gateway_utils:find_sup_child(Sup, Name) of
        false -> {error, not_found};
        {ok, GwInstaPid} -> emqx_gateway_insta_sup:update(GwInstaPid, Config)
    end.

-spec start_insta(pid(), gateway_name()) -> ok | {error, any()}.
start_insta(Sup, Name) ->
    case emqx_gateway_utils:find_sup_child(Sup, Name) of
        false -> {error, not_found};
        {ok, GwInstaPid} -> emqx_gateway_insta_sup:enable(GwInstaPid)
    end.

-spec stop_insta(pid(), gateway_name()) -> ok | {error, any()}.
stop_insta(Sup, Name) ->
    case emqx_gateway_utils:find_sup_child(Sup, Name) of
        false -> {error, not_found};
        {ok, GwInstaPid} -> emqx_gateway_insta_sup:disable(GwInstaPid)
    end.

-spec list_insta(pid()) -> [gateway()].
list_insta(Sup) ->
    lists:filtermap(
        fun({Name, GwInstaPid, _Type, _Mods}) ->
            is_gateway_insta_id(Name) andalso
                {true, emqx_gateway_insta_sup:info(GwInstaPid)}
        end,
        supervisor:which_children(Sup)
    ).

%% Supervisor callback

%% @doc Initialize Top Supervisor for a Protocol
init([GwName]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    CmOpts = [{gwname, GwName}],
    CM = emqx_gateway_utils:childspec(worker, emqx_gateway_cm, [CmOpts]),
    Metrics = emqx_gateway_utils:childspec(worker, emqx_gateway_metrics, [GwName]),
    {ok, {SupFlags, [CM, Metrics]}}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

ctx(Sup, Name) ->
    {ok, CM} = emqx_gateway_utils:find_sup_child(Sup, emqx_gateway_cm),
    #{
        gwname => Name,
        cm => CM
    }.

is_gateway_insta_id(emqx_gateway_cm) ->
    false;
is_gateway_insta_id(emqx_gateway_metrics) ->
    false;
is_gateway_insta_id(_Id) ->
    true.
