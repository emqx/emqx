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

%% @doc The Gateway Top supervisor.
-module(emqx_gateway_gw_sup).

-behaviour(supervisor).

-include("include/emqx_gateway.hrl").

-export([start_link/1]).

-export([ create_insta/3
        , remove_insta/2
        , update_insta/2
        , start_insta/2
        , stop_insta/2
        , list_insta/1
        ]).

%% Supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(GwId) ->
    supervisor:start_link({local, GwId}, ?MODULE, [GwId]).

-spec create_insta(pid(), instance(), map()) -> {ok, GwInstaPid :: pid()} | {error, any()}.
create_insta(Sup, Insta = #{id := InstaId}, GwDscrptr) ->
    case emqx_gateway_utils:find_sup_child(Sup, InstaId) of
        {ok, _GwInstaPid} -> {error, alredy_existed};
        false ->
            %% XXX: More instances options to it?
            %%
            Ctx = ctx(Sup, InstaId),
            %%
            ChildSpec = emqx_gateway_utils:childspec(
                          InstaId,
                          worker,
                          emqx_gateway_insta_sup,
                          [Insta, Ctx, GwDscrptr]
                         ),
            emqx_gateway_utils:supervisor_ret(
              supervisor:start_child(Sup, ChildSpec)
             )
    end.

-spec remove_insta(pid(), InstaId :: atom()) -> ok | {error, any()}.
remove_insta(Sup, InstaId) ->
    case emqx_gateway_utils:find_sup_child(Sup, InstaId) of
        false -> ok;
        {ok, _GwInstaPid} ->
            %% TODO: ???
            %%ok = emqx_gateway_insta_sup:stop(GwInstaPid),
            ok = supervisor:terminate_child(Sup, InstaId),
            ok = supervisor:delete_child(Sup, InstaId)
    end.

-spec update_insta(pid(), NewInsta :: instance()) -> ok | {error, any()}.
update_insta(Sup, NewInsta = #{id := InstaId}) ->
    case emqx_gateway_utils:find_sup_child(Sup, InstaId) of
        false -> {error, not_found};
        {ok, GwInstaPid} ->
            emqx_gateway_insta_sup:update(GwInstaPid, NewInsta)
    end.

-spec start_insta(pid(), atom()) -> ok | {error, any()}.
start_insta(Sup, InstaId) ->
    case emqx_gateway_utils:find_sup_child(Sup, InstaId) of
        false -> {error, not_found};
        {ok, GwInstaPid} ->
            emqx_gateway_insta_sup:enable(GwInstaPid)
    end.

-spec stop_insta(pid(), atom()) -> ok | {error, any()}.
stop_insta(Sup, InstaId) ->
    case emqx_gateway_utils:find_sup_child(Sup, InstaId) of
        false -> {error, not_found};
        {ok, GwInstaPid} ->
            emqx_gateway_insta_sup:disable(GwInstaPid)
    end.

-spec list_insta(pid()) -> [instance()].
list_insta(Sup) ->
    lists:filtermap(
      fun({InstaId, GwInstaPid, _Type, _Mods}) ->
        is_gateway_insta_id(InstaId)
          andalso {true, emqx_gateway_insta_sup:info(GwInstaPid)}
      end, supervisor:which_children(Sup)).

%% Supervisor callback

%% @doc Initialize Top Supervisor for a Protocol
init([GwId]) ->
    SupFlags = #{ strategy => one_for_one
                , intensity => 10
                , period => 60
                },
    CmOpts = [{gwid, GwId}],
    CM = emqx_gateway_utils:childspec(worker, emqx_gateway_cm, [CmOpts]),
    Metrics = emqx_gateway_utils:childspec(worker, emqx_gateway_metrics, [GwId]),
    {ok, {SupFlags, [CM, Metrics]}}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

ctx(Sup, InstaId) ->
    {_, GwId} = erlang:process_info(Sup, registered_name),
    {ok, CM}  = emqx_gateway_utils:find_sup_child(Sup, emqx_gateway_cm),
    #{ instid => InstaId
     , gwid => GwId
     , cm => CM
     }.

is_gateway_insta_id(emqx_gateway_cm) ->
    false;
is_gateway_insta_id(_Id) ->
    true.
