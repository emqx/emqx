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
%%
%%  This supervisor has monitor a bunch of process/resources depended by
%%  gateway runtime
%%
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

start_link(Type) ->
    supervisor:start_link({local, Type}, ?MODULE, [Type]).

-spec create_insta(pid(), gateway(), map()) -> {ok, GwInstaPid :: pid()} | {error, any()}.
create_insta(Sup, Gateway = #{type := GwType}, GwDscrptr) ->
    case emqx_gateway_utils:find_sup_child(Sup, GwType) of
        {ok, _GwInstaPid} -> {error, alredy_existed};
        false ->
            Ctx = ctx(Sup, GwType),
            %%
            ChildSpec = emqx_gateway_utils:childspec(
                          GwType,
                          worker,
                          emqx_gateway_insta_sup,
                          [Gateway, Ctx, GwDscrptr]
                         ),
            emqx_gateway_utils:supervisor_ret(
              supervisor:start_child(Sup, ChildSpec)
             )
    end.

-spec remove_insta(pid(), GwType :: gateway_type()) -> ok | {error, any()}.
remove_insta(Sup, GwType) ->
    case emqx_gateway_utils:find_sup_child(Sup, GwType) of
        false -> ok;
        {ok, _GwInstaPid} ->
            ok = supervisor:terminate_child(Sup, GwType),
            ok = supervisor:delete_child(Sup, GwType)
    end.

-spec update_insta(pid(), NewGateway :: gateway()) -> ok | {error, any()}.
update_insta(Sup, NewGateway = #{type := GwType}) ->
    case emqx_gateway_utils:find_sup_child(Sup, GwType) of
        false -> {error, not_found};
        {ok, GwInstaPid} ->
            emqx_gateway_insta_sup:update(GwInstaPid, NewGateway)
    end.

-spec start_insta(pid(), gateway_type()) -> ok | {error, any()}.
start_insta(Sup, GwType) ->
    case emqx_gateway_utils:find_sup_child(Sup, GwType) of
        false -> {error, not_found};
        {ok, GwInstaPid} ->
            emqx_gateway_insta_sup:enable(GwInstaPid)
    end.

-spec stop_insta(pid(), gateway_type()) -> ok | {error, any()}.
stop_insta(Sup, GwType) ->
    case emqx_gateway_utils:find_sup_child(Sup, GwType) of
        false -> {error, not_found};
        {ok, GwInstaPid} ->
            emqx_gateway_insta_sup:disable(GwInstaPid)
    end.

-spec list_insta(pid()) -> [gateway()].
list_insta(Sup) ->
    lists:filtermap(
      fun({GwType, GwInstaPid, _Type, _Mods}) ->
        is_gateway_insta_id(GwType)
          andalso {true, emqx_gateway_insta_sup:info(GwInstaPid)}
      end, supervisor:which_children(Sup)).

%% Supervisor callback

%% @doc Initialize Top Supervisor for a Protocol
init([Type]) ->
    SupFlags = #{ strategy => one_for_one
                , intensity => 10
                , period => 60
                },
    CmOpts = [{type, Type}],
    CM = emqx_gateway_utils:childspec(worker, emqx_gateway_cm, [CmOpts]),
    Metrics = emqx_gateway_utils:childspec(worker, emqx_gateway_metrics, [Type]),
    {ok, {SupFlags, [CM, Metrics]}}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

ctx(Sup, GwType) ->
    {_, Type} = erlang:process_info(Sup, registered_name),
    {ok, CM}  = emqx_gateway_utils:find_sup_child(Sup, emqx_gateway_cm),
    #{ instid => GwType
     , type => Type
     , cm => CM
     }.

is_gateway_insta_id(emqx_gateway_cm) ->
    false;
is_gateway_insta_id(emqx_gateway_metrics) ->
    false;
is_gateway_insta_id(_Id) ->
    true.
