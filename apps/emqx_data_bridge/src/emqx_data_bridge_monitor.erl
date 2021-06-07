%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%% This process monitors all the data bridges, and try to restart a bridge
%% when one of it stopped.
-module(emqx_data_bridge_monitor).

-behaviour(gen_server).

%% API functions
-export([ start_link/0
        , ensure_all_started/1
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

ensure_all_started(Configs) ->
    gen_server:cast(?MODULE, {start_and_monitor, Configs}).

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({start_and_monitor, Configs}, State) ->
    ok = load_bridges(Configs),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%============================================================================
load_bridges(Configs) ->
    lists:foreach(fun load_bridge/1, Configs).

load_bridge(#{<<"name">> := Name, <<"type">> := Type,
              <<"config">> := Config}) ->
    case emqx_resource:check_and_create_local(
            emqx_data_bridge:name_to_resource_id(Name),
            emqx_data_bridge:resource_type(Type), Config) of
        {ok, _} -> ok;
        {error, already_created} -> ok;
        {error, Reason} ->
            error({load_bridge, Reason})
    end.
