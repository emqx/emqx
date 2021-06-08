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
-module(emqx_data_bridge_config_handler).

-behaviour(gen_server).

%% API functions
-export([ start_link/0
        , notify_updated/0
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

%% TODO: trigger the `updated` message from emqx_resource.
notify_updated() ->
    gen_server:cast(?MODULE, updated).

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% TODO: change the config handler as a behavoir that calls back the Mod:format_config/1
handle_cast(updated, State) ->
    Configs = [format_conf(Data) || Data <- emqx_data_bridge:list_bridges()],
    emqx_config_handler ! {emqx_data_bridge, Configs},
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

format_conf(#{resource_type := Type, id := Id, config := Conf}) ->
    #{type => Type, name => emqx_data_bridge:resource_id_to_name(Id),
      config => Conf}.
