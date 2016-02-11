%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%%% @doc PubSub Helper.
-module(emqttd_pubsub_helper).

-behaviour(gen_server).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

%% API Function Exports
-export([start_link/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {statsfun}).

-define(SERVER, ?MODULE).

%% @doc Start PubSub Helper.
-spec start_link(fun()) -> {ok, pid()} | ignore | {error, any()}.
start_link(StatsFun) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [StatsFun], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([StatsFun]) ->
    mnesia:subscribe(system),
    {ok, #state{statsfun = StatsFun}}.

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({mnesia_system_event, {mnesia_down, Node}}, State) ->
    %% TODO: mnesia master?
    Pattern = #mqtt_topic{_ = '_', node = Node},
    F = fun() ->
            [mnesia:delete_object(topic, R, write) ||
                R <- mnesia:match_object(topic, Pattern, write)]
        end,
    mnesia:transaction(F), noreply(State);

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, _State) ->
    mnesia:unsubscribe(system).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

noreply(State = #state{statsfun = StatsFun}) ->
    StatsFun(topic), {noreply, State}.

