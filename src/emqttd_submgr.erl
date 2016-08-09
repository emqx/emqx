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

-module(emqttd_submgr).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_server2).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

%% API Exports
-export([start_link/3, add_subscriber/2, async_add_subscriber/2,
         del_subscriber/2, async_del_subscriber/2]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id, env}).

-spec(start_link(atom(), pos_integer(), [tuple()]) -> {ok, pid()} | ignore | {error, any()}).
start_link(Pool, Id, Env) ->
    gen_server2:start_link({local, ?PROC_NAME(?MODULE, Id)}, ?MODULE, [Pool, Id, Env], []).

-spec(add_subscriber(binary(), emqttd:subscriber()) -> ok).
add_subscriber(Topic, Subscriber) ->
    gen_server2:call(pick(Topic), {add_subscriber, Topic, Subscriber}, infinity).

-spec(async_add_subscriber(binary(), emqttd:subscriber()) -> ok).
async_add_subscriber(Topic, Subscriber) ->
    gen_server2:cast(pick(Topic), {add_subscriber, Topic, Subscriber}).

-spec(del_subscriber(binary(), emqttd:subscriber()) -> ok).
del_subscriber(Topic, Subscriber) ->
    gen_server2:call(pick(Topic), {del_subscriber, Topic, Subscriber}, infinity).

-spec(async_del_subscriber(binary(), emqttd:subscriber()) -> ok).
async_del_subscriber(Topic, Subscriber) ->
    gen_server2:cast(pick(Topic), {del_subscriber, Topic, Subscriber}).

pick(Topic) -> gproc_pool:pick_worker(dispatcher, Topic).

init([Pool, Id, Env]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id, env = Env}}.

handle_call({add_subscriber, Topic, Subscriber}, _From, State) ->
    add_subscriber_(Topic, Subscriber),
    {reply, ok, State};

handle_call({del_subscriber, Topic, Subscriber}, _From, State) ->
    del_subscriber_(Topic, Subscriber), 
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast({add_subscriber, Topic, Subscriber}, State) ->
    add_subscriber_(Topic, Subscriber),
    {reply, ok, State};

handle_cast({del_subscriber, Topic, Subscriber}, State) ->
    del_subscriber_(Topic, Subscriber), 
    {reply, ok, State};

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internel Functions
%%--------------------------------------------------------------------

add_subscriber_(Topic, Subscriber) ->
    case ets:member(subscriber, Topic) of
        false -> emqttd_router:add_route(Topic, node());
        true  -> ok
    end,
    ets:insert(subscriber, {Topic, Subscriber}).

del_subscriber_(Topic, Subscriber) ->
    ets:delete_object(subscriber, {Topic, Subscriber}),
    case ets:member(subscriber, Topic) of
        false -> emqttd_router:del_route(Topic, node());
        true  -> ok
    end.

