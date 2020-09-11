%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sn_registry).

-behaviour(gen_server).

-include("emqx_sn.hrl").

-define(LOG(Level, Format, Args),
        emqx_logger:Level("MQTT-SN(registry): " ++ Format, Args)).

-export([ start_link/2
        , stop/1
        ]).

-export([ register_topic/3
        , unregister_topic/2
        ]).

-export([ lookup_topic/3
        , lookup_topic_id/3
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(TAB, ?MODULE).

-record(state, {tab, max_predef_topic_id = 0}).

-type(registry() :: {ets:tab(), pid()}).

%%-----------------------------------------------------------------------------

-spec(start_link(atom(), list()) -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link(Tab, PredefTopics) ->
    gen_server:start_link(?MODULE, [Tab, PredefTopics], []).

-spec(stop(registry()) -> ok).
stop({_Tab, Pid}) ->
    gen_server:stop(Pid, normal, infinity).

-spec(register_topic(registry(), binary(), binary()) -> integer() | {error, term()}).
register_topic({_, Pid}, ClientId, TopicName) when is_binary(TopicName) ->
    case emqx_topic:wildcard(TopicName) of
        false ->
            gen_server:call(Pid, {register, ClientId, TopicName});
        %% TopicId: in case of “accepted” the value that will be used as topic
        %% id by the gateway when sending PUBLISH messages to the client (not
        %% relevant in case of subscriptions to a short topic name or to a topic
        %% name which contains wildcard characters)
        true  -> {error, wildcard_topic}
    end.

-spec(lookup_topic(registry(), binary(), pos_integer()) -> undefined | binary()).
lookup_topic({Tab, _Pid}, ClientId, TopicId) when is_integer(TopicId) ->
    case lookup_element(Tab, {predef, TopicId}, 2) of
        undefined ->
            lookup_element(Tab, {ClientId, TopicId}, 2);
        Topic -> Topic
    end.

-spec(lookup_topic_id(registry(), binary(), binary())
      -> undefined
       | pos_integer()
       | {predef, integer()}).
lookup_topic_id({Tab, _Pid}, ClientId, TopicName) when is_binary(TopicName) ->
    case lookup_element(Tab, {predef, TopicName}, 2) of
        undefined ->
            lookup_element(Tab, {ClientId, TopicName}, 2);
        TopicId ->
            {predef, TopicId}
    end.

%% @private
lookup_element(Tab, Key, Pos) ->
    try ets:lookup_element(Tab, Key, Pos) catch error:badarg -> undefined end.

-spec(unregister_topic(registry(), binary()) -> ok).
unregister_topic({_Tab, Pid}, ClientId) ->
    gen_server:call(Pid, {unregister, ClientId}).

%%-----------------------------------------------------------------------------

init([Tab, PredefTopics]) ->
    %% {predef, TopicId}     -> TopicName
    %% {predef, TopicName}   -> TopicId
    %% {ClientId, TopicId}   -> TopicName
    %% {ClientId, TopicName} -> TopicId
    _ = ets:new(Tab, [set, public, named_table, {read_concurrency, true}]),
    MaxPredefId = lists:foldl(
                    fun({TopicId, TopicName}, AccId) ->
                        _ = ets:insert(Tab, {{predef, TopicId}, TopicName}),
                        _ = ets:insert(Tab, {{predef, TopicName}, TopicId}),
                        if TopicId > AccId -> TopicId; true -> AccId end
                    end, 0, PredefTopics),
    {ok, #state{tab = Tab, max_predef_topic_id = MaxPredefId}}.

handle_call({register, ClientId, TopicName}, _From,
            State = #state{tab = Tab, max_predef_topic_id = PredefId}) ->
    case lookup_topic_id({Tab, self()}, ClientId, TopicName) of
        {predef, PredefTopicId}  when is_integer(PredefTopicId) ->
            {reply, PredefTopicId, State};
        TopicId when is_integer(TopicId) ->
            {reply, TopicId, State};
        undefined ->
            case next_topic_id(Tab, PredefId, ClientId) of
                TopicId when TopicId >= 16#FFFF ->
                    {reply, {error, too_large}, State};
                TopicId ->
                    _ = ets:insert(Tab, {{ClientId, next_topic_id}, TopicId + 1}),
                    _ = ets:insert(Tab, {{ClientId, TopicName}, TopicId}),
                    _ = ets:insert(Tab, {{ClientId, TopicId}, TopicName}),
                    {reply, TopicId, State}
            end
    end;

handle_call({unregister, ClientId}, _From, State = #state{tab = Tab}) ->
    ets:match_delete(Tab, {{ClientId, '_'}, '_'}),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected request: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-----------------------------------------------------------------------------

next_topic_id(Tab, PredefId, ClientId) ->
    case ets:lookup(Tab, {ClientId, next_topic_id}) of
        [{_, Id}] -> Id;
        []        -> PredefId + 1
    end.
