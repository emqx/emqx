%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([ start_link/1
        , stop/0
        ]).

-export([ register_topic/2
        , unregister_topic/1
        ]).

-export([ lookup_topic/2
        , lookup_topic_id/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-ifdef(TEST).
-export([create_table/0]).
-endif.

-define(TAB, ?MODULE).

-record(state, {max_predef_topic_id = 0}).

-record(emqx_sn_registry, {key, value}).

%%-----------------------------------------------------------------------------

-spec(start_link(list()) -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link(PredefTopics) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [PredefTopics], []).

-spec(stop() -> ok).
stop() ->
    gen_server:stop(?MODULE, normal, infinity).

-spec(register_topic(binary(), binary()) -> integer() | {error, term()}).
register_topic(ClientId, TopicName) when is_binary(TopicName) ->
    case emqx_topic:wildcard(TopicName) of
        false ->
            gen_server:call(?MODULE, {register, ClientId, TopicName});
        %% TopicId: in case of “accepted” the value that will be used as topic
        %% id by the gateway when sending PUBLISH messages to the client (not
        %% relevant in case of subscriptions to a short topic name or to a topic
        %% name which contains wildcard characters)
        true  -> {error, wildcard_topic}
    end.

-spec(lookup_topic(binary(), pos_integer()) -> undefined | binary()).
lookup_topic(ClientId, TopicId) when is_integer(TopicId) ->
    case lookup_element(?TAB, {predef, TopicId}, 3) of
        undefined ->
            lookup_element(?TAB, {ClientId, TopicId}, 3);
        Topic -> Topic
    end.

-spec(lookup_topic_id(binary(), binary())
      -> undefined
       | pos_integer()
       | {predef, integer()}).
lookup_topic_id(ClientId, TopicName) when is_binary(TopicName) ->
    case lookup_element(?TAB, {predef, TopicName}, 3) of
        undefined ->
            lookup_element(?TAB, {ClientId, TopicName}, 3);
        TopicId ->
            {predef, TopicId}
    end.

%% @private
lookup_element(Tab, Key, Pos) ->
    try ets:lookup_element(Tab, Key, Pos) catch error:badarg -> undefined end.

-spec(unregister_topic(binary()) -> ok).
unregister_topic(ClientId) ->
    gen_server:call(?MODULE, {unregister, ClientId}).

%%-----------------------------------------------------------------------------

init([PredefTopics]) ->
    create_table(),
    %% {predef, TopicId}     -> TopicName
    %% {predef, TopicName}   -> TopicId
    %% {ClientId, TopicId}   -> TopicName
    %% {ClientId, TopicName} -> TopicId
    MaxPredefId = lists:foldl(
                    fun({TopicId, TopicName}, AccId) ->
                        mnesia:dirty_write(#emqx_sn_registry{key = {predef, TopicId},
                                                             value = TopicName}),
                        mnesia:dirty_write(#emqx_sn_registry{key = {predef, TopicName},
                                                             value = TopicId}),
                        if TopicId > AccId -> TopicId; true -> AccId end
                    end, 0, PredefTopics),
    {ok, #state{max_predef_topic_id = MaxPredefId}}.

create_table() ->
    %% Optimize storage
    StoreProps = [{ets, [{read_concurrency, true}]}],
    ok = ekka_mnesia:create_table(?MODULE, [
        {attributes, record_info(fields, emqx_sn_registry)},
        {ram_copies, [node()]},
        {storage_properties, StoreProps}]),
    ok = ekka_mnesia:copy_table(?MODULE, ram_copies).

handle_call({register, ClientId, TopicName}, _From,
            State = #state{max_predef_topic_id = PredefId}) ->
    case lookup_topic_id(ClientId, TopicName) of
        {predef, PredefTopicId}  when is_integer(PredefTopicId) ->
            {reply, PredefTopicId, State};
        TopicId when is_integer(TopicId) ->
            {reply, TopicId, State};
        undefined ->
            case next_topic_id(?TAB, PredefId, ClientId) of
                TopicId when TopicId >= 16#FFFF ->
                    {reply, {error, too_large}, State};
                TopicId ->
                    Fun = fun() ->
                        mnesia:write(#emqx_sn_registry{key = {ClientId, next_topic_id},
                                                            value = TopicId + 1}),
                        mnesia:write(#emqx_sn_registry{key = {ClientId, TopicName},
                                                            value = TopicId}),
                        mnesia:write(#emqx_sn_registry{key = {ClientId, TopicId},
                                                            value = TopicName})
                    end,
                    case mnesia:transaction(Fun) of
                        {atomic, ok} ->
                            {reply, TopicId, State};
                        {aborted, Error} ->
                            {reply, {error, Error}, State}
                    end
            end
    end;

handle_call({unregister, ClientId}, _From, State) ->
    Registry = mnesia:dirty_match_object({?TAB, {ClientId, '_'}, '_'}),
    lists:foreach(fun(R) -> mnesia:dirty_delete_object(R) end, Registry),
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
    case mnesia:dirty_read(Tab, {ClientId, next_topic_id}) of
        [#emqx_sn_registry{value = Id}] -> Id;
        []        -> PredefId + 1
    end.
