%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The MQTT-SN Topic Registry
%%
%% XXX:
-module(emqx_sn_registry).

-behaviour(gen_server).

-include("src/mqttsn/include/emqx_sn.hrl").
-include_lib("emqx/include/logger.hrl").

-export([start_link/2]).

-export([
    register_topic/3,
    unregister_topic/2
]).

-export([
    lookup_topic/3,
    lookup_topic_id/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([lookup_name/1]).

-define(SN_SHARD, emqx_sn_shard).

-record(state, {tabname, max_predef_topic_id = 0}).

-record(emqx_sn_registry, {key, value}).

-type registry() :: {Tab :: atom(), RegistryPid :: pid()}.

%%-----------------------------------------------------------------------------

-spec start_link(atom(), list()) ->
    ignore
    | {ok, pid()}
    | {error, Reason :: term()}.
start_link(InstaId, PredefTopics) ->
    gen_server:start_link(?MODULE, [InstaId, PredefTopics], []).

-spec register_topic(registry(), emqx_types:clientid(), emqx_types:topic()) ->
    integer()
    | {error, term()}.
register_topic({_, Pid}, ClientId, TopicName) when is_binary(TopicName) ->
    case emqx_topic:wildcard(TopicName) of
        false ->
            gen_server:call(Pid, {register, ClientId, TopicName});
        %% TopicId: in case of “accepted” the value that will be used as topic
        %% id by the gateway when sending PUBLISH messages to the client (not
        %% relevant in case of subscriptions to a short topic name or to a topic
        %% name which contains wildcard characters)
        true ->
            {error, wildcard_topic}
    end.

-spec lookup_topic(registry(), emqx_types:clientid(), pos_integer()) ->
    undefined
    | binary().
lookup_topic({Tab, _}, ClientId, TopicId) when is_integer(TopicId) ->
    case lookup_element(Tab, {predef, TopicId}, 3) of
        undefined ->
            lookup_element(Tab, {ClientId, TopicId}, 3);
        Topic ->
            Topic
    end.

-spec lookup_topic_id(registry(), emqx_types:clientid(), emqx_types:topic()) ->
    undefined
    | pos_integer()
    | {predef, integer()}.
lookup_topic_id({Tab, _}, ClientId, TopicName) when is_binary(TopicName) ->
    case lookup_element(Tab, {predef, TopicName}, 3) of
        undefined ->
            lookup_element(Tab, {ClientId, TopicName}, 3);
        TopicId ->
            {predef, TopicId}
    end.

%% @private
lookup_element(Tab, Key, Pos) ->
    try
        ets:lookup_element(Tab, Key, Pos)
    catch
        error:badarg -> undefined
    end.

-spec unregister_topic(registry(), emqx_types:clientid()) -> ok.
unregister_topic({_, Pid}, ClientId) ->
    gen_server:call(Pid, {unregister, ClientId}).

lookup_name(Pid) ->
    gen_server:call(Pid, name).

%%-----------------------------------------------------------------------------

name(InstaId) ->
    list_to_atom(lists:concat([emqx_sn_, InstaId, '_registry'])).

init([InstaId, PredefTopics]) ->
    %% {predef, TopicId}     -> TopicName
    %% {predef, TopicName}   -> TopicId
    %% {ClientId, TopicId}   -> TopicName
    %% {ClientId, TopicName} -> TopicId
    Tab = name(InstaId),
    ok = mria:create_table(Tab, [
        {storage, ram_copies},
        {record_name, emqx_sn_registry},
        {attributes, record_info(fields, emqx_sn_registry)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]},
        {rlog_shard, ?SN_SHARD}
    ]),
    ok = mria:wait_for_tables([Tab]),
    MaxPredefId = lists:foldl(
        fun(#{id := TopicId, topic := TopicName0}, AccId) ->
            TopicName = iolist_to_binary(TopicName0),
            mria:dirty_write(Tab, #emqx_sn_registry{
                key = {predef, TopicId},
                value = TopicName
            }),
            mria:dirty_write(Tab, #emqx_sn_registry{
                key = {predef, TopicName},
                value = TopicId
            }),
            if
                TopicId > AccId -> TopicId;
                true -> AccId
            end
        end,
        0,
        PredefTopics
    ),
    {ok, #state{tabname = Tab, max_predef_topic_id = MaxPredefId}}.

handle_call(
    {register, ClientId, TopicName},
    _From,
    State = #state{tabname = Tab, max_predef_topic_id = PredefId}
) ->
    case lookup_topic_id({Tab, self()}, ClientId, TopicName) of
        {predef, PredefTopicId} when is_integer(PredefTopicId) ->
            {reply, PredefTopicId, State};
        TopicId when is_integer(TopicId) ->
            {reply, TopicId, State};
        undefined ->
            case next_topic_id(Tab, PredefId, ClientId) of
                TopicId when TopicId >= 16#FFFF ->
                    {reply, {error, too_large}, State};
                TopicId ->
                    Fun = fun() ->
                        mnesia:write(
                            Tab,
                            #emqx_sn_registry{
                                key = {ClientId, next_topic_id},
                                value = TopicId + 1
                            },
                            write
                        ),
                        mnesia:write(
                            Tab,
                            #emqx_sn_registry{
                                key = {ClientId, TopicName},
                                value = TopicId
                            },
                            write
                        ),
                        mnesia:write(
                            Tab,
                            #emqx_sn_registry{
                                key = {ClientId, TopicId},
                                value = TopicName
                            },
                            write
                        )
                    end,
                    case mria:transaction(?SN_SHARD, Fun) of
                        {atomic, ok} ->
                            {reply, TopicId, State};
                        {aborted, Error} ->
                            {reply, {error, Error}, State}
                    end
            end
    end;
handle_call({unregister, ClientId}, _From, State = #state{tabname = Tab}) ->
    Registry = mnesia:dirty_match_object(
        Tab,
        {emqx_sn_registry, {ClientId, '_'}, '_'}
    ),
    lists:foreach(
        fun(R) ->
            mria:dirty_delete_object(Tab, R)
        end,
        Registry
    ),
    {reply, ok, State};
handle_call(name, _From, State = #state{tabname = Tab}) ->
    {reply, {Tab, self()}, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{
        msg => "unexpected_call",
        call => Req
    }),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{
        msg => "unexpected_cast",
        cast => Msg
    }),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{
        msg => "unexpected_info",
        info => Info
    }),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-----------------------------------------------------------------------------

next_topic_id(Tab, PredefId, ClientId) ->
    case mnesia:dirty_read(Tab, {ClientId, next_topic_id}) of
        [#emqx_sn_registry{value = Id}] -> Id;
        [] -> PredefId + 1
    end.
