%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mqttsn_registry).

-include("emqx_mqttsn.hrl").

-export([
    persist_predefined_topics/1,
    clear_predefined_topics/1
]).

-export([
    init/0,
    reg/2,
    unreg/2,
    lookup_topic/2,
    lookup_topic_id/2
]).

-export_type([registry/0]).

-define(PKEY(Id), {mqttsn, predef_topics, Id}).

-type registry() :: #{
    %% The last topic id aallocated
    last_topic_id := pos_integer(),
    %% The mapping from topic id to topic name
    id_to_name := map(),
    %% The mapping from topic name to topic id
    name_to_id := map()
}.

-type predef_topic() :: #{
    id := 1..1024,
    topic := iolist()
}.

%%-----------------------------------------------------------------------------
%% APIs

-spec persist_predefined_topics([predef_topic()]) -> ok.
persist_predefined_topics(PredefTopics) when is_list(PredefTopics) ->
    try
        F = fun(#{id := TopicId, topic := TopicName0}) when TopicId =< 1024 ->
            TopicName = iolist_to_binary(TopicName0),
            persistent_term:put(?PKEY(TopicId), TopicName),
            persistent_term:put(?PKEY(TopicName), TopicId)
        end,
        lists:foreach(F, PredefTopics)
    catch
        _:_ ->
            clear_predefined_topics(PredefTopics),
            error(badarg)
    end.

-spec clear_predefined_topics([predef_topic()]) -> ok.
clear_predefined_topics(PredefTopics) ->
    lists:foreach(
        fun(#{id := TopicId, topic := TopicName0}) ->
            TopicName = iolist_to_binary(TopicName0),
            persistent_term:erase(?PKEY(TopicId)),
            persistent_term:erase(?PKEY(TopicName))
        end,
        PredefTopics
    ),
    ok.

-spec init() -> registry().
init() ->
    #{
        last_topic_id => ?SN_MAX_PREDEF_TOPIC_ID,
        id_to_name => #{},
        name_to_id => #{}
    }.

-spec reg(emqx_types:topic(), registry()) ->
    {ok, integer(), registry()}
    | {error, term()}.
reg(
    TopicName,
    Registry
) when is_binary(TopicName) ->
    case emqx_topic:wildcard(TopicName) of
        false ->
            case lookup_topic_id(TopicName, Registry) of
                {predef, TopicId} when is_integer(TopicId) ->
                    {ok, TopicId, Registry};
                TopicId when is_integer(TopicId) ->
                    {ok, TopicId, Registry};
                undefined ->
                    do_reg(TopicName, Registry)
            end;
        %% TopicId: in case of “accepted” the value that will be used as topic
        %% id by the gateway when sending PUBLISH messages to the client (not
        %% relevant in case of subscriptions to a short topic name or to a topic
        %% name which contains wildcard characters)
        true ->
            {error, wildcard_topic}
    end.

do_reg(
    TopicName,
    Registry = #{
        last_topic_id := TopicId0,
        id_to_name := IdMap,
        name_to_id := NameMap
    }
) ->
    case next_topic_id(TopicId0) of
        {error, too_large} ->
            {error, too_large};
        NextTopicId ->
            NRegistry = Registry#{
                last_topic_id := NextTopicId,
                id_to_name := maps:put(NextTopicId, TopicName, IdMap),
                name_to_id := maps:put(TopicName, NextTopicId, NameMap)
            },
            {ok, NextTopicId, NRegistry}
    end.

next_topic_id(Id) when is_integer(Id) andalso (Id < 16#FFFF) ->
    Id + 1;
next_topic_id(Id) when is_integer(Id) ->
    {error, too_large}.

-spec lookup_topic(pos_integer(), registry()) ->
    undefined
    | binary().
lookup_topic(TopicId, _Registry = #{id_to_name := IdMap}) when is_integer(TopicId) ->
    case persistent_term:get(?PKEY(TopicId), undefined) of
        undefined ->
            maps:get(TopicId, IdMap, undefined);
        Topic ->
            Topic
    end.

-spec lookup_topic_id(emqx_types:topic(), registry()) ->
    undefined
    | pos_integer()
    | {predef, integer()}.
lookup_topic_id(TopicName, _Registry = #{name_to_id := NameMap}) when is_binary(TopicName) ->
    case persistent_term:get(?PKEY(TopicName), undefined) of
        undefined ->
            maps:get(TopicName, NameMap, undefined);
        TopicId ->
            {predef, TopicId}
    end.

-spec unreg(emqx_types:topic(), registry()) -> registry().
unreg(TopicName, Registry = #{name_to_id := NameMap, id_to_name := IdMap}) when
    is_binary(TopicName)
->
    case maps:find(TopicName, NameMap) of
        {ok, TopicId} ->
            Registry#{
                name_to_id := maps:remove(TopicName, NameMap),
                id_to_name := maps:remove(TopicId, IdMap)
            };
        error ->
            Registry
    end.
