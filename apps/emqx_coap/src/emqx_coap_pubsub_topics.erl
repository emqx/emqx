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

-module(emqx_coap_pubsub_topics).

-behaviour(gen_server).

-include("emqx_coap.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[CoAP-PS-TOPICS]").

-export([ start_link/0
        , stop/1
        ]).

-export([ add_topic_info/4
        , delete_topic_info/1
        , delete_sub_topics/1
        , is_topic_existed/1
        , is_topic_timeout/1
        , reset_topic_info/2
        , reset_topic_info/3
        , reset_topic_info/4
        , lookup_topic_info/1
        , lookup_topic_payload/1
        ]).

%% gen_server.
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {}).

-define(COAP_TOPIC_TABLE, coap_topic).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:stop(Pid).

add_topic_info(Topic, MaxAge, CT, Payload) when is_binary(Topic), is_integer(MaxAge), is_binary(CT), is_binary(Payload) ->
    gen_server:call(?MODULE, {add_topic, {Topic, MaxAge, CT, Payload}}).

delete_topic_info(Topic) when is_binary(Topic) ->
    gen_server:call(?MODULE, {remove_topic, Topic}).

delete_sub_topics(Topic) when is_binary(Topic) ->
    gen_server:cast(?MODULE, {remove_sub_topics, Topic}).

reset_topic_info(Topic, Payload) ->
    gen_server:call(?MODULE, {reset_topic, {Topic, Payload}}).

reset_topic_info(Topic, MaxAge, Payload) ->
    gen_server:call(?MODULE, {reset_topic, {Topic, MaxAge, Payload}}).

reset_topic_info(Topic, MaxAge, CT, Payload) ->
    gen_server:call(?MODULE, {reset_topic, {Topic, MaxAge, CT, Payload}}).

is_topic_existed(Topic) ->
    ets:member(?COAP_TOPIC_TABLE, Topic).

is_topic_timeout(Topic) when is_binary(Topic) ->
    [{Topic, MaxAge, _, _, TimeStamp}] = ets:lookup(?COAP_TOPIC_TABLE, Topic),
    %% MaxAge: x seconds
    MaxAge < ((erlang:system_time(millisecond) - TimeStamp) / 1000).

lookup_topic_info(Topic) ->
    ets:lookup(?COAP_TOPIC_TABLE, Topic).

lookup_topic_payload(Topic) ->
    try ets:lookup_element(?COAP_TOPIC_TABLE, Topic, 4)
    catch
        error:badarg -> undefined
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?COAP_TOPIC_TABLE, [set, named_table, protected]),
    ?LOG(debug, "Create the coap_topic table", []),
    {ok, #state{}}.

handle_call({add_topic, {Topic, MaxAge, CT, Payload}}, _From, State) ->
    Ret = create_table_element(Topic, MaxAge, CT, Payload),
    {reply, {ok, Ret}, State, hibernate};

handle_call({reset_topic, {Topic, Payload}}, _From, State) ->
    Ret = update_table_element(Topic, Payload),
    {reply, {ok, Ret}, State, hibernate};

handle_call({reset_topic, {Topic, MaxAge, Payload}}, _From, State) ->
    Ret = update_table_element(Topic, MaxAge, Payload),
    {reply, {ok, Ret}, State, hibernate};

handle_call({reset_topic, {Topic, MaxAge, CT, Payload}}, _From, State) ->
    Ret = update_table_element(Topic, MaxAge, CT, Payload),
    {reply, {ok, Ret}, State, hibernate};

handle_call({remove_topic, {Topic, _Content}}, _From, State) ->
    ets:delete(?COAP_TOPIC_TABLE, Topic),
    ?LOG(debug, "Remove topic ~p in the coap_topic table", [Topic]),
    {reply, ok, State, hibernate};

handle_call(Request, _From, State) ->
    ?LOG(error, "adapter unexpected call ~p", [Request]),
    {reply, ignored, State, hibernate}.

handle_cast({remove_sub_topics, TopicPrefix}, State) ->
    DeletedTopicNum = ets:foldl(fun ({Topic, _, _, _, _}, AccIn) ->
                                    case binary:match(Topic, TopicPrefix) =/= nomatch of
                                        true  ->
                                            ?LOG(debug, "Remove topic ~p in the coap_topic table", [Topic]),
                                            ets:delete(?COAP_TOPIC_TABLE, Topic),
                                            AccIn + 1;
                                        false ->
                                            AccIn
                                    end
                                end, 0, ?COAP_TOPIC_TABLE),
    ?LOG(debug, "Remove number of ~p topics with prefix=~p in the coap_topic table", [DeletedTopicNum, TopicPrefix]),
    {noreply, State, hibernate};

handle_cast(Msg, State) ->
    ?LOG(error, "broker_api unexpected cast ~p", [Msg]),
    {noreply, State, hibernate}.

handle_info(Info, State) ->
    ?LOG(error, "adapter unexpected info ~p", [Info]),
    {noreply, State, hibernate}.

terminate(Reason, #state{}) ->
    ets:delete(?COAP_TOPIC_TABLE),
    ?LOG(error, "the ~p terminate for reason ~p", [?MODULE, Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------
create_table_element(Topic, MaxAge, CT, Payload) ->
    TopicInfo = {Topic, MaxAge, CT, Payload, erlang:system_time(millisecond)},
    ?LOG(debug, "Insert ~p in the coap_topic table", [TopicInfo]),
    ets:insert_new(?COAP_TOPIC_TABLE, TopicInfo).

update_table_element(Topic, Payload) ->
    ?LOG(debug, "Update the topic=~p only with Payload", [Topic]),
    ets:update_element(?COAP_TOPIC_TABLE, Topic, [{4, Payload}, {5, erlang:system_time(millisecond)}]).

update_table_element(Topic, MaxAge, Payload) ->
    ?LOG(debug, "Update the topic=~p info of MaxAge=~p and Payload", [Topic, MaxAge]),
    ets:update_element(?COAP_TOPIC_TABLE, Topic, [{2, MaxAge}, {4, Payload}, {5, erlang:system_time(millisecond)}]).

update_table_element(Topic, MaxAge, CT, <<>>) ->
    ?LOG(debug, "Update the topic=~p info of MaxAge=~p, CT=~p, payload=<<>>", [Topic, MaxAge, CT]),
    ets:update_element(?COAP_TOPIC_TABLE, Topic, [{2, MaxAge}, {3, CT}, {5, erlang:system_time(millisecond)}]).
