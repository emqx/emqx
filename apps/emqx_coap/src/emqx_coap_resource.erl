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

-module(emqx_coap_resource).

-behaviour(coap_resource).

-include("emqx_coap.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("gen_coap/include/coap.hrl").

-logger_header("[CoAP-RES]").

-export([ coap_discover/2
        , coap_get/5
        , coap_post/4
        , coap_put/4
        , coap_delete/3
        , coap_observe/5
        , coap_unobserve/1
        , handle_info/2
        , coap_ack/2
        ]).

-ifdef(TEST).
-export([topic/1]).
-endif.

-define(MQTT_PREFIX, [<<"mqtt">>]).

% resource operations
coap_discover(_Prefix, _Args) ->
    [{absolute, [<<"mqtt">>], []}].

coap_get(ChId, ?MQTT_PREFIX, Path, Query, _Content) ->
    ?LOG(debug, "coap_get() Path=~p, Query=~p~n", [Path, redact_query(Query)]),
    #coap_mqtt_auth{clientid = Clientid, username = Usr, password = Passwd} = get_auth(Query),
    case emqx_coap_mqtt_adapter:client_pid(Clientid, Usr, Passwd, ChId) of
        {ok, Pid} ->
            put(mqtt_client_pid, Pid),
            #coap_content{};
        {error, auth_failure} ->
            put(mqtt_client_pid, undefined),
            {error, unauthorized};
        {error, bad_request} ->
            put(mqtt_client_pid, undefined),
            {error, bad_request};
        {error, _Other} ->
            put(mqtt_client_pid, undefined),
            {error, internal_server_error}
    end;
coap_get(ChId, Prefix, Path, Query, _Content) ->
    ?LOG(error, "ignore bad get request ChId=~p, Prefix=~p, Path=~p, Query=~p",
         [ChId, Prefix, Path, redact_query(Query)]),
    {error, bad_request}.

coap_post(_ChId, _Prefix, _Topic, _Content) ->
    {error, method_not_allowed}.

coap_put(_ChId, ?MQTT_PREFIX, Topic, #coap_content{payload = Payload}) when Topic =/= [] ->
    ?LOG(debug, "put message, Topic=~p, Payload=~p~n", [Topic, Payload]),
    Pid = get(mqtt_client_pid),
    emqx_coap_mqtt_adapter:publish(Pid, topic(Topic), Payload);
coap_put(_ChId, Prefix, Topic, Content) ->
    ?LOG(error, "put has error, Prefix=~p, Topic=~p, Content=~p", [Prefix, Topic, Content]),
    {error, bad_request}.

coap_delete(_ChId, _Prefix, _Topic) ->
    {error, method_not_allowed}.

coap_observe(ChId, ?MQTT_PREFIX, Topic, Ack, Content) when Topic =/= [] ->
    TrueTopic = topic(Topic),
    ?LOG(debug, "observe Topic=~p, Ack=~p", [TrueTopic, Ack]),
    Pid = get(mqtt_client_pid),
    case emqx_coap_mqtt_adapter:subscribe(Pid, TrueTopic) of
        ok -> {ok, {state, ChId, ?MQTT_PREFIX, [TrueTopic]}, content, Content};
        {error, Code} -> {error, Code}
    end;
coap_observe(ChId, Prefix, Topic, Ack, _Content) ->
    ?LOG(error, "unknown observe request ChId=~p, Prefix=~p, Topic=~p, Ack=~p", [ChId, Prefix, Topic, Ack]),
    {error, bad_request}.

coap_unobserve({state, _ChId, ?MQTT_PREFIX, Topic}) when Topic =/= [] ->
    ?LOG(debug, "unobserve ~p", [Topic]),
    Pid = get(mqtt_client_pid),
    emqx_coap_mqtt_adapter:unsubscribe(Pid, topic(Topic)),
    ok;
coap_unobserve({state, ChId, Prefix, Topic}) ->
    ?LOG(error, "ignore unknown unobserve request ChId=~p, Prefix=~p, Topic=~p", [ChId, Prefix, Topic]),
    ok.

handle_info({dispatch, Topic, Payload}, State) ->
    %% This clause should never be matched any more. We keep it here to handle
    %% the old format messages during the release upgrade.
    %% In this case the second function clause of `coap_ack/2` will be called,
    %% and the ACKs is discarded.
    ?LOG(debug, "dispatch Topic=~p, Payload=~p", [Topic, Payload]),
    {notify, [], #coap_content{format = <<"application/octet-stream">>, payload = Payload}, State};
handle_info({dispatch, Msg}, State) ->
    Payload = emqx_coap_mqtt_adapter:message_payload(Msg),
    {notify, {pub, Msg}, #coap_content{format = <<"application/octet-stream">>, payload = Payload}, State};
handle_info(Message, State) ->
    emqx_coap_mqtt_adapter:handle_info(Message, State).

coap_ack({pub, Msg}, State) ->
    ?LOG(debug, "received coap ack for publish msg: ~p", [Msg]),
    Pid = get(mqtt_client_pid),
    emqx_coap_mqtt_adapter:received_puback(Pid, Msg),
    {ok, State};
coap_ack(_Ref, State) ->
    ?LOG(debug, "received coap ack: ~p", [_Ref]),
    {ok, State}.

get_auth(Query) ->
    get_auth(Query, #coap_mqtt_auth{}).

get_auth([], Auth=#coap_mqtt_auth{}) ->
    Auth;
get_auth([<<$c, $=, Rest/binary>>|T], Auth=#coap_mqtt_auth{}) ->
    get_auth(T, Auth#coap_mqtt_auth{clientid = Rest});
get_auth([<<$u, $=, Rest/binary>>|T], Auth=#coap_mqtt_auth{}) ->
    get_auth(T, Auth#coap_mqtt_auth{username = Rest});
get_auth([<<$p, $=, Rest/binary>>|T], Auth=#coap_mqtt_auth{}) ->
    get_auth(T, Auth#coap_mqtt_auth{password = Rest});
get_auth([Param|T], Auth=#coap_mqtt_auth{}) ->
    ?LOG(error, "ignore unknown parameter ~p", [Param]),
    get_auth(T, Auth).

topic(Topic) when is_binary(Topic) -> Topic;
topic([]) -> <<>>;
topic([Path | TopicPath]) ->
    case topic(TopicPath) of
        <<>> -> Path;
        RemTopic ->
            <<Path/binary, $/, RemTopic/binary>>
    end.

redact_query(Auths) ->
    lists:map(fun(<<$p, $=, _Rest/binary>>) ->
                      <<$p, $=, "******">>;
                 (E) ->
                      E
              end,
              Auths).
