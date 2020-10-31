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

-module(emqx_mgmt_api_pubsub).

-include_lib("emqx_libs/include/emqx.hrl").
-include_lib("emqx_libs/include/emqx_mqtt.hrl").
-include("emqx_mgmt.hrl").

-import(proplists, [ get_value/2
                   , get_value/3
                   ]).

-import(minirest, [return/1]).

-rest_api(#{name   => mqtt_subscribe,
            method => 'POST',
            path   => "/mqtt/subscribe",
            func   => subscribe,
            descr  => "Subscribe a topic"}).

-rest_api(#{name   => mqtt_publish,
            method => 'POST',
            path   => "/mqtt/publish",
            func   => publish,
            descr  => "Publish a MQTT message"}).

-rest_api(#{name   => mqtt_unsubscribe,
            method => 'POST',
            path   => "/mqtt/unsubscribe",
            func   => unsubscribe,
            descr  => "Unsubscribe a topic"}).

-rest_api(#{name   => mqtt_subscribe_batch,
            method => 'POST',
            path   => "/mqtt/subscribe_batch",
            func   => subscribe_batch,
            descr  => "Batch subscribes topics"}).

-rest_api(#{name   => mqtt_publish_batch,
            method => 'POST',
            path   => "/mqtt/publish_batch",
            func   => publish_batch,
            descr  => "Batch publish MQTT messages"}).

-rest_api(#{name   => mqtt_unsubscribe_batch,
            method => 'POST',
            path   => "/mqtt/unsubscribe_batch",
            func   => unsubscribe_batch,
            descr  => "Batch unsubscribes topics"}).

-export([ subscribe/2
        , publish/2
        , unsubscribe/2
        , subscribe_batch/2
        , publish_batch/2
        , unsubscribe_batch/2
        ]).

subscribe(_Bindings, Params) ->
    logger:debug("API subscribe Params:~p", [Params]),
    {ClientId, Topic, QoS} = parse_subscribe_params(Params),
    return(do_subscribe(ClientId, Topic, QoS)).

publish(_Bindings, Params) ->
    logger:debug("API publish Params:~p", [Params]),
    {ClientId, Topic, Qos, Retain, Payload} = parse_publish_params(Params),
    return(do_publish(ClientId, Topic, Qos, Retain, Payload)).

unsubscribe(_Bindings, Params) ->
    logger:debug("API unsubscribe Params:~p", [Params]),
    {ClientId, Topic} = parse_unsubscribe_params(Params),
    return(do_unsubscribe(ClientId, Topic)).

subscribe_batch(_Bindings, Params) ->
    logger:debug("API subscribe batch Params:~p", [Params]),
    return({ok, loop_subscribe(Params)}).

publish_batch(_Bindings, Params) ->
    logger:debug("API publish batch Params:~p", [Params]),
    return({ok, loop_publish(Params)}).

unsubscribe_batch(_Bindings, Params) ->
    logger:debug("API unsubscribe batch Params:~p", [Params]),
    return({ok, loop_unsubscribe(Params)}).

loop_subscribe(Params) ->
    loop_subscribe(Params, []).
loop_subscribe([], Result) ->
    lists:reverse(Result);
loop_subscribe([Params | ParamsN], Acc) ->
    {ClientId, Topic, QoS} = parse_subscribe_params(Params),
    Code = case do_subscribe(ClientId, Topic, QoS) of
        ok -> 0;
        {_, Code0, _Reason} -> Code0
    end,
    Result = #{clientid => ClientId,
               topic => resp_topic(get_value(<<"topic">>, Params), get_value(<<"topics">>, Params, <<"">>)),
               code => Code},
    loop_subscribe(ParamsN, [Result | Acc]).

loop_publish(Params) ->
    loop_publish(Params, []).
loop_publish([], Result) ->
    lists:reverse(Result);
loop_publish([Params | ParamsN], Acc) ->
    {ClientId, Topic, Qos, Retain, Payload} = parse_publish_params(Params),
    Code = case do_publish(ClientId, Topic, Qos, Retain, Payload) of
        ok -> 0;
        {_, Code0, _} -> Code0
    end,
    Result = #{topic => resp_topic(get_value(<<"topic">>, Params), get_value(<<"topics">>, Params, <<"">>)),
               code => Code},
    loop_publish(ParamsN, [Result | Acc]).

loop_unsubscribe(Params) ->
    loop_unsubscribe(Params, []).
loop_unsubscribe([], Result) ->
    lists:reverse(Result);
loop_unsubscribe([Params | ParamsN], Acc) ->
    {ClientId, Topic} = parse_unsubscribe_params(Params),
    Code = case do_unsubscribe(ClientId, Topic) of
        ok -> 0;
        {_, Code0, _} -> Code0
    end,
    Result = #{clientid => ClientId,
               topic => resp_topic(get_value(<<"topic">>, Params), get_value(<<"topics">>, Params, <<"">>)),
               code => Code},
    loop_unsubscribe(ParamsN, [Result | Acc]).

do_subscribe(_ClientId, [], _QoS) ->
    {ok, ?ERROR15, bad_topic};
do_subscribe(ClientId, Topics, QoS) ->
    TopicTable = parse_topic_filters(Topics, QoS),
    case emqx_mgmt:subscribe(ClientId, TopicTable) of
        {error, Reason} -> {ok, ?ERROR12, Reason};
        _ -> ok
    end.

do_publish(_ClientId, [], _Qos, _Retain, _Payload) ->
    {ok, ?ERROR15, bad_topic};
do_publish(ClientId, Topics, Qos, Retain, Payload) ->
    lists:foreach(fun(Topic) ->
        Msg = emqx_message:make(ClientId, Qos, Topic, Payload),
        emqx_mgmt:publish(Msg#message{flags = #{retain => Retain}})
    end, Topics),
    ok.

do_unsubscribe(ClientId, Topic) ->
    case validate_by_filter(Topic) of
        true ->
            case emqx_mgmt:unsubscribe(ClientId, Topic) of
                {error, Reason} -> {ok, ?ERROR12, Reason};
                _ -> ok
            end;
        false ->
            {ok, ?ERROR15, bad_topic}
    end.

parse_subscribe_params(Params) ->
    ClientId = get_value(<<"clientid">>, Params),
    Topics   = topics(filter, get_value(<<"topic">>, Params), get_value(<<"topics">>, Params, <<"">>)),
    QoS      = get_value(<<"qos">>, Params, 0),
    {ClientId, Topics, QoS}.

parse_publish_params(Params) ->
    Topics   = topics(name, get_value(<<"topic">>, Params), get_value(<<"topics">>, Params, <<"">>)),
    ClientId = get_value(<<"clientid">>, Params),
    Payload  = decode_payload(get_value(<<"payload">>, Params, <<>>),
                              get_value(<<"encoding">>, Params, <<"plain">>)),
    Qos      = get_value(<<"qos">>, Params, 0),
    Retain   = get_value(<<"retain">>, Params, false),
    Payload1 = maybe_maps_to_binary(Payload),
    {ClientId, Topics, Qos, Retain, Payload1}.

parse_unsubscribe_params(Params) ->
    ClientId = get_value(<<"clientid">>, Params),
    Topic    = get_value(<<"topic">>, Params),
    {ClientId, Topic}.

topics(Type, undefined, Topics0) ->
    Topics = binary:split(Topics0, <<",">>, [global]),
    case Type of
        name -> lists:filter(fun(T) -> validate_by_name(T) end, Topics);
        filter -> lists:filter(fun(T) -> validate_by_filter(T) end, Topics)
    end;

topics(Type, Topic, _) ->
    topics(Type, undefined, Topic).

%%TODO:
% validate(qos, Qos) ->
%    (Qos >= ?QOS_0) and (Qos =< ?QOS_2).

validate_by_filter(Topic) ->
    validate_topic({filter, Topic}).

validate_by_name(Topic) ->
    validate_topic({name, Topic}).

validate_topic({Type, Topic}) ->
    try emqx_topic:validate({Type, Topic}) of
        true -> true
    catch
        error:_Reason -> false
    end.

parse_topic_filters(Topics, Qos) ->
    [begin
         {Topic, Opts} = emqx_topic:parse(Topic0),
         {Topic, Opts#{qos => Qos}}
     end || Topic0 <- Topics].

resp_topic(undefined, Topics) -> Topics;
resp_topic(Topic, _) -> Topic.

decode_payload(Payload, <<"base64">>) -> base64:decode(Payload);
decode_payload(Payload, _) -> Payload.

maybe_maps_to_binary(Payload) when is_binary(Payload) -> Payload;
maybe_maps_to_binary(Payload) ->
  try
      emqx_json:encode(Payload)
  catch
      _C : _E : S ->
         error({encode_payload_fail, S})
  end.
