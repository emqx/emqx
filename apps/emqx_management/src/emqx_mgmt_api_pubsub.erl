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

-module(emqx_mgmt_api_pubsub).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include("emqx_mgmt.hrl").

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
    minirest:return(do_subscribe(ClientId, Topic, QoS)).

publish(_Bindings, Params) ->
    logger:debug("API publish Params:~p", [Params]),
    try parse_publish_params(Params) of
        Result -> do_publish(Params, Result)
    catch
        _E : _R ->
            logger:debug("API publish result:~p ~p", [_E, _R]),
            minirest:return({ok, ?ERROR8, bad_params})
    end.

unsubscribe(_Bindings, Params) ->
    logger:debug("API unsubscribe Params:~p", [Params]),
    {ClientId, Topic} = parse_unsubscribe_params(Params),
    minirest:return(do_unsubscribe(ClientId, Topic)).

subscribe_batch(_Bindings, Params) ->
    logger:debug("API subscribe batch Params:~p", [Params]),
    minirest:return({ok, loop_subscribe(Params)}).

publish_batch(_Bindings, Params) ->
    logger:debug("API publish batch Params:~p", [Params]),
    minirest:return({ok, loop_publish(Params)}).

unsubscribe_batch(_Bindings, Params) ->
    logger:debug("API unsubscribe batch Params:~p", [Params]),
    minirest:return({ok, loop_unsubscribe(Params)}).

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
               topic => resp_topic(proplists:get_value(<<"topic">>, Params),
                                   proplists:get_value(<<"topics">>, Params, <<"">>)),
               code => Code},
    loop_subscribe(ParamsN, [Result | Acc]).

loop_publish(Params) ->
    loop_publish(Params, []).
loop_publish([], Result) ->
    lists:reverse(Result);
loop_publish([Params | ParamsN], Acc) ->
    Result =
        try parse_publish_params(Params) of
            Res ->
                Code = case do_publish(Params, Res) of
                           {ok, #{code := ?SUCCESS}} -> 0;
                           {ok, #{code := Code0}} -> Code0
                       end,
                #{topic => resp_topic(proplists:get_value(<<"topic">>, Params),
                                      proplists:get_value(<<"topics">>, Params, <<"">>)),
                  code => Code}
        catch
            _E : _R -> #{code => ?ERROR8, message => <<"bad_params">>}
        end,
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
               topic => resp_topic(proplists:get_value(<<"topic">>, Params),
                                   proplists:get_value(<<"topics">>, Params, <<"">>)),
               code => Code},
    loop_unsubscribe(ParamsN, [Result | Acc]).

do_subscribe(ClientId, _Topics, _QoS) when not is_binary(ClientId) ->
    {ok, ?ERROR8, <<"bad clientid: must be string">>};
do_subscribe(_ClientId, [], _QoS) ->
    {ok, ?ERROR15, bad_topic};
do_subscribe(_ClientId, _Topic, QoS) when QoS =/= 0 andalso QoS =/= 1 andalso QoS =/= 2 ->
    {ok, ?ERROR16, bad_qos};
do_subscribe(ClientId, Topics, QoS) ->
    TopicTable = parse_topic_filters(Topics, QoS),
    case emqx_mgmt:subscribe(ClientId, TopicTable) of
        {error, Reason} -> {ok, ?ERROR12, Reason};
        _ -> ok
    end.

do_publish(Params, {ClientId, Topic, Qos, Retain, Payload, Props}) ->
    case do_publish(ClientId, Topic, Qos, Retain, Payload, Props) of
        {ok, MsgIds} ->
            case proplists:get_value(<<"return">>, Params, undefined) of
                undefined -> minirest:return(ok);
                _Val ->
                    case proplists:get_value(<<"topics">>, Params, undefined) of
                        undefined -> minirest:return({ok, #{msgid => lists:last(MsgIds)}});
                        _ -> minirest:return({ok, #{msgids => MsgIds}})
                    end
            end;
        Result ->
            minirest:return(Result)
    end.

do_publish(ClientId, _Topics, _Qos, _Retain, _Payload, _Props)
  when not (is_binary(ClientId) or (ClientId =:= undefined)) ->
    {ok, ?ERROR8, <<"bad clientid: must be string">>};
do_publish(_ClientId, [], _Qos, _Retain, _Payload, _Props) ->
    {ok, ?ERROR15, bad_topic};
do_publish(ClientId, Topics, Qos, Retain, Payload, Props) ->
    MsgIds = lists:map(fun(Topic) ->
        Msg = emqx_message:make(ClientId, Qos, Topic, Payload,
            #{retain => Retain}, Props),
        _ = emqx_mgmt:publish(Msg),
        emqx_guid:to_hexstr(Msg#message.id)
    end, Topics),
    {ok, MsgIds}.

do_unsubscribe(ClientId, _Topic) when not is_binary(ClientId) ->
    {ok, ?ERROR8, <<"bad clientid: must be string">>};
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
    ClientId = proplists:get_value(<<"clientid">>, Params),
    Topics   = topics(filter, proplists:get_value(<<"topic">>, Params),
                              proplists:get_value(<<"topics">>, Params, <<"">>)),
    QoS      = proplists:get_value(<<"qos">>, Params, 0),
    {ClientId, Topics, QoS}.

parse_publish_params(Params) ->
    Topics    = topics(name, proplists:get_value(<<"topic">>, Params),
                             proplists:get_value(<<"topics">>, Params, <<"">>)),
    ClientId  = proplists:get_value(<<"clientid">>, Params),
    Payload   = decode_payload(proplists:get_value(<<"payload">>, Params, <<>>),
                               proplists:get_value(<<"encoding">>, Params, <<"plain">>)),
    Qos       = proplists:get_value(<<"qos">>, Params, 0),
    Retain    = proplists:get_value(<<"retain">>, Params, false),
    Payload1  = maybe_maps_to_binary(Payload),
    Props = parse_props(Params),
    {ClientId, Topics, Qos, Retain, Payload1, Props}.

parse_unsubscribe_params(Params) ->
    ClientId = proplists:get_value(<<"clientid">>, Params),
    Topic    = proplists:get_value(<<"topic">>, Params),
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

-define(PROP_MAPPING,
    #{<<"payload_format_indicator">> => 'Payload-Format-Indicator',
        <<"message_expiry_interval">> => 'Message-Expiry-Interval',
        <<"response_topic">> => 'Response-Topic',
        <<"correlation_data">> => 'Correlation-Data',
        <<"user_properties">> => 'User-Property',
        <<"subscription_identifier">> => 'Subscription-Identifier',
        <<"content_type">> => 'Content-Type'
    }).

parse_props(Params) ->
    Properties0 = proplists:get_value(<<"properties">>, Params, []),
    Properties1 = lists:foldl(fun({Name, Value}, Acc) ->
        case maps:find(Name, ?PROP_MAPPING) of
            {ok, Key} -> Acc#{Key => Value};
            error -> error({invalid_property, Name})
        end
                              end, #{}, Properties0),
    %% Compatible with older API
    UserProp1 = generate_user_props(proplists:get_value(<<"user_properties">>, Params, [])),
    UserProp2 =
        case Properties1 of
            #{'User-Property' := UserProp1List} -> generate_user_props(UserProp1List);
            _ -> []
        end,
    #{properties => Properties1#{'User-Property' => UserProp1 ++ UserProp2}}.

generate_user_props(UserProps) when is_list(UserProps)->
    lists:map(fun
                  ({Name, Value}) ->  {bin(Name), bin(Value)};
                  (Invalid) -> error({invalid_user_property, Invalid})
              end
        , UserProps);
generate_user_props(UserProps) ->
    error({user_properties_type_error, UserProps}).

bin(Bin) when is_binary(Bin) -> Bin;
bin(Num) when is_number(Num) -> number_to_binary(Num);
bin(Boolean) when is_boolean(Boolean) -> atom_to_binary(Boolean);
bin(Other) -> error({user_properties_type_error, Other}).

-define(FLOAT_PRECISION, 17).
number_to_binary(Int) when is_integer(Int) ->
    integer_to_binary(Int);
number_to_binary(Float) when is_float(Float) ->
    float_to_binary(Float, [{decimals, ?FLOAT_PRECISION}, compact]).
