%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_request_sender).

-export([start_link/3, stop/1, send/6]).

-include_lib("emqx/include/emqx_mqtt.hrl").

start_link(ResponseTopic, QoS, Options0) ->
    Parent = self(),
    MsgHandler = make_msg_handler(Parent),
    Options = [{msg_handler, MsgHandler} | Options0],
    case emqtt:start_link(Options) of
        {ok, Pid} ->
            {ok, _} = emqtt:connect(Pid),
            try subscribe(Pid, ResponseTopic, QoS) of
                ok -> {ok, Pid};
                {error, _} = Error -> Error
            catch
                C:E:S ->
                    emqtt:stop(Pid),
                    {error, {C, E, S}}
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Send a message to request topic with correlation-data `CorrData'.
%% Response should be delivered as a `{response, CorrData, Payload}'
send(Client, ReqTopic, RspTopic, CorrData, Payload, QoS) ->
    Props = #{
        'Response-Topic' => RspTopic,
        'Correlation-Data' => CorrData
    },
    Msg = #mqtt_msg{
        qos = QoS,
        topic = ReqTopic,
        props = Props,
        payload = Payload
    },
    case emqtt:publish(Client, Msg) of
        %% QoS = 0
        ok -> ok;
        {ok, _} -> ok;
        {error, _} = E -> E
    end.

stop(Pid) ->
    emqtt:disconnect(Pid).

subscribe(Client, Topic, QoS) ->
    case emqtt:subscribe(Client, Topic, QoS) of
        {ok, _, _} -> ok;
        {error, _} = Error -> Error
    end.

make_msg_handler(Parent) ->
    #{
        publish => fun(Msg) -> handle_msg(Msg, Parent) end,
        puback => fun(_Ack) -> ok end,
        disconnected => fun(_Reason) -> ok end
    }.

handle_msg(Msg, Parent) ->
    #{properties := Props, payload := Payload} = Msg,
    CorrData = maps:get('Correlation-Data', Props),
    Parent ! {response, CorrData, Payload},
    ok.
