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

-module(emqx_request_sender).

-export([start_link/3, stop/1, send/6]).

-include("emqx_mqtt.hrl").

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
                C : E : S ->
                    emqtt:stop(Pid),
                    {error, {C, E, S}}
            end;
        {error, _} = Error -> Error
    end.

%% @doc Send a message to request topic with correlation-data `CorrData'.
%% Response should be delivered as a `{response, CorrData, Payload}'
send(Client, ReqTopic, RspTopic, CorrData, Payload, QoS) ->
    Props = #{'Response-Topic' => RspTopic,
              'Correlation-Data' => CorrData
             },
    Msg = #mqtt_msg{qos = QoS,
                    topic = ReqTopic,
                    props = Props,
                    payload = Payload
                   },
    case emqtt:publish(Client, Msg) of
        ok -> ok; %% QoS = 0
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
    #{publish => fun(Msg) -> handle_msg(Msg, Parent) end,
      puback => fun(_Ack) -> ok end,
      disconnected => fun(_Reason) -> ok end
     }.

handle_msg(Msg, Parent) ->
    #{properties := Props, payload := Payload} = Msg,
    CorrData = maps:get('Correlation-Data', Props),
    Parent ! {response, CorrData, Payload},
    ok.
