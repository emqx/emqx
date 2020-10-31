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

-module(emqx_request_handler).

-export([start_link/4, stop/1]).

-include("emqx_mqtt.hrl").

-type qos() :: emqx_mqtt_types:qos_name() | emqx_mqtt_types:qos().
-type topic() :: emqx_topic:topic().
-type handler() :: fun((CorrData :: binary(), ReqPayload :: binary()) -> RspPayload :: binary()).

-spec start_link(topic(), qos(), handler(), emqtt:options()) ->
        {ok, pid()} | {error, any()}.
start_link(RequestTopic, QoS, RequestHandler, Options0) ->
    Parent = self(),
    MsgHandler = make_msg_handler(RequestHandler, Parent),
    Options = [{msg_handler, MsgHandler} | Options0],
    case emqtt:start_link(Options) of
        {ok, Pid} ->
            {ok, _} = emqtt:connect(Pid),
            try subscribe(Pid, RequestTopic, QoS) of
                ok -> {ok, Pid};
                {error, _} = Error -> Error
            catch
                C : E : S ->
                    emqtt:stop(Pid),
                    {error, {C, E, S}}
            end;
        {error, _} = Error -> Error
    end.

stop(Pid) ->
    emqtt:disconnect(Pid).

make_msg_handler(RequestHandler, Parent) ->
    #{publish => fun(Msg) -> handle_msg(Msg, RequestHandler, Parent) end,
      puback => fun(_Ack) -> ok end,
      disconnected => fun(_Reason) -> ok end
     }.

handle_msg(ReqMsg, RequestHandler, Parent) ->
    #{qos := QoS, properties := Props, payload := ReqPayload} = ReqMsg,
    case maps:find('Response-Topic', Props) of
        {ok, RspTopic} when RspTopic =/= <<>> ->
            CorrData = maps:get('Correlation-Data', Props),
            RspProps = maps:without(['Response-Topic'], Props),
            RspPayload = RequestHandler(CorrData, ReqPayload),
            RspMsg = #mqtt_msg{qos = QoS,
                               topic = RspTopic,
                               props = RspProps,
                               payload = RspPayload
                              },
            emqx_logger:debug("~p sending response msg to topic ~s with~n"
                              "corr-data=~p~npayload=~p",
                              [?MODULE, RspTopic, CorrData, RspPayload]),
            ok = send_response(RspMsg);
        _ ->
            Parent ! {discarded, ReqPayload},
            ok
    end.

send_response(Msg) ->
    %% This function is evaluated by emqtt itself.
    %% hence delegate to another temp process for the loopback gen_statem call.
    Client = self(),
    _ = spawn_link(fun() ->
                           case emqtt:publish(Client, Msg) of
                               ok -> ok;
                               {ok, _} -> ok;
                               {error, Reason} -> exit({failed_to_publish_response, Reason})
                           end
                   end),
    ok.

subscribe(Client, Topic, QoS) ->
    {ok, _Props, _QoS} =
        emqtt:subscribe(Client, [{Topic, [{rh, 2}, {rap, false},
                                       {nl, true}, {qos, QoS}]}]),
    ok.
