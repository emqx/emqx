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

%% a coap to mqtt adapter with a retained topic message database
-module(emqx_coap_pubsub_handler).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include("src/coap/include/emqx_coap.hrl").

-export([handle_request/4]).

-import(emqx_coap_message, [response/2, response/3]).
-import(emqx_coap_medium, [reply/2, reply/3]).
-import(emqx_coap_channel, [run_hooks/3]).

-define(UNSUB(Topic, Msg), #{subscribe => {Topic, Msg}}).
-define(SUB(Topic, Token, Msg), #{subscribe => {{Topic, Token}, Msg}}).
-define(SUBOPTS, #{qos => 0, rh => 1, rap => 0, nl => 0, is_new => false}).

%% TODO maybe can merge this code into emqx_coap_session, simplify the call chain

handle_request(Path, #coap_message{method = Method} = Msg, Ctx, CInfo) ->
    case check_topic(Path) of
        {ok, Topic} ->
            handle_method(Method, Topic, Msg, Ctx, CInfo);
        _ ->
            reply({error, bad_request}, <<"invalid topic">>, Msg)
    end.

handle_method(get, Topic, Msg, Ctx, CInfo) ->
    case emqx_coap_message:get_option(observe, Msg) of
        0 ->
            subscribe(Msg, Topic, Ctx, CInfo);
        1 ->
            unsubscribe(Msg, Topic, Ctx, CInfo);
        _ ->
            reply({error, bad_request}, <<"invalid observe value">>, Msg)
    end;
handle_method(post, Topic, #coap_message{payload = Payload} = Msg, Ctx, CInfo) ->
    case emqx_coap_channel:validator(publish, Topic, Ctx, CInfo) of
        allow ->
            #{clientid := ClientId} = CInfo,
            MountTopic = mount(CInfo, Topic),
            QOS = get_publish_qos(Msg),
            %% TODO: Append message metadata into headers
            MQTTMsg = emqx_message:make(ClientId, QOS, MountTopic, Payload),
            MQTTMsg2 = apply_publish_opts(Msg, MQTTMsg),
            _ = emqx_broker:publish(MQTTMsg2),
            reply({ok, changed}, Msg);
        _ ->
            reply({error, unauthorized}, Msg)
    end;
handle_method(_, _, Msg, _, _) ->
    reply({error, method_not_allowed}, Msg).

check_topic([]) ->
    error;
check_topic(Path) ->
    Sep = <<"/">>,
    {ok,
        emqx_http_lib:uri_decode(
            lists:foldl(
                fun(Part, Acc) ->
                    <<Acc/binary, Sep/binary, Part/binary>>
                end,
                <<>>,
                Path
            )
        )}.

get_sub_opts(#coap_message{options = Opts} = Msg) ->
    SubOpts = maps:fold(fun parse_sub_opts/3, #{}, Opts),
    case SubOpts of
        #{qos := _} ->
            maps:merge(SubOpts, ?SUBOPTS);
        _ ->
            CfgType = emqx_conf:get([gateway, coap, subscribe_qos], ?QOS_0),
            maps:merge(SubOpts, ?SUBOPTS#{qos => type_to_qos(CfgType, Msg)})
    end.

parse_sub_opts(<<"qos">>, V, Opts) ->
    Opts#{qos => erlang:binary_to_integer(V)};
parse_sub_opts(<<"nl">>, V, Opts) ->
    Opts#{nl => erlang:binary_to_integer(V)};
parse_sub_opts(<<"rh">>, V, Opts) ->
    Opts#{rh => erlang:binary_to_integer(V)};
parse_sub_opts(_, _, Opts) ->
    Opts.

type_to_qos(qos0, _) ->
    ?QOS_0;
type_to_qos(qos1, _) ->
    ?QOS_1;
type_to_qos(qos2, _) ->
    ?QOS_2;
type_to_qos(coap, #coap_message{type = Type}) ->
    case Type of
        non ->
            ?QOS_0;
        _ ->
            ?QOS_1
    end.

get_publish_qos(Msg) ->
    case emqx_coap_message:get_option(uri_query, Msg) of
        #{<<"qos">> := QOS} ->
            erlang:binary_to_integer(QOS);
        _ ->
            CfgType = emqx_conf:get([gateway, coap, publish_qos], ?QOS_0),
            type_to_qos(CfgType, Msg)
    end.

apply_publish_opts(Msg, MQTTMsg) ->
    maps:fold(
        fun
            (<<"retain">>, V, Acc) ->
                Val = erlang:binary_to_atom(V),
                emqx_message:set_flag(retain, Val, Acc);
            (<<"expiry">>, V, Acc) ->
                Val = erlang:binary_to_integer(V),
                Props = emqx_message:get_header(properties, Acc),
                emqx_message:set_header(
                    properties,
                    Props#{'Message-Expiry-Interval' => Val},
                    Acc
                );
            (_, _, Acc) ->
                Acc
        end,
        MQTTMsg,
        emqx_coap_message:get_option(uri_query, Msg)
    ).

subscribe(#coap_message{token = <<>>} = Msg, _, _, _) ->
    reply({error, bad_request}, <<"observe without token">>, Msg);
subscribe(#coap_message{token = Token} = Msg, Topic, Ctx, CInfo) ->
    case emqx_coap_channel:validator(subscribe, Topic, Ctx, CInfo) of
        allow ->
            #{clientid := ClientId} = CInfo,
            SubOpts = get_sub_opts(Msg),
            MountTopic = mount(CInfo, Topic),
            emqx_broker:subscribe(MountTopic, ClientId, SubOpts),
            run_hooks(Ctx, 'session.subscribed', [CInfo, MountTopic, SubOpts]),
            ?SUB(MountTopic, Token, Msg);
        _ ->
            reply({error, unauthorized}, Msg)
    end.

unsubscribe(Msg, Topic, Ctx, CInfo) ->
    MountTopic = mount(CInfo, Topic),
    emqx_broker:unsubscribe(MountTopic),
    run_hooks(Ctx, 'session.unsubscribed', [CInfo, Topic, ?SUBOPTS]),
    ?UNSUB(MountTopic, Msg).

mount(#{mountpoint := Mountpoint}, Topic) ->
    <<Mountpoint/binary, Topic/binary>>.
