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

-module(emqx_connector_mqtt_msg).

-export([
    to_binary/1,
    from_binary/1,
    make_pub_vars/2,
    to_remote_msg/2,
    to_broker_msg/3,
    estimate_size/1
]).

-export([
    replace_vars_in_str/2,
    replace_simple_var/2
]).

-export_type([msg/0]).

-include_lib("emqx/include/emqx.hrl").

-include_lib("emqtt/include/emqtt.hrl").

-type msg() :: emqx_types:message().
-type exp_msg() :: emqx_types:message() | #mqtt_msg{}.
-type remote_config() :: #{
    topic := binary(),
    qos := original | integer(),
    retain := original | boolean(),
    payload := binary()
}.
-type variables() :: #{
    mountpoint := undefined | binary(),
    remote := remote_config()
}.

make_pub_vars(_, undefined) ->
    undefined;
make_pub_vars(Mountpoint, Conf) when is_map(Conf) ->
    Conf#{mountpoint => Mountpoint}.

%% @doc Make export format:
%% 1. Mount topic to a prefix
%% 2. Fix QoS to 1
%% @end
%% Shame that we have to know the callback module here
%% would be great if we can get rid of #mqtt_msg{} record
%% and use #message{} in all places.
-spec to_remote_msg(msg() | map(), variables()) ->
    exp_msg().
to_remote_msg(#message{flags = Flags0} = Msg, Vars) ->
    Retain0 = maps:get(retain, Flags0, false),
    {Columns, _} = emqx_rule_events:eventmsg_publish(Msg),
    MapMsg = maps:put(retain, Retain0, Columns),
    to_remote_msg(MapMsg, Vars);
to_remote_msg(MapMsg, #{
    remote := #{
        topic := TopicToken,
        payload := PayloadToken,
        qos := QoSToken,
        retain := RetainToken
    },
    mountpoint := Mountpoint
}) when is_map(MapMsg) ->
    Topic = replace_vars_in_str(TopicToken, MapMsg),
    Payload = process_payload(PayloadToken, MapMsg),
    QoS = replace_simple_var(QoSToken, MapMsg),
    Retain = replace_simple_var(RetainToken, MapMsg),
    PubProps = maps:get(pub_props, MapMsg, #{}),
    #mqtt_msg{
        qos = QoS,
        retain = Retain,
        topic = topic(Mountpoint, Topic),
        props = emqx_misc:pub_props_to_packet(PubProps),
        payload = Payload
    };
to_remote_msg(#message{topic = Topic} = Msg, #{mountpoint := Mountpoint}) ->
    Msg#message{topic = topic(Mountpoint, Topic)}.

%% published from remote node over a MQTT connection
to_broker_msg(Msg, Vars, undefined) ->
    to_broker_msg(Msg, Vars, #{});
to_broker_msg(
    #{dup := Dup} = MapMsg,
    #{
        local := #{
            topic := TopicToken,
            payload := PayloadToken,
            qos := QoSToken,
            retain := RetainToken
        },
        mountpoint := Mountpoint
    },
    Props
) ->
    Topic = replace_vars_in_str(TopicToken, MapMsg),
    Payload = process_payload(PayloadToken, MapMsg),
    QoS = replace_simple_var(QoSToken, MapMsg),
    Retain = replace_simple_var(RetainToken, MapMsg),
    PubProps = maps:get(pub_props, MapMsg, #{}),
    set_headers(
        Props#{properties => emqx_misc:pub_props_to_packet(PubProps)},
        emqx_message:set_flags(
            #{dup => Dup, retain => Retain},
            emqx_message:make(bridge, QoS, topic(Mountpoint, Topic), Payload)
        )
    ).

process_payload([], Msg) ->
    emqx_json:encode(Msg);
process_payload(Tks, Msg) ->
    replace_vars_in_str(Tks, Msg).

%% Replace a string contains vars to another string in which the placeholders are replace by the
%% corresponding values. For example, given "a: ${var}", if the var=1, the result string will be:
%% "a: 1".
replace_vars_in_str(Tokens, Data) when is_list(Tokens) ->
    emqx_plugin_libs_rule:proc_tmpl(Tokens, Data, #{return => full_binary});
replace_vars_in_str(Val, _Data) ->
    Val.

%% Replace a simple var to its value. For example, given "${var}", if the var=1, then the result
%% value will be an integer 1.
replace_simple_var(Tokens, Data) when is_list(Tokens) ->
    [Var] = emqx_plugin_libs_rule:proc_tmpl(Tokens, Data, #{return => rawlist}),
    Var;
replace_simple_var(Val, _Data) ->
    Val.

%% @doc Make `binary()' in order to make iodata to be persisted on disk.
-spec to_binary(msg()) -> binary().
to_binary(Msg) -> term_to_binary(Msg).

%% @doc Unmarshal binary into `msg()'.
-spec from_binary(binary()) -> msg().
from_binary(Bin) -> binary_to_term(Bin).

%% @doc Estimate the size of a message.
%% Count only the topic length + payload size
%% There is no topic and payload for event message. So count all `Msg` term
-spec estimate_size(msg()) -> integer().
estimate_size(#message{topic = Topic, payload = Payload}) ->
    size(Topic) + size(Payload);
estimate_size(#{topic := Topic, payload := Payload}) ->
    size(Topic) + size(Payload);
estimate_size(Term) ->
    erlang:external_size(Term).

set_headers(Val, Msg) ->
    emqx_message:set_headers(Val, Msg).
topic(undefined, Topic) -> Topic;
topic(Prefix, Topic) -> emqx_topic:prepend(Prefix, Topic).
