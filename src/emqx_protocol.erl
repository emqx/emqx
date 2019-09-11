%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% MQTT Protocol
-module(emqx_protocol).

-include("types.hrl").
-include("emqx_mqtt.hrl").

-export([ init/2
        , info/1
        , info/2
        , attrs/1
        ]).

-export([ find_alias/2
        , save_alias/3
        , clear_will_msg/1
        ]).

-export_type([protocol/0]).

-record(protocol, {
          %% MQTT Proto Name
          proto_name :: binary(),
          %% MQTT Proto Version
          proto_ver :: emqx_types:ver(),
          %% Clean Start Flag
          clean_start :: boolean(),
          %% MQTT Keepalive interval
          keepalive :: non_neg_integer(),
          %% ClientId in CONNECT Packet
          client_id :: emqx_types:client_id(),
          %% Username in CONNECT Packet
          username :: emqx_types:username(),
          %% MQTT Will Msg
          will_msg :: emqx_types:message(),
          %% MQTT Topic Aliases
          topic_aliases :: maybe(map()),
          %% MQTT Topic Alias Maximum
          alias_maximum :: maybe(map())
         }).

-opaque(protocol() :: #protocol{}).

-define(INFO_KEYS, record_info(fields, protocol)).

-define(ATTR_KEYS, [proto_name, proto_ver, clean_start, keepalive]).

-spec(init(#mqtt_packet_connect{}, atom()) -> protocol()).
init(#mqtt_packet_connect{proto_name  = ProtoName,
                          proto_ver   = ProtoVer,
                          clean_start = CleanStart,
                          keepalive   = Keepalive,
                          properties  = Properties,
                          client_id   = ClientId,
                          username    = Username} = ConnPkt, Zone) ->
    WillMsg = emqx_packet:will_msg(ConnPkt),
    #protocol{proto_name    = ProtoName,
              proto_ver     = ProtoVer,
              clean_start   = CleanStart,
              keepalive     = Keepalive,
              client_id     = ClientId,
              username      = Username,
              will_msg      = WillMsg,
              alias_maximum = #{outbound => get_property('Topic-Alias-Maximum', Properties, 0),
                                inbound => maps:get(max_topic_alias, emqx_mqtt_caps:get_caps(Zone), 0)}
             }.

-spec(info(protocol()) -> emqx_types:infos()).
info(Proto) ->
    maps:from_list(info(?INFO_KEYS, Proto)).

-spec(info(atom()|list(atom()), protocol()) -> term()).
info(Keys, Proto) when is_list(Keys) ->
    [{Key, info(Key, Proto)} || Key <- Keys];
info(proto_name, #protocol{proto_name = ProtoName}) ->
    ProtoName;
info(proto_ver, #protocol{proto_ver = ProtoVer}) ->
    ProtoVer;
info(clean_start, #protocol{clean_start = CleanStart}) ->
    CleanStart;
info(keepalive, #protocol{keepalive = Keepalive}) ->
    Keepalive;
info(client_id, #protocol{client_id = ClientId}) ->
    ClientId;
info(username, #protocol{username = Username}) ->
    Username;
info(will_msg, #protocol{will_msg = WillMsg}) ->
    WillMsg;
info(will_delay_interval, #protocol{will_msg = undefined}) ->
    0;
info(will_delay_interval, #protocol{will_msg = WillMsg}) ->
    emqx_message:get_header('Will-Delay-Interval', WillMsg, 0);
info(topic_aliases, #protocol{topic_aliases = Aliases}) ->
    Aliases;
info(alias_maximum, #protocol{alias_maximum = AliasMaximum}) ->
    AliasMaximum.

-spec(attrs(protocol()) -> emqx_types:attrs()).
attrs(Proto) ->
    maps:from_list(info(?ATTR_KEYS, Proto)).

-spec(find_alias(emqx_types:alias_id(), protocol())
      -> {ok, emqx_types:topic()} | false).
find_alias(_AliasId, #protocol{topic_aliases = undefined}) ->
    false;
find_alias(AliasId, #protocol{topic_aliases = Aliases}) ->
    maps:find(AliasId, Aliases).

-spec(save_alias(emqx_types:alias_id(), emqx_types:topic(), protocol())
      -> protocol()).
save_alias(AliasId, Topic, Proto = #protocol{topic_aliases = undefined}) ->
    Proto#protocol{topic_aliases = #{AliasId => Topic}};
save_alias(AliasId, Topic, Proto = #protocol{topic_aliases = Aliases}) ->
    Proto#protocol{topic_aliases = maps:put(AliasId, Topic, Aliases)}.

clear_will_msg(Protocol) ->
    Protocol#protocol{will_msg = undefined}.

get_property(_Name, undefined, Default) ->
    Default;
get_property(Name, Props, Default) ->
    maps:get(Name, Props, Default).
