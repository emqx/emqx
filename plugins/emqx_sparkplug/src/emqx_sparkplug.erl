%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_sparkplug).

%% API
-export([
    publish_nbirth/2,
    publish_dbirth/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_sparkplug.hrl").
-include_lib("emqx_schema_registry/include/emqx_schema_registry_internal_spb.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-type nbirth() :: #nbirth{}.
-type dbirth() :: #dbirth{}.
-type message() :: emqx_types:message().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec publish_nbirth(nbirth(), message()) -> ok.
publish_nbirth(NBirth, OriginalMsg) ->
    #nbirth{
        namespace = Namespace,
        group_id = GroupId,
        edge_node_id = EdgeNodeId
    } = NBirth,
    #message{from = ClientId, payload = Payload} = OriginalMsg,
    Topic = emqx_topic:join([?SPB_CERT_PREFIX, Namespace, GroupId, ~"NBIRTH", EdgeNodeId]),
    Flags = #{retain => true},
    QoS = 2,
    Headers = #{},
    Msg = emqx_message:make(ClientId, QoS, Topic, Payload, Flags, Headers),
    _ = emqx_broker:publish(Msg),
    ok.

-spec publish_dbirth(dbirth(), message()) -> ok.
publish_dbirth(DBirth, OriginalMsg) ->
    #dbirth{
        namespace = Namespace,
        group_id = GroupId,
        edge_node_id = EdgeNodeId,
        device_id = DeviceId
    } = DBirth,
    #message{from = ClientId, payload = Payload} = OriginalMsg,
    Topic = emqx_topic:join([?SPB_CERT_PREFIX, Namespace, GroupId, ~"DBIRTH", EdgeNodeId, DeviceId]),
    Flags = #{retain => true},
    QoS = 2,
    Headers = #{},
    Msg = emqx_message:make(ClientId, QoS, Topic, Payload, Flags, Headers),
    _ = emqx_broker:publish(Msg),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
