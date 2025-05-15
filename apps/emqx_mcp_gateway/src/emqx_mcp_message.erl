%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mcp_message).
-include("emqx_mcp_gateway.hrl").
-include("emqx_mcp_errors.hrl").

-export([
    json_rpc_request/3,
    json_rpc_response/2,
    json_rpc_error/4,
    json_rpc_notification/1,
    json_rpc_notification/2
]).

-export([
    initialize_request/2,
    initialize_request/3,
    initialize_response/3,
    initialized_notification/0
]).

-export([decode_rpc_msg/1, topic_type_of_rpc_msg/1, get_topic/2]).

-export([
    send_server_online_message/3,
    send_server_offline_message/1,
    publish_mcp_server_message/5
]).

%%==============================================================================
%% MCP Requests/Responses/Notifications
%%==============================================================================
initialize_request(ClientInfo, Capabilities) ->
    initialize_request(1, ClientInfo, Capabilities).

initialize_request(Id, ClientInfo, Capabilities) ->
    json_rpc_request(
        Id,
        <<"initialize">>,
        #{
            <<"protocolVersion">> => ?MCP_VERSION,
            <<"clientInfo">> => ClientInfo,
            <<"capabilities">> => Capabilities
        }
    ).

initialize_response(Id, ServerInfo, Capabilities) ->
    json_rpc_response(
        Id,
        #{
            <<"protocolVersion">> => ?MCP_VERSION,
            <<"serverInfo">> => ServerInfo,
            <<"capabilities">> => Capabilities
        }
    ).

initialized_notification() ->
    json_rpc_notification(<<"notifications/initialized">>).

%%==============================================================================
%% JSON RPC Messages
%%==============================================================================
json_rpc_request(Id, Method, Params) ->
    emqx_utils_json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"method">> => Method,
        <<"params">> => Params,
        <<"id">> => Id
    }).

json_rpc_response(Id, Result) ->
    emqx_utils_json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"result">> => Result,
        <<"id">> => Id
    }).

json_rpc_notification(Method) ->
    emqx_utils_json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"method">> => Method
    }).

json_rpc_notification(Method, Params) ->
    emqx_utils_json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"method">> => Method,
        <<"params">> => Params
    }).

json_rpc_error(Id, Code, Message, Data) ->
    emqx_utils_json:encode(#{
        <<"jsonrpc">> => <<"2.0">>,
        <<"error">> => #{
            <<"code">> => Code,
            <<"message">> => Message,
            <<"data">> => Data
        },
        <<"id">> => Id
    }).

decode_rpc_msg(Msg) ->
    try emqx_utils_json:decode(Msg) of
        #{
            <<"jsonrpc">> := <<"2.0">>,
            <<"method">> := Method,
            <<"params">> := Params,
            <<"id">> := Id
        } ->
            {ok, #{type => json_rpc_request, method => Method, id => Id, params => Params}};
        #{<<"jsonrpc">> := <<"2.0">>, <<"result">> := Result, <<"id">> := Id} ->
            {ok, #{type => json_rpc_response, id => Id, result => Result}};
        #{<<"jsonrpc">> := <<"2.0">>, <<"error">> := Error, <<"id">> := Id} ->
            {ok, #{type => json_rpc_error, id => Id, error => Error}};
        #{<<"jsonrpc">> := <<"2.0">>, <<"method">> := Method, <<"params">> := Params} ->
            {ok, #{type => json_rpc_notification, method => Method, params => Params}};
        #{<<"jsonrpc">> := <<"2.0">>, <<"method">> := Method} ->
            {ok, #{type => json_rpc_notification, method => Method}};
        Msg1 ->
            {error, #{reason => ?ERR_MALFORMED_JSON_RPC, msg => Msg1}}
    catch
        error:Reason ->
            {error, #{reason => ?ERR_INVALID_JSON, msg => Msg, details => Reason}}
    end.

topic_type_of_rpc_msg(Msg) when is_binary(Msg) ->
    case decode_rpc_msg(Msg) of
        {ok, RpcMsg} ->
            topic_type_of_rpc_msg(RpcMsg);
        {error, _} = Err ->
            Err
    end;
topic_type_of_rpc_msg(#{method := <<"initialize">>}) ->
    server_control;
topic_type_of_rpc_msg(#{method := <<"notifications/resources/updated">>}) ->
    server_resources_updated;
topic_type_of_rpc_msg(#{method := <<"notifications/server/online">>}) ->
    server_presence;
topic_type_of_rpc_msg(#{method := <<"notifications/disconnected">>}) ->
    client_presence;
topic_type_of_rpc_msg(#{method := <<"notifications/roots/list_changed">>}) ->
    client_capability_list_changed;
topic_type_of_rpc_msg(#{type := json_rpc_notification, method := Method}) ->
    case string:find(Method, <<"/list_changed">>) of
        <<"/list_changed">> -> server_capability_list_changed;
        _ -> rpc
    end;
topic_type_of_rpc_msg(_) ->
    rpc.

get_topic(server_control, #{server_id := ServerId, server_name := ServerName}) ->
    <<"$mcp-server/", ServerId/binary, "/", ServerName/binary>>;
get_topic(server_capability_list_changed, #{server_id := ServerId, server_name := ServerName}) ->
    <<"$mcp-server/capability/list-changed/", ServerId/binary, "/", ServerName/binary>>;
get_topic(server_resources_updated, #{server_id := ServerId, server_name := ServerName}) ->
    <<"$mcp-server/capability/resource-updated/", ServerId/binary, "/", ServerName/binary>>;
get_topic(server_presence, #{server_id := ServerId, server_name := ServerName}) ->
    <<"$mcp-server/presence/", ServerId/binary, "/", ServerName/binary>>;
get_topic(client_presence, #{mcp_clientid := McpClientId}) ->
    <<"$mcp-client/presence/", McpClientId/binary>>;
get_topic(client_capability_list_changed, #{mcp_clientid := McpClientId}) ->
    <<"$mcp-client/capability/list-changed/", McpClientId/binary>>;
get_topic(rpc, #{mcp_clientid := McpClientId, server_name := ServerName}) ->
    <<"$mcp-rpc-endpoint/", McpClientId/binary, "/", ServerName/binary>>.

send_server_online_message(ServerName, ServerDesc, ServerMeta) ->
    Payload = json_rpc_notification(<<"notifications/server/online">>, #{
        <<"server_name">> => ServerName,
        <<"description">> => ServerDesc,
        <<"meta">> => ServerMeta
    }),
    publish_mcp_server_message(ServerName, undefined, server_presence, #{}, Payload).

send_server_offline_message(ServerName) ->
    %% No payload for offline message
    Payload = <<>>,
    publish_mcp_server_message(ServerName, undefined, server_presence, #{}, Payload).

publish_mcp_server_message(ServerName, McpClientId, TopicType, Flags, Payload) ->
    ServerId = ?MCP_SERVER_ID(ServerName),
    Topic = get_topic(TopicType, #{
        server_id => ServerId,
        server_name => ServerName,
        mcp_clientid => McpClientId
    }),
    UserProps = [
        {<<"MCP-COMPONENT-TYPE">>, <<"mcp-server">>},
        {<<"MCP-MQTT-CLIENT-ID">>, ServerId}
    ],
    Headers = #{
        properties => #{
            'User-Property' => UserProps
        }
    },
    QoS = 1,
    MqttMsg = emqx_message:make(ServerId, QoS, Topic, Payload, Flags, Headers),
    _ = emqx:publish(MqttMsg),
    ok.
