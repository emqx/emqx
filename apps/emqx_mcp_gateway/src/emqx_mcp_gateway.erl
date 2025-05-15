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

-module(emqx_mcp_gateway).
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_mcp_errors.hrl").

-export([
    enable/0,
    disable/0,
    post_config_update/5
]).

-export([
    on_session_creating/1,
    on_client_connack/3,
    on_message_publish/1,
    on_session_subscribed/3
]).

-define(PROP_K_MCP_COMP_TYPE, <<"MCP-COMPONENT-TYPE">>).
-define(PROP_K_MCP_SERVER_NAME, <<"MCP-SERVER-NAME">>).
%%==============================================================================
%% Config update
%%==============================================================================
post_config_update([mcp], _UpdateReq, #{enable := false}, _OldConf, _AppEnvs) ->
    ?SLOG(debug, #{msg => "disabling_mcp_gateway"}),
    disable();
post_config_update([mcp], _UpdateReq, #{enable := true} = _NewConf, _OldConf, _AppEnvs) ->
    ?SLOG(debug, #{msg => "enabling_mcp_gateway"}),
    enable();
post_config_update([mcp, servers, Id], '$remove', undefined, OldConf, _AppEnvs) ->
    ?SLOG(debug, #{msg => "remove mcp server", id => Id}),
    ServerName = maps:get(server_name, OldConf),
    stop_mcp_server(ServerName);
post_config_update([mcp, servers, Id], _UpdateReq, NewConf, _OldConf, _AppEnvs) ->
    ?SLOG(debug, #{msg => "add or update mcp server", id => Id}),
    start_mcp_server(Id, NewConf).

%%==============================================================================
%% APIs
%%==============================================================================
enable() ->
    start_mcp_servers(emqx:get_config([mcp, servers])),
    register_hook(),
    emqx_ctl:register_command(mcp, {emqx_mcp_gateway_cli, cmd}).

disable() ->
    unregister_hook(),
    emqx_ctl:unregister_command(mcp),
    stop_mcp_servers(),
    %% Restart the dispatcher to clean up the state
    emqx_mcp_server_dispatcher:restart().

%%==============================================================================
%% Hooks
%%==============================================================================
on_session_creating(
    #{
        conninfo := #{username := Username} = ConnInfo,
        will_msg := WillMsg =
            #message{
                topic = <<"$mcp-server/presence/", ServerIdAndName/binary>>
            }
    } = ChannelData
) ->
    UserPropsConn = maps:get('User-Property', maps:get(conn_props, ConnInfo, #{}), []),
    case proplists:get_value(?PROP_K_MCP_COMP_TYPE, UserPropsConn) of
        <<"mcp-server">> ->
            {ServerId, ServerName} = split_id_and_server_name(ServerIdAndName),
            case get_broker_suggested_server_name(Username) of
                {ok, ServerName} ->
                    {ok, ChannelData};
                {ok, SuggestedName} ->
                    Topic1 =
                        <<"$mcp-server/presence/", ServerId/binary, "/", SuggestedName/binary>>,
                    {ok, ChannelData#{
                        conninfo => ConnInfo#{mcp_server_name => SuggestedName},
                        will_msg => WillMsg#message{
                            topic = Topic1
                        }
                    }};
                _ ->
                    {ok, ChannelData}
            end;
        ComponentType ->
            ?SLOG(warning, #{
                msg => mcp_server_component_property_not_set,
                ?PROP_K_MCP_COMP_TYPE => ComponentType
            }),
            {ok, ChannelData}
    end;
on_session_creating(ChannelData) ->
    {ok, ChannelData}.

on_client_connack(ConnInfo = #{mcp_server_name := SuggestedName}, success, ConnAckProps) ->
    UserPropsConn = maps:get('User-Property', maps:get(conn_props, ConnInfo, #{}), []),
    case proplists:get_value(?PROP_K_MCP_COMP_TYPE, UserPropsConn) of
        <<"mcp-server">> ->
            {ok, add_broker_suggested_server_name(SuggestedName, ConnAckProps)};
        _ ->
            {ok, ConnAckProps}
    end;
on_client_connack(_ConnInfo, _Rc, ConnAckProps) ->
    {ok, ConnAckProps}.

on_message_publish(#message{topic = <<"$mcp-server/capability", _/binary>>} = Message) ->
    %% ignore capability notifications sent by mcp server
    {ok, Message};
on_message_publish(#message{topic = <<"$mcp-server/presence", _/binary>>} = Message) ->
    %% ignore online/offline notifications sent by mcp server
    {ok, Message};
on_message_publish(
    #message{
        from = McpClientId,
        topic = <<"$mcp-server/", ServerIdAndName/binary>>,
        headers = Headers,
        payload = RawInitReq
    } = Message
) ->
    {_, ServerName} = split_id_and_server_name(ServerIdAndName),
    case emqx_mcp_message:decode_rpc_msg(RawInitReq) of
        {ok, #{type := json_rpc_request, method := <<"initialize">>, id := Id}} ->
            Credentials = #{username => maps:get(username, Headers, undefined)},
            send_initialize_request(Id, ServerName, McpClientId, Credentials, RawInitReq);
        {ok, #{type := json_rpc_request, method := Method, id := Id}} ->
            ErrMsg = emqx_mcp_message:json_rpc_error(
                Id,
                ?ERR_C_UNEXPECTED_METHOD,
                ?ERR_UNEXPECTED_METHOD,
                #{expected => <<"initialize">>, received => Method}
            ),
            emqx_mcp_message:publish_mcp_server_message(
                ServerName, McpClientId, rpc, #{}, ErrMsg
            );
        {ok, Msg} ->
            ?SLOG(error, #{msg => unsupported_mcp_server_msg, rpc_msg => Msg});
        {error, #{reason := Reason} = Details} ->
            D = maps:remove(reason, Details),
            ErrCode =
                case Reason of
                    ?ERR_INVALID_JSON -> ?ERR_C_INVALID_JSON;
                    ?ERR_MALFORMED_JSON_RPC -> ?ERR_C_MALFORMED_JSON_RPC
                end,
            ErrMsg = emqx_mcp_message:json_rpc_error(0, ErrCode, Reason, D),
            emqx_mcp_message:publish_mcp_server_message(
                ServerName, McpClientId, rpc, #{}, ErrMsg
            )
    end,
    {ok, Message};
on_message_publish(
    #message{
        from = McpClientId,
        topic = <<"$mcp-client/presence/", McpClientId/binary>>,
        payload = PresenceMsg
    } = Message
) ->
    case emqx_mcp_message:decode_rpc_msg(PresenceMsg) of
        {ok, #{method := <<"notifications/disconnected">>}} ->
            ServerNamePids = get_mcp_server_name_pid_mapping(),
            ServerNames = maps:keys(ServerNamePids),
            lists:foreach(
                fun(ServerName) ->
                    _ = maybe_call_mcp_server(ServerName, client_disconnected)
                end,
                ServerNames
            ),
            ok;
        {ok, Msg} ->
            ?SLOG(error, #{msg => unsupported_client_presence_msg, rpc_msg => Msg});
        {error, Reason} ->
            ?SLOG(error, #{msg => decode_rpc_msg_failed, reason => Reason})
    end,
    {ok, Message};
on_message_publish(
    #message{
        from = McpClientId,
        topic = <<"$mcp-client/capability/list-changed/", McpClientId/binary>>,
        payload = ListChangedNotify
    } = Message
) ->
    ServerNamePids = get_mcp_server_name_pid_mapping(),
    ServerNames = maps:keys(ServerNamePids),
    lists:foreach(
        fun(ServerName) ->
            _ = maybe_call_mcp_server(ServerName, {rpc, ListChangedNotify})
        end,
        ServerNames
    ),
    {ok, Message};
on_message_publish(
    #message{
        from = McpClientId,
        topic = <<"$mcp-rpc-endpoint/", ClientIdAndServerName/binary>>,
        payload = RpcMsg
    } = Message
) ->
    {_, ServerName} = split_id_and_server_name(ClientIdAndServerName),
    case maybe_call_mcp_server(ServerName, {rpc, RpcMsg}) of
        {error, Reason} ->
            case emqx_mcp_message:decode_rpc_msg(RpcMsg) of
                {ok, #{type := json_rpc_request, id := Id}} ->
                    ErrMsg = error_to_rpc_msg(Id, Reason),
                    emqx_mcp_message:publish_mcp_server_message(
                        ServerName, McpClientId, rpc, #{}, ErrMsg
                    );
                {error, Reason} ->
                    ?SLOG(error, #{msg => decode_rpc_msg_failed, reason => Reason})
            end;
        _ ->
            ok
    end,
    {ok, Message};
on_message_publish(Message) ->
    %% Ignore other messages
    {ok, Message}.

on_session_subscribed(_, <<"$mcp-server/presence/", ServerIdAndName/binary>> = _Topic, _SubOpts) ->
    {_, ServerNameFilter} =
        case string:split(ServerIdAndName, <<"/">>) of
            [Id, ServerName] -> {Id, ServerName};
            [ServerName] -> {undefined, ServerName};
            _ -> throw({error, {invalid_server_name_filter, ServerIdAndName}})
        end,
    maps:foreach(
        fun(_, #{enable := true, server_name := ServerName} = ServerConf) ->
            case emqx_topic:match(ServerName, ServerNameFilter) of
                true ->
                    ServerDesc = maps:get(server_desc, ServerConf, <<>>),
                    ServerMeta = server_meta(ServerName),
                    emqx_mcp_message:send_server_online_message(ServerName, ServerDesc, ServerMeta);
                false ->
                    ok
            end
        end,
        emqx:get_config([mcp, servers])
    ),
    ok;
on_session_subscribed(_, _Topic, _SubOpts) ->
    %% Ignore other topics
    ok.

add_broker_suggested_server_name(SuggestedName, ConnAckProps) ->
    UserPropsConnAck = maps:get('User-Property', ConnAckProps, []),
    UserPropsConnAck1 = [{?PROP_K_MCP_SERVER_NAME, SuggestedName} | UserPropsConnAck],
    ConnAckProps#{'User-Property' => UserPropsConnAck1}.

split_id_and_server_name(Str) ->
    %% Split the server ID and name from the topic
    case string:split(Str, <<"/">>) of
        [Id, ServerName] -> {Id, ServerName};
        _ -> throw({error, {invalid_id_and_server_name, Str}})
    end.

get_broker_suggested_server_name(undefined) ->
    undefined;
get_broker_suggested_server_name(Username) ->
    emqx_mcp_server_name_manager:get_server_name(Username).

register_hook() ->
    hook('session.creating', {?MODULE, on_session_creating, []}),
    hook('client.connack', {?MODULE, on_client_connack, []}),
    hook('message.publish', {?MODULE, on_message_publish, []}),
    hook('session.subscribed', {?MODULE, on_session_subscribed, []}),
    ok.

unregister_hook() ->
    unhook('session.creating', {?MODULE, on_session_creating}),
    unhook('client.connack', {?MODULE, on_client_connack}),
    unhook('message.publish', {?MODULE, on_message_publish}),
    unhook('session.subscribed', {?MODULE, on_session_subscribed}),
    ok.

hook(HookPoint, MFA) ->
    %% Higher priority than retainer, make it possible to handle mcp service discovery
    %% messages in this module rather than in emqx_retainer.
    Priority = ?HP_RETAINER + 1,
    ok = emqx_hooks:put(HookPoint, MFA, Priority).

unhook(HookPoint, MFA) ->
    ok = emqx_hooks:del(HookPoint, MFA).

%%==============================================================================
%% Internal functions
%%==============================================================================
start_mcp_servers(Servers) ->
    %% Start all MCP servers
    maps:foreach(fun start_mcp_server/2, Servers).

start_mcp_server(
    Name, #{enable := true, server_type := SType, server_name := ServerName} = ServerConf
) ->
    Conf = #{
        name => Name,
        server_name => ServerName,
        server_conf => maps:without([server_type, enable], ServerConf),
        mod => mcp_server_callback_module(SType),
        opts => #{}
    },
    ok = emqx_mcp_server_dispatcher:stop_servers(ServerName),
    ok = emqx_mcp_server_dispatcher:start_listening_servers(Conf);
start_mcp_server(_Name, #{enable := false}) ->
    ok.

stop_mcp_servers() ->
    emqx_mcp_server:stop_supervised_all().

stop_mcp_server(ServerName) ->
    emqx_mcp_server_dispatcher:stop_servers(ServerName).

mcp_server_callback_module(stdio) ->
    emqx_mcp_server_stdio;
mcp_server_callback_module(http) ->
    emqx_mcp_server_http;
mcp_server_callback_module(internal) ->
    emqx_mcp_server_internal;
mcp_server_callback_module(SType) ->
    throw({error, {invalid_mcp_server_type, SType}}).

server_meta(ServerName) ->
    case emqx_mcp_authorization:get_roles(ServerName) of
        {ok, Roles} ->
            #{
                <<"authorization">> => #{
                    <<"roles">> => Roles
                }
            };
        _ ->
            #{}
    end.

send_initialize_request(Id, ServerName, McpClientId, Credentials, RawInitReq) ->
    case
        emqx_mcp_server_dispatcher:initialize(ServerName, McpClientId, Credentials, Id, RawInitReq)
    of
        {ok, #{raw_response := Resp, server_pid := ServerPid}} ->
            register_mcp_server_pid(ServerName, ServerPid),
            emqx_mcp_message:publish_mcp_server_message(ServerName, McpClientId, rpc, #{}, Resp);
        {json_rpc_error, ErrMsg} ->
            emqx_mcp_message:publish_mcp_server_message(ServerName, McpClientId, rpc, #{}, ErrMsg);
        {error, Reason} ->
            ErrMsg = error_to_rpc_msg(Id, Reason),
            emqx_mcp_message:publish_mcp_server_message(ServerName, McpClientId, rpc, #{}, ErrMsg)
    end.

error_to_rpc_msg(Id, Reason) when is_atom(Reason) ->
    emqx_mcp_message:json_rpc_error(Id, ?ERR_CODE(Reason), Reason, #{});
error_to_rpc_msg(Id, #{reason := Reason} = Details) when is_atom(Reason) ->
    emqx_mcp_message:json_rpc_error(Id, ?ERR_CODE(Reason), Reason, maps:remove(reason, Details)).

register_mcp_server_pid(ServerName, ServerPid) ->
    ServerNamePids = get_mcp_server_name_pid_mapping(),
    erlang:put(mcp_server_pid, ServerNamePids#{ServerName => ServerPid}).

get_mcp_server_pid(ServerName) ->
    ServerNamePids = get_mcp_server_name_pid_mapping(),
    maps:find(ServerName, ServerNamePids).

get_mcp_server_name_pid_mapping() ->
    case erlang:get(mcp_server_pid) of
        undefined -> #{};
        ServerNamePids -> ServerNamePids
    end.

maybe_call_mcp_server(ServerName, Request) ->
    case get_mcp_server_pid(ServerName) of
        {ok, ServerPid} ->
            emqx_mcp_server:safe_call(ServerPid, Request, infinity);
        _ ->
            %% ignore if no server running
            ok
    end.
