-module(emqx_mcp_gateway).
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    enable/0,
    disable/0,
    list/0,
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
post_config_update(_KeyPath, _UpdateReq, #{enable := false}, _OldConf, _AppEnvs) ->
    ?SLOG(debug, #{msg => "disabling_mcp_gateway"}),
    disable();
post_config_update(_KeyPath, _UpdateReq, #{enable := true} = _NewConf, _OldConf, _AppEnvs) ->
    ?SLOG(debug, #{msg => "enabling_mcp_gateway"}),
    enable().

%%==============================================================================
%% APIs
%%==============================================================================
enable() ->
    start_mcp_servers(emqx:get_config([mcp, servers])),
    register_hook(),
    emqx_conf:add_handler([mcp], emqx_mcp_gateway),
    emqx_ctl:register_command(mcp, {emqx_mcp_gateway_cli, cmd}).

disable() ->
    unregister_hook(),
    emqx_conf:remove_handler([mcp]),
    emqx_ctl:unregister_command(mcp),
    stop_mcp_servers(),
    %% Restart the dispatcher to clean up the state
    emqx_mcp_server_dispatcher:restart().

list() ->
    emqx:get_raw_config([mcp], []).

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
            {ServerId, ServerName} = split_server_id_and_name(ServerIdAndName),
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

on_message_publish(#message{topic = <<"$mcp", _/binary>>} = Message) ->
    {ok, Message};
on_message_publish(Message) ->
    %% Ignore other messages
    {ok, Message}.

on_session_subscribed(_, <<"$mcp", _/binary>> = _Topic, _SubOpts) ->
    ok;
on_session_subscribed(_, _Topic, _SubOpts) ->
    %% Ignore other topics
    ok.

add_broker_suggested_server_name(SuggestedName, ConnAckProps) ->
    UserPropsConnAck = maps:get('User-Property', ConnAckProps, []),
    UserPropsConnAck1 = [{?PROP_K_MCP_SERVER_NAME, SuggestedName} | UserPropsConnAck],
    ConnAckProps#{'User-Property' => UserPropsConnAck1}.

split_server_id_and_name(ServerIdAndName) ->
    %% Split the server ID and name from the topic
    case string:split(ServerIdAndName, <<"/">>) of
        [ServerId, ServerName] -> {ServerId, ServerName};
        _ -> throw({error, {invalid_server_id_and_name, ServerIdAndName}})
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
    maps:foreach(
        fun(Name, #{enable := true} = ServerConf) ->
            #{server_type := SType, server_name := ServerName} = ServerConf,
            Conf = #{
                name => Name,
                server_name => ServerName,
                server_conf => maps:without([server_type, enable], ServerConf),
                mod => mcp_server_callback_module(SType),
                opts => #{}
            },
            ok = emqx_mcp_server_dispatcher:start_server_pool(Conf)
        end,
        Servers
    ).

stop_mcp_servers() ->
    emqx_mcp_server:stop_supervised_all().

mcp_server_callback_module(stdio) ->
    emqx_mcp_server_stdio;
mcp_server_callback_module(http) ->
    emqx_mcp_server_http;
mcp_server_callback_module(internal) ->
    emqx_mcp_server_internal;
mcp_server_callback_module(SType) ->
    throw({error, {invalid_mcp_server_type, SType}}).
