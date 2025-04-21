-module(emqx_mcp_server_stdio).

-include("emqx_mcp_gateway.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    connect_server/1,
    handle_initialize_response/2,
    handle_info/2,
    handle_close/1
]).

-export([
    send_initialize_request/2,
    send_ping/2
]).

-define(CLIENT_INFO, #{
    <<"name">> => <<"emqx-mcp-gateway">>,
    <<"version">> => <<"0.1.0">>
}).
-define(LINE_BYTES, 4096).

connect_server(#{command := Cmd, args := Args, env := Env0}) ->
    Env = [{emqx_utils_conv:str(K), emqx_utils_conv:str(V)} || {K, V} <- maps:to_list(Env0)],
    ?SLOG(debug, #{msg => "connect_server", cmd => Cmd, args => Args, env => Env}),
    try
        Port = open_port(Cmd, Args, Env),
        MonRef = erlang:monitor(port, Port),
        {ok, #{port => Port, port_mon => MonRef, pending_requests => #{}}}
    catch
        error:Reason ->
            {error, Reason}
    end.

send_initialize_request(McpClientId, #{port := Port, pending_requests := PendingReqs} = State) ->
    Id = 1,
    InitMsg = emqx_mcp_message:initialize_request(Id, ?CLIENT_INFO, #{}),
    true = erlang:port_command(Port, InitMsg),
    {ok, State#{
        pending_requests => PendingReqs#{
            Id => #{
                mcp_client_id => McpClientId,
                timestamp => msg_ts(),
                msg_type => initialize
            }
        },
        next_req_id => Id + 1
    }}.

send_ping(McpClientId, #{port := Port, pending_requests := PendingReqs, next_req_id := Id} = State) ->
    PingMsg = emqx_mcp_message:json_rpc_request(Id, <<"ping">>, #{<<"clientId">> => McpClientId}),
    true = erlang:port_command(Port, PingMsg),
    {ok, State#{
        pending_requests => PendingReqs#{
            Id => #{
                mcp_client_id => McpClientId,
                timestamp => msg_ts(),
                msg_type => ping
            }
        }
    }}.

handle_info({'DOWN', MonRef, port, _Port, _Reason}, #{port_mon := MonRef} = State) ->
    handle_close(State),
    {stop, State}.

handle_close(#{port := Port, port_mon := MonRef}) ->
    erlang:demonitor(MonRef, [flush]),
    erlang:port_close(Port),
    ok;
handle_close(_) ->
    ok.

handle_initialize_response({_Port, {data, Data}}, State) ->
    PendingReqs = maps:get(pending_requests, State, #{}),
    case emqx_mcp_message:decode_rpc_msg(Data) of
        {ok, #{type := json_rpc_response, id := Id, result := Result}} ->
            case maps:take(Id, PendingReqs) of
                {#{mcp_client_id := McpClientId, msg_type := initialize}, PendingReqs1} ->
                    validate_initialize_response(
                        Result,
                        McpClientId,
                        State#{pending_requests => PendingReqs1}
                    );
                error ->
                    {stop, {invalid_initialize_response_id, Id}}
            end;
        {ok, #{type := json_rpc_error, id := Id, error := Error}} ->
            case maps:take(Id, PendingReqs) of
                {#{msg_type := initialize}, _PendingReqs1} ->
                    {stop, {initialize_failed, Error}};
                error ->
                    {stop, {invalid_initialize_response_id, Id}}
            end;
        {error, Reason} ->
            {stop, {invalid_initialize_response, Reason}}
    end.

validate_initialize_response(Result, #{port := Port} = State, _McpClientId) ->
    case maps:get(<<"protocolVersion">>, Result) of
        ?MCP_VERSION ->
            InitializedMs = emqx_mcp_message:initialized_notification(),
            true = erlang:port_command(Port, InitializedMs),
            {ok, State};
        Vsn ->
            ErrorMsg = emqx_mcp_message:json_rpc_error(
                1,
                -32602,
                <<"Unsupported protocol version">>,
                #{
                    <<"supported">> => [?MCP_VERSION],
                    <<"requested">> => Vsn
                }
            ),
            true = erlang:port_command(Port, ErrorMsg),
            {stop, unsupported_protocol_version}
    end.

msg_ts() ->
    erlang:monotonic_time(millisecond).

open_port(Cmd, Args, Env) ->
    PortSetting = [{args, Args}, {env, Env}, binary, {line, ?LINE_BYTES}, hide, stderr_to_stdout],
    erlang:open_port({spawn_executable, Cmd}, PortSetting).
