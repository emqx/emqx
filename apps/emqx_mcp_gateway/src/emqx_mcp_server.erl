-module(emqx_mcp_server).

-include_lib("emqx/include/logger.hrl").
-include("emqx_mcp_gateway.hrl").

-behaviour(gen_statem).

%% API
-export([
    start_supervised/1,
    start_link/1,
    stop_supervised_all/0,
    stop/1
]).

%% gen_statem callbacks
-export([init/1, callback_mode/0, terminate/3, code_change/4]).

%% gen_statem state functions
-export([idle/3, server_connected/3, wait_init_response/3, server_initialized/3]).

-export_type([config/0]).

-type state_name() ::
    idle
    | server_connected
    | wait_init_response
    | server_initialized.

-type server_name() :: emqx_types:topic().
-type mcp_client_id() :: emqx_types:clientid().
-type mcp_server_config() :: #{}.
-type opts() :: #{}.
-type config() :: #{
    name := atom(),
    server_name := server_name(),
    server_conf := mcp_server_config(),
    mod := module(),
    opts => opts()
}.
-type mcp_msg_type() ::
    initialize
    | initialized
    | ping
    | progress_notification
    | set_logging_level
    | list_resources
    | list_resource_templates
    | read_resource
    | subscribe_resource
    | unsubscribe_resource
    | call_tool
    | list_prompts
    | get_prompt
    | complete
    | list_tools
    | roots_list_changed.
-type pending_requests() :: #{
    integer() => #{
        mcp_msg_type := mcp_msg_type(),
        timestamp := integer()
    }
}.

-type mcp_state() :: map().

-type loop_data() :: #{
    mod := module(),
    mcp_server_conf := map(),
    opts := opts(),
    pending_requests := pending_requests(),
    mcp_state := mcp_state(),
    next_req_id := integer(),
    mcp_client_id := mcp_client_id(),
    init_caller => pid() | undefined
}.

-define(CLIENT_INFO, #{
    <<"name">> => <<"emqx-mcp-gateway">>,
    <<"version">> => <<"0.1.0">>
}).

-define(handle_common, ?FUNCTION_NAME(T, C, D) -> handle_common(?FUNCTION_NAME, T, C, D)).
-define(log_enter_state(OldState),
    ?SLOG(debug, #{msg => enter_state, state => ?FUNCTION_NAME, previous => OldState})
).
-define(MCP_RPC_REQ(RPC),
    RPC =:= send_ping;
    RPC =:= send_progress_notification;
    RPC =:= set_logging_level;
    RPC =:= list_resources;
    RPC =:= list_resource_templates;
    RPC =:= read_resource;
    RPC =:= subscribe_resource;
    RPC =:= unsubscribe_resource;
    RPC =:= call_tool;
    RPC =:= list_prompts;
    RPC =:= get_prompt;
    RPC =:= complete;
    RPC =:= list_tools;
    RPC =:= send_roots_list_changed
).

%%==============================================================================
%% API
%%==============================================================================
-spec start_supervised(config()) -> supervisor:startchild_ret().
start_supervised(Conf) ->
    emqx_mcp_server_sup:start_child(Conf).

stop_supervised_all() ->
    %% Stop all MCP servers
    StartedServers = supervisor:which_children(emqx_mcp_server_sup),
    lists:foreach(
        fun({_Name, Pid, _, _}) ->
            supervisor:terminate_child(emqx_mcp_server_sup, Pid)
        end,
        StartedServers
    ).

-spec start_link(config()) -> gen_statem:start_ret().
start_link(Conf) ->
    gen_statem:start_link(?MODULE, Conf, []).

stop(Pid) ->
    gen_statem:cast(Pid, stop).

%% gen_statem callbacks
-spec init(config()) ->
    gen_statem:init_result(state_name(), loop_data()).
init(#{name := Name, server_name := ServerName, server_conf := ServerConf, mod := Mod} = Conf) ->
    LoopData = #{
        name => Name,
        server_name => ServerName,
        mod => Mod,
        mcp_server_conf => ServerConf,
        opts => maps:get(opts, Conf, #{}),
        pending_requests => #{},
        mcp_state => #{},
        next_req_id => 1
    },
    {ok, idle, LoopData, [{next_event, internal, connect_server}]}.

callback_mode() ->
    [state_functions, state_enter].

-spec idle(enter | gen_statem:event_type(), state_name(), loop_data()) ->
    gen_statem:state_enter_result(state_name(), loop_data())
    | gen_statem:event_handler_result(state_name(), loop_data()).
idle(enter, _OldState, _LoopData) ->
    {keep_state_and_data, []};
idle(internal, connect_server, #{mod := Mod, mcp_server_conf := ServerConf} = LoopData) ->
    case Mod:connect_server(ServerConf) of
        {ok, McpState} ->
            {next_state, server_connected, LoopData#{mcp_state => McpState}};
        {error, Reason} ->
            ?SLOG(error, #{msg => connect_server_failed, reason => Reason}),
            shutdown(#{error => Reason})
    end;
idle(_EventType, _Event, _LoopData) ->
    {keep_state_and_data, []};
?handle_common.

-spec server_connected(enter | gen_statem:event_type(), state_name(), loop_data()) ->
    gen_statem:state_enter_result(state_name(), loop_data())
    | gen_statem:event_handler_result(state_name(), loop_data()).
server_connected(enter, OldState, _LoopData) ->
    ?log_enter_state(OldState),
    {keep_state_and_data, []};
server_connected({call, From}, {client_initialize, Caller, McpClientId, Credentials}, LoopData) ->
    %% reply the dispatcher, not the caller
    gen_statem:reply(From, ok),
    case authorize(Credentials) of
        ok ->
            case send_initialize_request(LoopData) of
                {ok, LoopData1} ->
                    {next_state, wait_init_response,
                        LoopData1#{
                            init_caller => Caller,
                            mcp_client_id => McpClientId
                        },
                        [{state_timeout, ?INIT_TIMEOUT, handshake_timeout}]};
                {error, Reason} ->
                    shutdown(#{error => Reason}, [{reply, From, {error, Reason}}])
            end;
        {error, Reason} ->
            ?SLOG(error, #{msg => authorize_failed, reason => Reason}),
            shutdown(#{error => Reason}, [{reply, From, {error, Reason}}])
    end;
?handle_common.

-spec wait_init_response(enter | gen_statem:event_type(), state_name(), loop_data()) ->
    gen_statem:state_enter_result(state_name(), loop_data())
    | gen_statem:event_handler_result(state_name(), loop_data()).
wait_init_response(enter, OldState, _LoopData) ->
    ?log_enter_state(OldState),
    {keep_state_and_data, []};
wait_init_response(info, Packet, #{mod := Mod, init_caller := From} = LoopData) ->
    McpState = maps:get(mcp_state, LoopData),
    case Mod:decode_packet(Packet, McpState) of
        {ok, Msg, McpState1} ->
            case handle_initialize_response(Msg, LoopData#{mcp_state => McpState1}) of
                {ok, LoopData1} ->
                    {next_state, server_initialized, LoopData1, [{reply, From, ok}]};
                {error, Reason} ->
                    shutdown(#{error => Reason}, [{reply, From, {error, Reason}}])
            end;
        {more, McpState1} ->
            {keep_state, LoopData#{mcp_state => McpState1}};
        {error, Reason} ->
            ?SLOG(error, #{msg => invalid_initialize_response, reason => Reason}),
            {error, {invalid_initialize_response, Reason}}
    end;
?handle_common.

-spec server_initialized(enter | gen_statem:event_type(), state_name(), loop_data()) ->
    gen_statem:state_enter_result(state_name(), loop_data())
    | gen_statem:event_handler_result(state_name(), loop_data()).
server_initialized(enter, OldState, _LoopData) ->
    ?log_enter_state(OldState),
    {keep_state_and_data, []};
server_initialized(info, Info, #{mod := Mod, mcp_state := McpState} = LoopData) ->
    case Mod:decode_packet(Info, McpState) of
        {ok, Msg, McpState1} ->
            handle_operation_msg(Msg, LoopData#{mcp_state => McpState1});
        {more, McpState1} ->
            {keep_state, LoopData#{mcp_state => McpState1}};
        {error, _} ->
            handle_common(?FUNCTION_NAME, info, Info, LoopData)
    end;
server_initialized(
    {call, From}, {Rpc, Request}, #{mod := Mod, mcp_state := McpState} = LoopData
) when
    ?MCP_RPC_REQ(Rpc)
->
    case Mod:Rpc(Request, McpState) of
        {ok, Result, McpState1} ->
            {keep_state, LoopData#{mcp_state => McpState1}, [{reply, From, Result}]};
        {error, Reason} ->
            ?SLOG(error, #{msg => rpc_failed, reason => Reason}),
            {keep_state_and_data, [{reply, From, {error, Reason}}]}
    end;
server_initialized({call, From}, {client_disconnected, McpClientId}, #{
    mod := Mod, mcp_state := McpState
}) ->
    case Mod:close(McpClientId, McpState) of
        ok ->
            {keep_state_and_data, [{reply, From, ok}]};
        {error, Reason} ->
            ?SLOG(error, #{msg => close_failed, reason => Reason}),
            {keep_state_and_data, [{reply, From, {error, Reason}}]}
    end;
?handle_common.

terminate(_Reason, _State, #{mod := Mod, mcp_state := McpState} = _LoopData) ->
    Mod:handle_close(McpState),
    ok;
terminate(_Reason, _State, _LoopData) ->
    ok.

code_change(_OldVsn, State, LoopData, _Extra) ->
    {ok, State, LoopData}.

handle_common(_State, state_timeout, TimeoutReason, _LoopData) ->
    shutdown(#{error => TimeoutReason});
handle_common(_State, cast, stop, _LoopData) ->
    shutdown(#{error => normal});
handle_common(State, info, Info, LoopData) ->
    handle_common_msg(State, Info, LoopData);
handle_common(State, EventType, EventContent, _LoopData) ->
    ?SLOG(error, #{
        msg => unexpected_msg,
        state => State,
        event_type => EventType,
        event_content => EventContent
    }),
    {keep_state_and_data, []}.

shutdown(ErrObj) ->
    shutdown(ErrObj, []).

shutdown(#{error := normal}, Actions) ->
    {stop, normal, Actions};
shutdown(#{error := Error} = ErrObj, Actions) when is_atom(Error) ->
    ?SLOG(warning, ErrObj#{msg => shutdown}),
    {stop, {shutdown, Error}, Actions}.

%%==============================================================================
%% Authorization
%%==============================================================================
authorize(_Credentials) ->
    ok.

%%==============================================================================
%% Internal functions
%%==============================================================================
send_initialize_request(
    #{
        pending_requests := PendingReqs,
        mod := Mod,
        mcp_state := McpState,
        next_req_id := Id
    } = LoopData
) ->
    InitMsg = emqx_mcp_message:initialize_request(Id, ?CLIENT_INFO, #{}),
    case Mod:send_msg(McpState, InitMsg) of
        {ok, McpState1} ->
            {ok, LoopData#{
                pending_requests => PendingReqs#{
                    Id => #{
                        timestamp => msg_ts(),
                        mcp_msg_type => initialize
                    }
                },
                mcp_state => McpState1,
                next_req_id => Id + 1
            }};
        {error, Reason} ->
            ?SLOG(error, #{msg => initialize_failed, reason => Reason}),
            {error, Reason}
    end.

handle_initialize_response(Msg, LoopData) ->
    PendingReqs = maps:get(pending_requests, LoopData, #{}),
    case emqx_mcp_message:decode_rpc_msg(Msg) of
        {ok, #{type := json_rpc_response, id := Id, result := Result}} ->
            case maps:take(Id, PendingReqs) of
                {#{msg_type := initialize}, PendingReqs1} ->
                    validate_initialize_response(
                        Id,
                        Result,
                        LoopData#{pending_requests => PendingReqs1}
                    );
                error ->
                    {error, {invalid_initialize_response_id, Id}}
            end;
        {ok, #{type := json_rpc_error, id := Id, error := Error}} ->
            case maps:take(Id, PendingReqs) of
                {#{msg_type := initialize}, _PendingReqs1} ->
                    {error, {initialize_failed, Error}};
                error ->
                    {error, {invalid_initialize_response_id, Id}}
            end;
        {error, Reason} ->
            {error, {invalid_initialize_response, Reason}}
    end.

validate_initialize_response(Id, Result, #{mod := Mod, mcp_state := McpState} = LoopData) ->
    case maps:get(<<"protocolVersion">>, Result) of
        ?MCP_VERSION ->
            InitializedMs = emqx_mcp_message:initialized_notification(),
            Mod:send_msg(McpState, InitializedMs),
            {ok, LoopData};
        Vsn ->
            ErrorMsg = emqx_mcp_message:json_rpc_error(
                Id,
                -32602,
                <<"Unsupported protocol version">>,
                #{
                    <<"supported">> => [?MCP_VERSION],
                    <<"requested">> => Vsn
                }
            ),
            Mod:send_msg(McpState, ErrorMsg),
            {error, unsupported_protocol_version}
    end.

handle_operation_msg(Msg, LoopData) ->
    case emqx_mcp_message:decode_rpc_msg(Msg) of
        {ok, RpcMsg} ->
            %% Forward RPC msgs to the MCP client
            send_to_mcp_client(RpcMsg, Msg, LoopData);
        {error, _Reason} ->
            %% Not a valid RPC msg, handle it as a common msg
            handle_common_msg(server_initialized, Msg, LoopData)
    end.

handle_common_msg(_State, Msg, #{mod := Mod, mcp_state := McpState} = LoopData) ->
    case Mod:handle_msg(Msg, McpState) of
        {ok, McpState1} ->
            {keep_state, LoopData#{mcp_state => McpState1}};
        {error, Reason} ->
            ?SLOG(error, #{msg => handle_common_msg_error, reason => Reason}),
            keep_state_and_data;
        {stop, Reason} ->
            shutdown(#{error => Reason})
    end;
handle_common_msg(State, Msg, _LoopData) ->
    ?SLOG(error, #{
        msg => unexpected_common_msg,
        state => State,
        event_content => Msg
    }),
    keep_state_and_data.

send_to_mcp_client(RpcMsg, Msg, #{name := Name, server_name := ServerName}) ->
    NameB = emqx_utils_conv:bin(Name),
    McpServerId = ?MCP_SERVER_ID(NameB),
    Topic = emqx_message:get_topic(
        emqx_mcp_message:topic_type_of_rpc_msg(RpcMsg),
        #{server_id => McpServerId, server_name => ServerName}
    ),
    Flags = #{},
    UserProps = [
        {<<"MCP-COMPONENT-TYPE">>, <<"mcp-server">>},
        {<<"MCP-MQTT-CLIENT-ID">>, McpServerId}
    ],
    Headers = #{
        properties => #{
            'User-Property' => UserProps
        }
    },
    MqttMsg = emqx_message:make(McpServerId, 1, Topic, Msg, Flags, Headers),
    _ = emqx:publish(MqttMsg),
    ok.

msg_ts() ->
    erlang:monotonic_time(millisecond).
