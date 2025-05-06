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

-module(emqx_mcp_server).

-include_lib("emqx/include/logger.hrl").
-include("emqx_mcp_gateway.hrl").
-include("emqx_mcp_errors.hrl").

-behaviour(gen_statem).

%% API
-export([
    start_supervised/1,
    start_link/1,
    stop_supervised_all/0,
    stop/1,
    safe_call/3
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

safe_call(ServerRef, Message, Timeout) ->
    try
        gen_statem:call(ServerRef, Message, {clean_timeout, Timeout})
    catch
        error:badarg ->
            {error, ?ERR_NO_SERVER_AVAILABLE};
        exit:{R, _} when R == noproc; R == normal; R == shutdown ->
            {error, ?ERR_NO_SERVER_AVAILABLE};
        exit:{timeout, _} ->
            {error, ?ERR_TIMEOUT};
        exit:{{shutdown, _}, _} ->
            {error, ?ERR_NO_SERVER_AVAILABLE}
    end.

%% gen_statem callbacks
-spec init(config()) ->
    gen_statem:init_result(state_name(), loop_data()).
init(#{name := Name, server_name := ServerName, server_conf := ServerConf, mod := Mod} = Conf) ->
    process_flag(trap_exit, true),
    LoopData = #{
        name => Name,
        server_name => ServerName,
        mod => Mod,
        mcp_server_conf => ServerConf,
        opts => maps:get(opts, Conf, #{}),
        pending_requests => #{},
        mcp_state => #{}
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
server_connected(
    {call, From}, {client_initialize, Caller, McpClientId, Credentials, Id, RawInitReq}, LoopData
) ->
    %% reply the dispatcher, not the caller
    gen_statem:reply(From, ok),
    case authorize(Credentials) of
        ok ->
            case forward_initialize_request(Id, RawInitReq, LoopData) of
                {ok, LoopData1} ->
                    {next_state, wait_init_response,
                        LoopData1#{
                            init_caller => Caller,
                            mcp_client_id => McpClientId
                        },
                        [{state_timeout, ?INIT_TIMEOUT, initialize_timeout}]};
                {error, Reason} ->
                    shutdown(#{error => Reason}, [{reply, Caller, {error, Reason}}])
            end;
        {error, Reason} ->
            ?SLOG(error, #{msg => authorize_failed, reason => Reason}),
            shutdown(#{error => Reason}, [{reply, Caller, {error, Reason}}])
    end;
?handle_common.

-spec wait_init_response(enter | gen_statem:event_type(), state_name(), loop_data()) ->
    gen_statem:state_enter_result(state_name(), loop_data())
    | gen_statem:event_handler_result(state_name(), loop_data()).
wait_init_response(enter, OldState, _LoopData) ->
    ?log_enter_state(OldState),
    {keep_state_and_data, []};
wait_init_response(state_timeout, TimeoutReason, #{init_caller := Caller}) ->
    shutdown(#{error => TimeoutReason}, [{reply, Caller, {error, {timeout, TimeoutReason}}}]);
wait_init_response(info, Packet, #{mod := Mod, init_caller := Caller} = LoopData) ->
    McpState = maps:get(mcp_state, LoopData),
    case Mod:unpack(Packet, McpState) of
        {ok, Resp, McpState1} ->
            case handle_initialize_response(Resp, LoopData#{mcp_state => McpState1}) of
                {ok, LoopData1} ->
                    Return = {ok, #{raw_response => Resp, server_pid => self()}},
                    {next_state, server_initialized, LoopData1, [{reply, Caller, Return}]};
                {error, #{reason := ?ERR_INVALID_JSON}} ->
                    handle_mcp_server_log_msg(Resp, LoopData#{mcp_state => McpState1});
                {error, Reason} = Err ->
                    shutdown(#{error => Reason}, [{reply, Caller, Err}])
            end;
        {more, McpState1} ->
            {keep_state, LoopData#{mcp_state => McpState1}};
        {stop, Reason} ->
            shutdown(#{error => Reason}, [{reply, Caller, {error, Reason}}]);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => unexpected_info, reason => Reason, info => Packet, state => ?FUNCTION_NAME
            }),
            keep_state_and_data
    end;
?handle_common.

-spec server_initialized(enter | gen_statem:event_type(), state_name(), loop_data()) ->
    gen_statem:state_enter_result(state_name(), loop_data())
    | gen_statem:event_handler_result(state_name(), loop_data()).
server_initialized(enter, OldState, _LoopData) ->
    ?log_enter_state(OldState),
    {keep_state_and_data, []};
server_initialized(info, Info, #{mod := Mod, mcp_state := McpState} = LoopData0) ->
    case Mod:unpack(Info, McpState) of
        {ok, Msg, McpState1} ->
            LoopData = LoopData0#{mcp_state => McpState1},
            case emqx_mcp_message:decode_rpc_msg(Msg) of
                {ok, RpcMsg} ->
                    %% Forward RPC msgs to the MCP client
                    #{server_name := ServerName} = LoopData,
                    TopicType = emqx_mcp_message:topic_type_of_rpc_msg(RpcMsg),
                    McpClientId = maps:get(mcp_client_id, LoopData),
                    emqx_mcp_message:publish_mcp_server_message(
                        ServerName, McpClientId, TopicType, #{}, Msg
                    ),
                    {keep_state, LoopData};
                {error, #{reason := ?ERR_INVALID_JSON}} ->
                    handle_mcp_server_log_msg(Msg, LoopData);
                {error, _Reason} ->
                    handle_unexpected_msg(server_initialized, Msg, LoopData)
            end;
        {more, McpState1} ->
            {keep_state, LoopData0#{mcp_state => McpState1}};
        {stop, Reason} ->
            shutdown(#{error => Reason});
        {error, _} ->
            handle_common(?FUNCTION_NAME, info, Info, LoopData0)
    end;
server_initialized(
    {call, From}, {rpc, RpcMsg}, #{mod := Mod, mcp_state := McpState} = LoopData
) ->
    case Mod:send_msg(McpState, RpcMsg) of
        {ok, McpState1} ->
            {keep_state, LoopData#{mcp_state => McpState1}, [{reply, From, ok}]};
        {error, Reason} ->
            ?SLOG(error, #{msg => send_rpc_failed, reason => Reason}),
            Reply = #{reason => ?ERR_SEND_TO_MCP_SERVER_FAILED, details => Reason},
            {keep_state_and_data, [{reply, From, Reply}]}
    end;
server_initialized({call, From}, client_disconnected, #{
    mod := Mod, mcp_state := McpState
}) ->
    case Mod:handle_close(McpState) of
        ok ->
            {keep_state_and_data, [{reply, From, ok}]};
        {error, Reason} ->
            ?SLOG(error, #{msg => close_failed, reason => Reason}),
            {keep_state_and_data, [{reply, From, {error, Reason}}]}
    end;
?handle_common.

terminate(_Reason, State, #{mod := Mod, mcp_state := McpState} = LoopData) ->
    Mod:handle_close(McpState),
    case State of
        server_initialized ->
            %% Notify the client that the server has disconnected
            ServerName = maps:get(server_name, LoopData),
            emqx_mcp_message:send_server_offline_message(ServerName);
        _ ->
            ok
    end;
terminate(_Reason, _State, _LoopData) ->
    ok.

code_change(_OldVsn, State, LoopData, _Extra) ->
    {ok, State, LoopData}.

handle_common(_State, state_timeout, TimeoutReason, _LoopData) ->
    shutdown(#{error => TimeoutReason});
handle_common(_State, cast, stop, _LoopData) ->
    shutdown(#{error => normal});
handle_common(State, info, Info, LoopData) ->
    handle_unexpected_msg(State, Info, LoopData);
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
forward_initialize_request(
    Id,
    RawInitReq,
    #{
        pending_requests := PendingReqs,
        mod := Mod,
        mcp_state := McpState
    } = LoopData
) ->
    case Mod:send_msg(McpState, RawInitReq) of
        {ok, McpState1} ->
            {ok, LoopData#{
                pending_requests => PendingReqs#{
                    Id => #{
                        timestamp => msg_ts(),
                        mcp_msg_type => initialize
                    }
                },
                mcp_state => McpState1
            }};
        {error, Reason} ->
            ?SLOG(error, #{msg => forward_initialize_request_failed, reason => Reason}),
            {error, #{reason => ?ERR_SEND_TO_MCP_SERVER_FAILED, details => Reason}}
    end.

handle_initialize_response(Resp, LoopData) ->
    PendingReqs = maps:get(pending_requests, LoopData, #{}),
    case emqx_mcp_message:decode_rpc_msg(Resp) of
        {ok, #{type := json_rpc_response, id := Id}} ->
            case maps:take(Id, PendingReqs) of
                {#{mcp_msg_type := initialize}, PendingReqs1} ->
                    {ok, LoopData#{pending_requests => PendingReqs1}};
                error ->
                    {error, #{reason => ?ERR_WRONG_SERVER_RESPONSE_ID, id => Id}}
            end;
        {ok, #{type := json_rpc_error, id := Id}} ->
            case maps:take(Id, PendingReqs) of
                {#{mcp_msg_type := initialize}, _PendingReqs1} ->
                    {json_rpc_error, Resp};
                error ->
                    {error, #{reason => ?ERR_WRONG_SERVER_RESPONSE_ID, id => Id}}
            end;
        {error, _} = Err ->
            Err
    end.

handle_unexpected_msg(State, Msg, LoopData) ->
    ?SLOG(error, #{
        msg => unexpected_common_msg,
        state => State,
        event_content => Msg
    }),
    {keep_state, LoopData}.

handle_mcp_server_log_msg(Msg, LoopData) ->
    ?SLOG(debug, #{msg => received_non_json_msg_from_mcp_server, details => Msg}),
    {keep_state, LoopData}.

msg_ts() ->
    erlang:monotonic_time(millisecond).
