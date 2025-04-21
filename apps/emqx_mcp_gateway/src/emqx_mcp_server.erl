-module(emqx_mcp_server).

-include_lib("emqx/include/logger.hrl").

-behaviour(gen_statem).

%% API
-export([start_supervised/4, stop_supervised_all/0, start_link/3, stop/1, client_initialize/3]).

%% gen_statem callbacks
-export([init/1, callback_mode/0, terminate/3, code_change/4]).

%% gen_statem state functions
-export([idle/3, server_connected/3, wait_init_response/3, server_initialized/3]).

-type state_name() ::
    idle
    | server_connected
    | wait_init_response
    | server_initialized.

-type mcp_server_config() :: #{}.
-type opts() :: #{}.
-type loop_data() :: #{}.

-define(handle_common, ?FUNCTION_NAME(T, C, D) -> handle_common(?FUNCTION_NAME, T, C, D)).
-define(log_enter_state(OldState),
    ?SLOG(debug, #{msg => enter_state, state => ?FUNCTION_NAME, previous => OldState})
).
-define(RPC_TIMEOUT, 5000).
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
start_supervised(Name, Mod, ServerConf, Opts) ->
    Spec = #{
        id => Name,
        start => {?MODULE, start_link, [Mod, ServerConf, Opts]},
        restart => permanent,
        shutdown => 3_000,
        type => worker,
        modules => [?MODULE]
    },
    supervisor:start_child(emqx_mcp_gateway_sup, Spec).

stop_supervised_all() ->
    %% Stop all MCP servers
    StartedServers = supervisor:which_children(emqx_mcp_gateway_sup),
    lists:foreach(
        fun({Name, _Pid, _, _}) ->
            case supervisor:terminate_child(emqx_mcp_gateway_sup, Name) of
                ok -> supervisor:delete_child(emqx_mcp_gateway_sup, Name);
                Error -> Error
            end
        end,
        StartedServers
    ).

start_link(Mod, ServerConf, Opts) ->
    gen_statem:start_link(?MODULE, {Mod, ServerConf, Opts}, []).

stop(Pid) ->
    gen_statem:stop(Pid).

client_initialize(Pid, McpClientId, Credentials) ->
    gen_statem:call(
        Pid, {client_initialize, McpClientId, Credentials}, {clean_timeout, ?RPC_TIMEOUT}
    ).

%% gen_statem callbacks
-spec init({module(), mcp_server_config(), opts()}) ->
    gen_statem:init_result(state_name(), loop_data()).
init({Mod, ServerConf, Opts}) ->
    {ok, idle, #{mod => Mod, mcp_server_conf => ServerConf, opts => Opts}, [
        {next_event, internal, connect_server}
    ]}.

callback_mode() ->
    [state_functions, state_enter].

idle(enter, _OldState, _LoopData) ->
    {keep_state_and_data, []};
idle(internal, connect_server, #{mod := Mod, mcp_server_conf := ServerConf} = Data) ->
    case Mod:connect_server(ServerConf) of
        {ok, McpState} ->
            {next_state, server_connected, Data#{mcp_state => McpState}};
        {error, Reason} ->
            ?SLOG(error, #{msg => connect_server_failed, reason => Reason}),
            shutdown(#{error => Reason})
    end;
idle(_EventType, _Event, _Data) ->
    {keep_state_and_data, []};
?handle_common.

server_connected(enter, OldState, _LoopData) ->
    ?log_enter_state(OldState),
    {keep_state_and_data, []};
server_connected(
    {call, From},
    {client_initialize, McpClientId, Credentials},
    #{
        mod := Mod, mcp_state := McpState
    } = Data
) ->
    case authorize(Credentials) of
        ok ->
            case Mod:send_initialize_request(McpClientId, McpState) of
                {ok, McpState1} ->
                    {next_state, wait_init_response, Data#{
                        mcp_state => McpState1, init_caller => From
                    }};
                {error, Reason} ->
                    ?SLOG(error, #{msg => initialize_failed, reason => Reason}),
                    {keep_state_and_data, [{reply, From, {error, Reason}}]}
            end;
        {error, Reason} ->
            ?SLOG(error, #{msg => authorize_failed, reason => Reason}),
            {keep_state_and_data, [{reply, From, {error, Reason}}]}
    end;
?handle_common.

wait_init_response(enter, OldState, _LoopData) ->
    ?log_enter_state(OldState),
    {keep_state_and_data, []};
wait_init_response(info, Data, #{mod := Mod, mcp_state := McpState, init_caller := From} = Data) ->
    case Mod:handle_initialize_response(Data, McpState) of
        {ok, McpState1} ->
            {next_state, server_initialized, Data#{mcp_state => McpState1}, [{reply, From, ok}]};
        {stop, Reason} ->
            shutdown(#{error => Reason}, [{reply, From, {error, Reason}}])
    end;
?handle_common.

server_initialized(enter, OldState, _LoopData) ->
    ?log_enter_state(OldState),
    {keep_state_and_data, []};
server_initialized({call, From}, {Rpc, Request}, #{mod := Mod, mcp_state := McpState} = Data) when
    ?MCP_RPC_REQ(Rpc)
->
    case Mod:Rpc(Request, McpState) of
        {ok, Result, McpState1} ->
            {keep_state, Data#{mcp_state => McpState1}, [{reply, From, Result}]};
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

terminate(_Reason, _State, #{mod := Mod, mcp_state := McpState} = _Data) ->
    Mod:handle_close(McpState),
    ok;
terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

handle_common(State, state_timeout, TimeoutReason, _LoopData) ->
    shutdown(#{error => TimeoutReason, state => State});
handle_common(_State, info, Data, #{mod := Mod, mcp_state := McpState} = Data) ->
    case Mod:handle_info(Data, McpState) of
        {ok, McpState1} ->
            {next_state, server_initialized, Data#{mcp_state => McpState1}};
        {stop, Reason} ->
            shutdown(#{error => Reason})
    end;
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

authorize(_Credentials) ->
    ok.
