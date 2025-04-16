-module(emqx_mcp_server).

-behaviour(gen_statem).

%% API
-export([start_link/2, stop/1, connect_server/1, client_initialize/2]).

%% gen_statem callbacks
-export([init/1, callback_mode/0, terminate/3, code_change/4]).

%% gen_statem state functions
-export([idle/3, server_ready/3]).

-type state_name() ::
    idle
    | server_ready.

-type mcp_server_config() :: #{}.
-type loop_data() :: #{}.

-define(handle_common, ?FUNCTION_NAME(T, C, D) -> handle_common(?FUNCTION_NAME, T, C, D)).
-define(log_enter_state(OldState),
    logger:debug(#{msg => enter_state, state => ?FUNCTION_NAME, previous => OldState})
).

%% API
start_link(Mod, Conf) ->
    gen_statem:start_link(?MODULE, {Mod, Conf}, []).

stop(Pid) ->
    gen_statem:stop(Pid).

connect_server(Pid) ->
    gen_statem:call(Pid, connect_server).

client_initialize(Pid, Credentials) ->
    gen_statem:call(Pid, {client_initialize, Credentials}).

%% gen_statem callbacks
-spec init({module(), mcp_server_config()}) -> gen_statem:init_result(state_name(), loop_data()).
init({Mod, Conf}) ->
    {ok, idle, #{mod => Mod, mcp_server_conf => Conf}}.

callback_mode() ->
    [state_functions, state_enter].

idle(enter, _OldState, _LoopData) ->
    {keep_state_and_data, []};
idle({call, From}, connect_server, #{mod := Mod, mcp_server_conf := Conf} = Data) ->
    case Mod:connect_server(Conf) of
        {ok, McpState} ->
            {next_state, server_ready, Data#{mcp_state => McpState}, [{reply, From, ok}]};
        {error, Reason} ->
            logger:error(#{msg => initialize_failed, reason => Reason}),
            shutdown(#{error => Reason})
    end;
idle(_EventType, _Event, _Data) ->
    {keep_state_and_data, []};
?handle_common.

server_ready(enter, _OldState, _LoopData) ->
    ?log_enter_state(idle),
    {keep_state_and_data, []};
server_ready({call, From}, {client_initialize, Credentials}, #{mcp_state := McpState}) ->
    Reply =
        case authorize(Credentials) of
            ok -> {ok, McpState};
            {error, Reason} -> {error, Reason}
        end,
    {keep_state_and_data, [{reply, From, Reply}]};
?handle_common.

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

handle_common(State, state_timeout, TimeoutReason, _LoopData) ->
    shutdown(#{error => TimeoutReason, state => State});
handle_common(State, EventType, EventContent, _LoopData) ->
    logger:error(#{
        msg => unexpected_msg,
        state => State,
        event_type => EventType,
        event_content => EventContent
    }),
    {keep_state_and_data, []}.

shutdown(#{error := normal}) ->
    {stop, normal};
shutdown(#{error := Error} = ErrObj) when is_atom(Error) ->
    logger:warning(ErrObj#{msg => shutdown}),
    {stop, {shutdown, Error}}.

authorize(_Credentials) ->
    ok.
