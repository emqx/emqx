-module(emqx_mcp_server_dispatcher).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include("emqx_mcp_gateway.hrl").

%% API
-export([start_link/0, stop/0, restart/0, start_server_pool/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Types
-define(SERVER, ?MODULE).
-define(POOL_SIZE, 16).

%%==============================================================================
%% API Functions
%%==============================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).

restart() ->
    ok = supervisor:terminate_child(emqx_mcp_gateway_sup, ?MODULE),
    {ok, _} = supervisor:restart_child(emqx_mcp_gateway_sup, ?MODULE),
    ok.

-spec start_server_pool(emqx_mcp_server:config()) -> ok.
start_server_pool(Conf) ->
    gen_server:cast(?SERVER, {start_server_pool, Conf}).

%%==============================================================================
%% GenServer Callbacks
%%==============================================================================
init([]) ->
    {ok, #{}}.

handle_call(
    {initialize, ServerName, McpClientId, Credentials},
    Caller,
    #{listening_mcp_servers := McpServers} = State
) ->
    case maps:get(ServerName, McpServers, []) of
        [] ->
            {reply, {error, {no_server_available, ServerName}}, State};
        [{Pid, Conf} | Servers] ->
            Request = {client_initialize, Caller, McpClientId, Credentials},
            %% NOTE: this is an "async" call which should return immediately
            case safe_call(Pid, Request, 100) of
                ok ->
                    start_server_pool(Conf),
                    %% the emqx_mcp_sever will reply to the request
                    {noreply, State#{listening_mcp_servers => McpServers#{ServerName => Servers}}};
                {error, Reason} ->
                    emqx_mcp_server:stop(Pid),
                    {reply, {error, Reason}, State#{
                        listening_mcp_servers => McpServers#{ServerName => Servers}
                    }}
            end
    end;
handle_call({initialize, _, _, _}, _From, State) ->
    {reply, {error, no_server_name_available}, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({start_server_pool, #{server_name := ServerName} = Conf}, State) ->
    McpServers = maps:get(listening_mcp_servers, State, #{}),
    Servers = maps:get(ServerName, McpServers, []),
    case ?POOL_SIZE - length(Servers) of
        Num when Num > 0 ->
            start_n_mcp_servers(Num, Conf, State);
        _ ->
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%==============================================================================
%% Internal Functions
%%==============================================================================
-spec start_mcp_server(emqx_mcp_server:config()) -> {pid(), emqx_mcp_server:config()}.
start_mcp_server(Conf) ->
    case emqx_mcp_server:start_supervised(Conf) of
        {ok, Pid} ->
            {Pid, Conf};
        {error, {already_started, Pid}} ->
            {Pid, Conf};
        {error, Reason} ->
            throw({start_mcp_server_failed, Reason})
    end.

start_n_mcp_servers(Num, #{server_name := ServerName} = Conf, State) ->
    McpServers = maps:get(listening_mcp_servers, State, #{}),
    Servers = maps:get(ServerName, McpServers, []),
    try
        NewServers = [start_mcp_server(Conf) || _ <- lists:seq(1, Num)],
        {noreply, State#{
            listening_mcp_servers => McpServers#{ServerName => NewServers ++ Servers}
        }}
    catch
        throw:Reason ->
            ?SLOG(error, #{
                msg => start_mcp_server_pool_failed,
                server_name => ServerName,
                reason => Reason
            }),
            {noreply, State};
        error:Reason:St ->
            ?SLOG(error, #{
                msg => start_mcp_server_pool_failed,
                server_name => ServerName,
                reason => Reason,
                stacktrace => St
            }),
            {noreply, State}
    end.

safe_call(ServerRef, Message, Timeout) ->
    try
        gen_statem:call(ServerRef, Message, {clean_timeout, Timeout})
    catch
        error:badarg ->
            {error, not_found};
        exit:{R, _} when R == noproc; R == normal; R == shutdown ->
            {error, not_found};
        exit:{timeout, _} ->
            {error, timeout};
        exit:{{shutdown, removed}, _} ->
            {error, not_found}
    end.
