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

-module(emqx_mcp_server_dispatcher).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include("emqx_mcp_gateway.hrl").
-include("emqx_mcp_errors.hrl").

%% API
-export([start_link/0, restart/0, start_listening_servers/1, initialize/5, stop_servers/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Types
-define(SERVER, ?MODULE).
-define(POOL_SIZE, 4).

%%==============================================================================
%% API Functions
%%==============================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

restart() ->
    ok = supervisor:terminate_child(emqx_mcp_gateway_sup, ?MODULE),
    {ok, _} = supervisor:restart_child(emqx_mcp_gateway_sup, ?MODULE),
    ok.

-spec start_listening_servers(emqx_mcp_server:config()) -> ok.
start_listening_servers(Conf) ->
    gen_server:cast(?SERVER, {start_listening_servers, Conf}).

initialize(ServerName, McpClientId, Credentials, Id, RawInitReq) ->
    Req = {initialize, ServerName, McpClientId, Credentials, Id, RawInitReq},
    gen_server:call(?SERVER, Req, infinity).

stop_servers(ServerName) ->
    gen_server:call(?SERVER, {stop_servers, ServerName}, infinity).

%%==============================================================================
%% GenServer Callbacks
%%==============================================================================
init([]) ->
    {ok, #{}}.

handle_call(
    {initialize, ServerName, McpClientId, Credentials, Id, RawInitReq},
    Caller,
    #{listening_mcp_servers := McpServers} = State
) ->
    case maps:get(ServerName, McpServers, []) of
        [] ->
            {reply, {error, ?ERR_NO_SERVER_AVAILABLE}, State};
        [{Pid, Conf} | Servers] ->
            Request = {client_initialize, Caller, McpClientId, Credentials, Id, RawInitReq},
            %% NOTE: this is an "async" call which should return immediately.
            %% We use 'call' instead of 'cast' here, because:
            %%  1. monitor the server process in case of crash
            %%  2. add back pressure to the caller
            case emqx_mcp_server:safe_call(Pid, Request, 100) of
                ok ->
                    %register_mcp_client_server_mapping(McpClientId, Pid),
                    start_listening_servers(Conf),
                    %% We don't reply the caller here, instead the emqx_mcp_server will
                    %% send the response back to the caller.
                    {noreply, State#{listening_mcp_servers => McpServers#{ServerName => Servers}}};
                {error, Reason} ->
                    emqx_mcp_server:stop(Pid),
                    {reply, {error, Reason}, State#{
                        listening_mcp_servers => McpServers#{ServerName => Servers}
                    }}
            end
    end;
handle_call({stop_servers, ServerName}, _From, State) ->
    McpServers = maps:get(listening_mcp_servers, State, #{}),
    Servers = maps:get(ServerName, McpServers, []),
    lists:foreach(
        fun({Pid, _Conf}) ->
            emqx_mcp_server:stop(Pid)
        end,
        Servers
    ),
    {reply, ok, State#{
        listening_mcp_servers => maps:remove(ServerName, McpServers)
    }};
handle_call({initialize, _, _, _}, _From, State) ->
    {reply, {error, no_server_name_available}, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({start_listening_servers, #{server_name := ServerName} = Conf}, State) ->
    McpServers = maps:get(listening_mcp_servers, State, #{}),
    Servers = maps:get(ServerName, McpServers, []),
    case ?POOL_SIZE - length(Servers) of
        Num when Num > 0 ->
            start_n_mcp_servers(Num, Conf, State);
        _ ->
            {noreply, State}
    end;
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => unexpected_cast, message => Msg}),
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    McpServers1 = maps:map(
        fun(_ServerName, Servers) ->
            lists:keydelete(Pid, 1, Servers)
        end,
        maps:get(listening_mcp_servers, State, #{})
    ),
    {noreply, State#{
        listening_mcp_servers => McpServers1
    }};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => unexpected_info, message => Info}),
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
            _ = erlang:monitor(process, Pid),
            {Pid, Conf};
        {error, Reason} ->
            throw({start_mcp_server_failed, Reason})
    end.

start_n_mcp_servers(Num, #{server_name := ServerName} = Conf, State) ->
    McpServers = maps:get(listening_mcp_servers, State, #{}),
    Servers = maps:get(ServerName, McpServers, []),
    NewServers =
        try
            [start_mcp_server(Conf) || _ <- lists:seq(1, Num)]
        catch
            throw:start_mcp_server_failed ->
                []
        end,
    {noreply, State#{
        listening_mcp_servers => McpServers#{ServerName => NewServers ++ Servers}
    }}.
