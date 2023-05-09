%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_agent).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

-include_lib("stdlib/include/qlc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    start_link/0,
    enable/1,
    disable/1,
    status/0
]).

-export([
    init/1,
    handle_call/3,
    handle_info/2,
    handle_cast/2,
    code_change/3
]).

-define(ENABLE_KIND, emqx_node_rebalance).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-type status() :: {enabled, pid()} | disabled.

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec enable(pid()) -> ok_or_error(already_enabled | eviction_agent_busy).
enable(CoordinatorPid) ->
    gen_server:call(?MODULE, {enable, CoordinatorPid}).

-spec disable(pid()) -> ok_or_error(already_disabled | invalid_coordinator).
disable(CoordinatorPid) ->
    gen_server:call(?MODULE, {disable, CoordinatorPid}).

-spec status() -> status().
status() ->
    gen_server:call(?MODULE, status).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{}}.

handle_call({enable, CoordinatorPid}, _From, St) ->
    case St of
        #{coordinator_pid := _Pid} ->
            {reply, {error, already_enabled}, St};
        _ ->
            true = link(CoordinatorPid),
            EvictionAgentPid = whereis(emqx_eviction_agent),
            true = link(EvictionAgentPid),
            case emqx_eviction_agent:enable(?ENABLE_KIND, undefined) of
                ok ->
                    {reply, ok, #{
                        coordinator_pid => CoordinatorPid,
                        eviction_agent_pid => EvictionAgentPid
                    }};
                {error, eviction_agent_busy} ->
                    true = unlink(EvictionAgentPid),
                    true = unlink(CoordinatorPid),
                    {reply, {error, eviction_agent_busy}, St}
            end
    end;
handle_call({disable, CoordinatorPid}, _From, St) ->
    case St of
        #{
            coordinator_pid := CoordinatorPid,
            eviction_agent_pid := EvictionAgentPid
        } ->
            _ = emqx_eviction_agent:disable(?ENABLE_KIND),
            true = unlink(EvictionAgentPid),
            true = unlink(CoordinatorPid),
            NewSt = maps:without(
                [coordinator_pid, eviction_agent_pid],
                St
            ),
            {reply, ok, NewSt};
        #{coordinator_pid := _CoordinatorPid} ->
            {reply, {error, invalid_coordinator}, St};
        #{} ->
            {reply, {error, already_disabled}, St}
    end;
handle_call(status, _From, St) ->
    case St of
        #{coordinator_pid := Pid} ->
            {reply, {enabled, Pid}, St};
        _ ->
            {reply, disabled, St}
    end;
handle_call(Msg, _From, St) ->
    ?SLOG(warning, #{
        msg => "unknown_call",
        call => Msg,
        state => St
    }),
    {reply, ignored, St}.

handle_info(Msg, St) ->
    ?SLOG(warning, #{
        msg => "unknown_info",
        info => Msg,
        state => St
    }),
    {noreply, St}.

handle_cast(Msg, St) ->
    ?SLOG(warning, #{
        msg => "unknown_cast",
        cast => Msg,
        state => St
    }),
    {noreply, St}.

code_change(_Vsn, State, _Extra) ->
    {ok, State}.
