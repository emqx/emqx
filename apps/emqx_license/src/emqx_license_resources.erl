%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_resources).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-define(CHECK_INTERVAL, 5000).

-export([
    start_link/0,
    start_link/1,
    %% RPC
    local_connection_count/0,
    %% hot call
    cached_connection_count/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(OK(EXPR),
    (fun() ->
        try
            _ = EXPR,
            ok
        catch
            _:_ -> ok
        end
    end)()
).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link(?CHECK_INTERVAL).

-spec start_link(timeout()) -> {ok, pid()}.
start_link(CheckInterval) when is_integer(CheckInterval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [CheckInterval], []).

-spec local_connection_count() -> non_neg_integer().
local_connection_count() ->
    emqx_cm:get_connected_client_count().

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([CheckInterval]) ->
    _ = ets:new(?MODULE, [set, protected, named_table]),
    State = ensure_timer(#{check_peer_interval => CheckInterval}),
    {ok, State}.

handle_call(_Req, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(update_resources, State) ->
    true = update_resources(),
    connection_quota_early_alarm(),
    ?tp(emqx_license_resources_updated, #{}),
    {noreply, ensure_timer(State)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------
connection_quota_early_alarm() ->
    connection_quota_early_alarm(emqx_license_checker:limits()).

connection_quota_early_alarm({ok, #{max_connections := Max}}) when is_integer(Max) ->
    Count = cached_connection_count(),
    Low = emqx_conf:get([license, connection_low_watermark], 0.75),
    High = emqx_conf:get([license, connection_high_watermark], 0.80),
    Count > Max * High andalso
        begin
            HighPercent = float_to_binary(High * 100, [{decimals, 0}]),
            Message = iolist_to_binary([
                "License: live connection number exceeds ", HighPercent, "%"
            ]),
            ?OK(emqx_alarm:activate(license_quota, #{high_watermark => HighPercent}, Message))
        end,
    Count < Max * Low andalso ?OK(emqx_alarm:ensure_deactivated(license_quota));
connection_quota_early_alarm(_Limits) ->
    ok.

cached_connection_count() ->
    try ets:lookup(?MODULE, total_connection_count) of
        [{total_connection_count, N}] -> N;
        _ -> 0
    catch
        error:badarg -> 0
    end.

update_resources() ->
    ets:insert(?MODULE, {total_connection_count, total_connection_count()}).

ensure_timer(#{check_peer_interval := CheckInterval} = State) ->
    _ =
        case State of
            #{timer := Timer} -> erlang:cancel_timer(Timer);
            _ -> ok
        end,
    State#{timer => erlang:send_after(CheckInterval, self(), update_resources)}.

total_connection_count() ->
    Nodes = mria:running_nodes(),
    Results = emqx_license_proto_v2:remote_connection_counts(Nodes),
    Counts = [Count || {ok, Count} <- Results],
    lists:sum(Counts) + emqx_gateway_cm_registry:get_connected_client_count().
