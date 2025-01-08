%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Shared subscription group elector process.
%% Hosted under the _shared subscription registry_ supervisor.
%% Responsible for starting the leader election process that eventually
%% finishes with 2 outcomes:
%% 1. The elector wins the leadership.
%%    In this case the elector _becomes_ the leader, by entering the
%%    `emqx_ds_shared_sub_leader` process loop.
%% 2. The elector finds the active leader.
%%    In this case the elector idles while the leader is considered active
%%    and redirects any connect requests to the active leader.
-module(emqx_ds_shared_sub_elector).

-include("emqx_ds_shared_sub_proto.hrl").

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Internal API
-export([
    start_link/1
]).

-behaviour(gen_statem).
-export([
    callback_mode/0,
    init/1,
    handle_event/4
]).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

start_link(ShareTopic) ->
    gen_statem:start_link(?MODULE, {elect, ShareTopic}, []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-record(follower, {
    topic :: emqx_persistent_session_ds:share_topic_filter(),
    leader :: pid(),
    alive_until :: non_neg_integer()
}).

callback_mode() ->
    handle_event_function.

init(Elect = {elect, _ShareTopic}) ->
    %% NOTE
    %% Important to have it here, because this process can become
    %% `emqx_ds_shared_sub_leader`, which has `terminate/2` logic.
    _ = erlang:process_flag(trap_exit, true),
    {ok, electing, undefined, {next_event, internal, Elect}}.

handle_event(internal, {elect, ShareTopic}, electing, _) ->
    elect(ShareTopic, _TS = emqx_message:timestamp_now());
handle_event(
    info, ?borrower_connect_match(_BorrowerId, _ShareTopic) = ConnectMessage, follower, Data
) ->
    %% NOTE: Redirecting to the known leader.
    ok = connect_leader(ConnectMessage, Data),
    keep_state_and_data;
handle_event(state_timeout, invalidate, follower, _Data) ->
    {stop, {shutdown, invalidate}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

elect(ShareTopic, TS) ->
    StoreID = emqx_ds_shared_sub_store:mk_id(ShareTopic),
    case emqx_ds_shared_sub_store:claim_leadership(StoreID, _Leader = self(), TS) of
        {ok, LeaderClaim} ->
            %% Become the leader.
            ?tp(debug, shared_sub_elector_becomes_leader, #{
                id => ShareTopic,
                store => StoreID,
                leader => LeaderClaim
            }),
            emqx_ds_shared_sub_leader:become(ShareTopic, LeaderClaim);
        {exists, LeaderClaim} ->
            %% Turn into the follower that redirects connect requests to the leader
            %% while it's considered alive. Note that the leader may in theory decide
            %% to let go of leadership earlier than that.
            AliveUntil = emqx_ds_shared_sub_store:alive_until(LeaderClaim),
            ?tp(debug, shared_sub_elector_becomes_follower, #{
                id => ShareTopic,
                store => StoreID,
                leader => LeaderClaim,
                until => AliveUntil
            }),
            TTL = AliveUntil - TS,
            Data = #follower{
                topic = ShareTopic,
                leader = emqx_ds_shared_sub_store:leader_id(LeaderClaim),
                alive_until = AliveUntil
            },
            {next_state, follower, Data, {state_timeout, max(0, TTL), invalidate}};
        {error, Class, Reason} = Error ->
            ?tp(warning, "Shared subscription leader election failed", #{
                id => ShareTopic,
                store => StoreID,
                error => Error
            }),
            case Class of
                recoverable -> StopReason = {shutdown, Reason};
                unrecoverable -> StopReason = Error
            end,
            {stop, StopReason}
    end.

connect_leader(ConnectMessage, #follower{leader = Pid}) ->
    emqx_ds_shared_sub_proto:send_to_leader(Pid, ConnectMessage).
