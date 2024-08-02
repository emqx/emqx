%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    start_link/2
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

start_link(ShareTopic, StartTime) ->
    gen_server:start_link(?MODULE, {elect, ShareTopic, StartTime}, []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-record(follower, {
    topic :: emqx_persistent_session_ds:share_topic_filter(),
    leader :: pid(),
    alive_until :: non_neg_integer()
}).

init(Elect = {elect, _ShareTopic, _StartTime}) ->
    %% NOTE
    %% Important to have it here, because this process can become
    %% `emqx_ds_shared_sub_leader`, which has `terminate/2` logic.
    _ = erlang:process_flag(trap_exit, true),
    {ok, #{}, {continue, Elect}}.

handle_continue({elect, ShareTopic, StartTime}, _State) ->
    elect(ShareTopic, StartTime).

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(?agent_connect_leader_match(Agent, AgentMetadata, _ShareTopic), State) ->
    %% NOTE: Redirecting to the known leader.
    ok = connect_leader(Agent, AgentMetadata, State),
    {noreply, State};
handle_info({timeout, _TRef, invalidate}, State) ->
    {stop, {shutdown, invalidate}, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

elect(ShareTopic, TS) ->
    Group = emqx_ds_shared_sub_leader:group_name(ShareTopic),
    case emqx_ds_shared_sub_leader_store:claim_leadership(Group, _Leader = self(), TS) of
        {ok, LeaderClaim} ->
            %% Become the leader.
            ?tp(debug, shared_sub_elector_becomes_leader, #{
                id => ShareTopic,
                group => Group,
                leader => LeaderClaim
            }),
            emqx_ds_shared_sub_leader:become(ShareTopic, TS, LeaderClaim);
        {exists, LeaderClaim} ->
            %% Turn into the follower that redirects connect requests to the leader
            %% while it's considered alive. Note that the leader may in theory decide
            %% to let go of leadership earlier than that.
            AliveUntil = emqx_ds_shared_sub_leader_store:alive_until(LeaderClaim),
            ?tp(debug, shared_sub_elector_becomes_follower, #{
                id => ShareTopic,
                group => Group,
                leader => LeaderClaim,
                until => AliveUntil
            }),
            TTL = AliveUntil - TS,
            _TRef = erlang:start_timer(max(0, TTL), self(), invalidate),
            St = #follower{
                topic = ShareTopic,
                leader = emqx_ds_shared_sub_leader_store:leader_id(LeaderClaim),
                alive_until = AliveUntil
            },
            {noreply, St};
        {error, Class, Reason} = Error ->
            ?tp(warning, "Shared subscription leader election failed", #{
                id => ShareTopic,
                group => Group,
                error => Error
            }),
            case Class of
                recoverable -> StopReason = {shutdown, Reason};
                unrecoverable -> StopReason = Error
            end,
            {stop, StopReason, ShareTopic}
    end.

connect_leader(Agent, AgentMetadata, #follower{topic = ShareTopic, leader = Pid}) ->
    emqx_ds_shared_sub_proto:agent_connect_leader(Pid, Agent, AgentMetadata, ShareTopic).
