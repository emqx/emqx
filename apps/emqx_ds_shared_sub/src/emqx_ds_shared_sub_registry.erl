%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_registry).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-export([
    start_link/0,
    child_spec/0,

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([
    lookup_leader/3
]).

-record(lookup_leader, {
    agent :: emqx_ds_shared_sub_proto:agent(),
    agent_metadata :: emqx_ds_shared_sub_proto:agent_metadata(),
    share_topic_filter :: emqx_persistent_session_ds:share_topic_filter()
}).

-define(gproc_id(ID), {n, l, ID}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec lookup_leader(
    emqx_ds_shared_sub_proto:agent(),
    emqx_ds_shared_sub_proto:agent_metadata(),
    emqx_persistent_session_ds:share_topic_filter()
) -> ok.
lookup_leader(Agent, AgentMetadata, ShareTopicFilter) ->
    gen_server:cast(?MODULE, #lookup_leader{
        agent = Agent, agent_metadata = AgentMetadata, share_topic_filter = ShareTopicFilter
    }).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker
    }.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(
    #lookup_leader{
        agent = Agent,
        agent_metadata = AgentMetadata,
        share_topic_filter = ShareTopicFilter
    },
    State
) ->
    State1 = do_lookup_leader(Agent, AgentMetadata, ShareTopicFilter, State),
    {noreply, State1}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_lookup_leader(Agent, AgentMetadata, ShareTopicFilter, State) ->
    %% TODO https://emqx.atlassian.net/browse/EMQX-12309
    %% Cluster-wide unique leader election should be implemented
    Id = emqx_ds_shared_sub_leader:id(ShareTopicFilter),
    LeaderPid =
        case gproc:where(?gproc_id(Id)) of
            undefined ->
                {ok, Pid} = emqx_ds_shared_sub_leader_sup:start_leader(#{
                    share_topic_filter => ShareTopicFilter
                }),
                {ok, NewLeaderPid} = emqx_ds_shared_sub_leader:register(
                    Pid,
                    fun() ->
                        {LPid, _} = gproc:reg_or_locate(?gproc_id(Id)),
                        LPid
                    end
                ),
                NewLeaderPid;
            Pid ->
                Pid
        end,
    ?SLOG(info, #{
        msg => lookup_leader,
        agent => Agent,
        share_topic_filter => ShareTopicFilter,
        leader => LeaderPid
    }),
    ok = emqx_ds_shared_sub_proto:agent_connect_leader(
        LeaderPid, Agent, AgentMetadata, ShareTopicFilter
    ),
    State.
