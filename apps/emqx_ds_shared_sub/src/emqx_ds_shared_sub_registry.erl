%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_registry).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include("emqx_ds_shared_sub.hrl").

-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([
    lookup_leader/2
]).

-record(lookup_leader, {
    agent :: emqx_ds_shared_sub:agent(),
    topic_filter :: emqx_persistent_session_ds:share_topic_filter()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

lookup_leader(Agent, TopicFilter) ->
    gen_server:cast(?MODULE, #lookup_leader{agent = Agent, topic_filter = TopicFilter}).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(#lookup_leader{agent = Agent, topic_filter = TopicFilter}, State) ->
    State1 = do_lookup_leader(Agent, TopicFilter, State),
    {noreply, State1}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_lookup_leader(Agent, TopicFilter, State) ->
    %% TODO
    %% Cluster-wide unique leader election should be implemented
    Id = emqx_ds_shared_sub_leader:id(TopicFilter),
    LeaderPid =
        case gproc:where(?gproc_id(Id)) of
            undefined ->
                {ok, Pid} = emqx_ds_shared_sub_leader_sup:start_leader(#{
                    topic_filter => TopicFilter
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
    ok = emqx_ds_shared_sub_proto:agent_connect_leader(LeaderPid, Agent, TopicFilter),
    State.
