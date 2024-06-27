%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_leader).

-behaviour(gen_statem).

-include("emqx_ds_shared_sub_proto.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_persistent_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    register/2,

    start_link/1,
    child_spec/1,
    id/1,

    callback_mode/0,
    init/1,
    handle_event/4,
    terminate/3
]).

-type options() :: #{
    topic_filter := emqx_persistent_session_ds:share_topic_filter()
}.

%% Agent states

-define(waiting_replaying, waiting_replaying).
-define(replaying, replaying).
-define(waiting_updating, waiting_updating).
-define(updating, updating).

-type agent_state() :: #{
    %% Our view of group gm's status
    %% it lags the actual state
    state := emqx_ds_shared_sub_agent:status(),
    prev_version := emqx_maybe:t(emqx_ds_shared_sub_proto:version()),
    version := emqx_ds_shared_sub_proto:version(),
    streams := list(emqx_ds:stream()),
    revoked_streams := list(emqx_ds:stream())
}.

-type data() :: #{
    group := emqx_types:group(),
    topic := emqx_types:topic(),
    %% For ds router, not an actual session_id
    router_id := binary(),
    %% TODO https://emqx.atlassian.net/browse/EMQX-12307
    %% Persist progress
    %% TODO https://emqx.atlassian.net/browse/EMQX-12575
    %% Implement some stats to assign evenly?
    stream_progresses := #{
        emqx_ds:stream() => emqx_ds:iterator()
    },
    agents := #{
        emqx_ds_shared_sub_proto:agent() => agent_state()
    },
    stream_owners := #{
        emqx_ds:stream() => emqx_ds_shared_sub_proto:agent()
    }
}.

-export_type([
    options/0,
    data/0
]).

%% States

-define(leader_waiting_registration, leader_waiting_registration).
-define(leader_active, leader_active).

%% Events

-record(register, {
    register_fun :: fun(() -> pid())
}).
-record(renew_streams, {}).
-record(renew_leases, {}).
-record(drop_timeout, {}).

%% Constants

%% TODO https://emqx.atlassian.net/browse/EMQX-12574
%% Move to settings
-define(RENEW_LEASE_INTERVAL, 1000).
-define(RENEW_STREAMS_INTERVAL, 1000).
-define(DROP_TIMEOUT_INTERVAL, 1000).

-define(AGENT_TIMEOUT, 5000).

-define(START_TIME_THRESHOLD, 5000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

register(Pid, Fun) ->
    gen_statem:call(Pid, #register{register_fun = Fun}).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

child_spec(#{topic_filter := TopicFilter} = Options) ->
    #{
        id => id(TopicFilter),
        start => {?MODULE, start_link, [Options]},
        restart => temporary,
        shutdown => 5000,
        type => worker
    }.

start_link(Options) ->
    gen_statem:start_link(?MODULE, [Options], []).

id(#share{group = Group} = _TopicFilter) ->
    {?MODULE, Group}.

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> [handle_event_function, state_enter].

init([#{topic_filter := #share{group = Group, topic = Topic}} = _Options]) ->
    Data = #{
        group => Group,
        topic => Topic,
        router_id => gen_router_id(),
        start_time => now_ms() - ?START_TIME_THRESHOLD,
        stream_progresses => #{},
        stream_owners => #{},
        agents => #{}
    },
    {ok, ?leader_waiting_registration, Data}.

%%--------------------------------------------------------------------
%% waiting_registration state

handle_event({call, From}, #register{register_fun = Fun}, ?leader_waiting_registration, Data) ->
    Self = self(),
    case Fun() of
        Self ->
            {next_state, ?leader_active, Data, {reply, From, {ok, Self}}};
        OtherPid ->
            {stop_and_reply, normal, {reply, From, {ok, OtherPid}}}
    end;
%%--------------------------------------------------------------------
%% repalying state
handle_event(enter, _OldState, ?leader_active, #{topic := Topic, router_id := RouterId} = _Data) ->
    ?tp(warning, shared_sub_leader_enter_actve, #{topic => Topic, router_id => RouterId}),
    ok = emqx_persistent_session_ds_router:do_add_route(Topic, RouterId),
    {keep_state_and_data, [
        {{timeout, #renew_streams{}}, 0, #renew_streams{}},
        {{timeout, #renew_leases{}}, ?RENEW_LEASE_INTERVAL, #renew_leases{}},
        {{timeout, #drop_timeout{}}, ?DROP_TIMEOUT_INTERVAL, #drop_timeout{}}
    ]};
%%--------------------------------------------------------------------
%% timers
%% renew_streams timer
handle_event({timeout, #renew_streams{}}, #renew_streams{}, ?leader_active, Data0) ->
    % ?tp(warning, shared_sub_leader_timeout, #{timeout => renew_streams}),
    Data1 = renew_streams(Data0),
    {keep_state, Data1, {{timeout, #renew_streams{}}, ?RENEW_STREAMS_INTERVAL, #renew_streams{}}};
%% renew_leases timer
handle_event({timeout, #renew_leases{}}, #renew_leases{}, ?leader_active, Data0) ->
    % ?tp(warning, shared_sub_leader_timeout, #{timeout => renew_leases}),
    Data1 = renew_leases(Data0),
    {keep_state, Data1, {{timeout, #renew_leases{}}, ?RENEW_LEASE_INTERVAL, #renew_leases{}}};
%% drop_timeout timer
handle_event({timeout, #drop_timeout{}}, #drop_timeout{}, ?leader_active, Data0) ->
    % ?tp(warning, shared_sub_leader_timeout, #{timeout => drop_timeout}),
    Data1 = drop_timeout_agents(Data0),
    {keep_state, Data1, {{timeout, #drop_timeout{}}, ?DROP_TIMEOUT_INTERVAL, #drop_timeout{}}};
%%--------------------------------------------------------------------
%% agent events
handle_event(info, ?agent_connect_leader_match(Agent, _TopicFilter), ?leader_active, Data0) ->
    % ?tp(warning, shared_sub_leader_connect_agent, #{agent => Agent}),
    Data1 = connect_agent(Data0, Agent),
    {keep_state, Data1};
handle_event(
    info,
    ?agent_update_stream_states_match(Agent, StreamProgresses, Version),
    ?leader_active,
    Data0
) ->
    % ?tp(warning, shared_sub_leader_update_stream_states, #{agent => Agent, version => Version}),
    Data1 = with_agent(Data0, Agent, fun() ->
        update_agent_stream_states(Data0, Agent, StreamProgresses, Version)
    end),
    {keep_state, Data1};
handle_event(
    info,
    ?agent_update_stream_states_match(Agent, StreamProgresses, VersionOld, VersionNew),
    ?leader_active,
    Data0
) ->
    % ?tp(warning, shared_sub_leader_update_stream_states, #{
    %     agent => Agent, version_old => VersionOld, version_new => VersionNew
    % }),
    Data1 = with_agent(Data0, Agent, fun() ->
        update_agent_stream_states(Data0, Agent, StreamProgresses, VersionOld, VersionNew)
    end),
    {keep_state, Data1};
%%--------------------------------------------------------------------
%% fallback
handle_event(enter, _OldState, _State, _Data) ->
    keep_state_and_data;
handle_event(Event, Content, State, _Data) ->
    ?SLOG(warning, #{
        msg => unexpected_event,
        event => Event,
        content => Content,
        state => State
    }),
    keep_state_and_data.

terminate(_Reason, _State, #{topic := Topic, router_id := RouterId} = _Data) ->
    ok = emqx_persistent_session_ds_router:do_delete_route(Topic, RouterId),
    ok.

%%--------------------------------------------------------------------
%% Event handlers
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Renew streams

%% * Find new streams in DS
%% * Revoke streams from agents having too many streams
%% * Assign streams to agents having too few streams

renew_streams(#{start_time := StartTime, stream_progresses := Progresses, topic := Topic} = Data0) ->
    TopicFilter = emqx_topic:words(Topic),
    {_, Streams} = lists:unzip(
        emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, StartTime)
    ),
    %% TODO https://emqx.atlassian.net/browse/EMQX-12572
    %% Handle stream removal
    NewProgresses = lists:foldl(
        fun(Stream, ProgressesAcc) ->
            case ProgressesAcc of
                #{Stream := _} ->
                    ProgressesAcc;
                _ ->
                    {ok, It} = emqx_ds:make_iterator(
                        ?PERSISTENT_MESSAGE_DB, Stream, TopicFilter, StartTime
                    ),
                    ProgressesAcc#{Stream => It}
            end
        end,
        Progresses,
        Streams
    ),
    Data1 = Data0#{stream_progresses => NewProgresses},
    ?SLOG(info, #{
        msg => leader_renew_streams,
        topic_filter => TopicFilter,
        streams => length(Streams)
    }),
    Data2 = revoke_streams(Data1),
    Data3 = assign_streams(Data2),
    Data3.

%% We revoke streams from agents that have too many streams (> desired_stream_count_per_agent).
%% We revoke only from replaying agents.
%% After revoking, no unassigned streams appear. Streams will become unassigned
%% only after agents report them as acked and unsubscribed.
revoke_streams(Data0) ->
    DesiredStreamsPerAgent = desired_stream_count_per_agent(Data0),
    Agents = replaying_agents(Data0),
    lists:foldl(
        fun(Agent, DataAcc) ->
            revoke_excess_streams_from_agent(DataAcc, Agent, DesiredStreamsPerAgent)
        end,
        Data0,
        Agents
    ).

revoke_excess_streams_from_agent(Data0, Agent, DesiredCount) ->
    #{streams := Streams0, revoked_streams := []} = AgentState0 = get_agent_state(Data0, Agent),
    RevokeCount = length(Streams0) - DesiredCount,
    AgentState1 =
        case RevokeCount > 0 of
            false ->
                AgentState0;
            true ->
                ?tp(warning, shared_sub_leader_revoke_streams, #{
                    agent => Agent,
                    agent_stream_count => length(Streams0),
                    revoke_count => RevokeCount,
                    desired_count => DesiredCount
                }),
                revoke_streams_from_agent(Data0, Agent, AgentState0, RevokeCount)
        end,
    set_agent_state(Data0, Agent, AgentState1).

revoke_streams_from_agent(
    Data,
    Agent,
    #{
        streams := Streams0, revoked_streams := []
    } = AgentState0,
    RevokeCount
) ->
    RevokedStreams = select_streams_for_revoke(Data, AgentState0, RevokeCount),
    Streams = Streams0 -- RevokedStreams,
    agent_transition_to_waiting_updating(Data, Agent, AgentState0, Streams, RevokedStreams).

select_streams_for_revoke(
    _Data, #{streams := Streams, revoked_streams := []} = _AgentState, RevokeCount
) ->
    %% TODO
    %% Some intellectual logic should be used regarding:
    %% * shard ids (better spread shards across different streams);
    %% * stream stats (how much data was replayed from stream,
    %%   heavy streams should be distributed across different agents);
    %% * data locality (agents better preserve streams with data available on the agent's node)
    lists:sublist(shuffle(Streams), RevokeCount).

%% We assign streams to agents that have too few streams (< desired_stream_count_per_agent).
%% We assign only to replaying agents.
assign_streams(Data0) ->
    DesiredStreamsPerAgent = desired_stream_count_per_agent(Data0),
    Agents = replaying_agents(Data0),
    lists:foldl(
        fun(Agent, DataAcc) ->
            assign_lacking_streams(DataAcc, Agent, DesiredStreamsPerAgent)
        end,
        Data0,
        Agents
    ).

assign_lacking_streams(Data0, Agent, DesiredCount) ->
    #{streams := Streams0, revoked_streams := []} = get_agent_state(Data0, Agent),
    AssignCount = DesiredCount - length(Streams0),
    case AssignCount > 0 of
        false ->
            Data0;
        true ->
            ?tp(warning, shared_sub_leader_assign_streams, #{
                agent => Agent,
                agent_stream_count => length(Streams0),
                assign_count => AssignCount,
                desired_count => DesiredCount
            }),
            assign_streams_to_agent(Data0, Agent, AssignCount)
    end.

assign_streams_to_agent(Data0, Agent, AssignCount) ->
    StreamsToAssign = select_streams_for_assign(Data0, Agent, AssignCount),
    Data1 = set_stream_ownership_to_agent(Data0, Agent, StreamsToAssign),
    #{agents := #{Agent := AgentState0}} = Data1,
    #{streams := Streams0, revoked_streams := []} = AgentState0,
    Streams1 = Streams0 ++ StreamsToAssign,
    AgentState1 = agent_transition_to_waiting_updating(Data0, Agent, AgentState0, Streams1, []),
    set_agent_state(Data1, Agent, AgentState1).

select_streams_for_assign(Data0, _Agent, AssignCount) ->
    %% TODO
    %% Some intellectual logic should be used. See `select_streams_for_revoke/3`.
    UnassignedStreams = unassigned_streams(Data0),
    lists:sublist(shuffle(UnassignedStreams), AssignCount).

%%--------------------------------------------------------------------
%% Handle a newly connected agent

connect_agent(
    #{group := Group} = Data,
    Agent
) ->
    %% TODO
    %% implement graceful reconnection of the same agent
    ?SLOG(info, #{
        msg => leader_agent_connected,
        agent => Agent,
        group => Group
    }),
    DesiredCount = desired_stream_count_for_new_agent(Data),
    assign_initial_streams_to_agent(Data, Agent, DesiredCount).

assign_initial_streams_to_agent(Data, Agent, AssignCount) ->
    InitialStreamsToAssign = select_streams_for_assign(Data, Agent, AssignCount),
    Data1 = set_stream_ownership_to_agent(Data, Agent, InitialStreamsToAssign),
    AgentState = agent_transition_to_initial_waiting_replaying(
        Data1, Agent, InitialStreamsToAssign
    ),
    set_agent_state(Data1, Agent, AgentState).

%%--------------------------------------------------------------------
%% Drop agents that stopped reporting progress

drop_timeout_agents(#{agents := Agents} = Data) ->
    Now = now_ms(),
    lists:foldl(
        fun({Agent, #{update_deadline := Deadline} = _AgentState}, DataAcc) ->
            case Deadline < Now of
                true ->
                    ?SLOG(info, #{
                        msg => leader_agent_timeout,
                        agent => Agent
                    }),
                    drop_invalidate_agent(DataAcc, Agent);
                false ->
                    DataAcc
            end
        end,
        Data,
        maps:to_list(Agents)
    ).

%%--------------------------------------------------------------------
%% Send lease confirmations to agents

renew_leases(#{agents := AgentStates} = Data) ->
    ?tp(warning, shared_sub_leader_renew_leases, #{agents => maps:keys(AgentStates)}),
    ok = lists:foreach(
        fun({Agent, AgentState}) ->
            renew_lease(Data, Agent, AgentState)
        end,
        maps:to_list(AgentStates)
    ),
    Data.

renew_lease(#{group := Group}, Agent, #{state := ?replaying, version := Version}) ->
    ok = emqx_ds_shared_sub_proto:leader_renew_stream_lease(Agent, Group, Version);
renew_lease(#{group := Group}, Agent, #{state := ?waiting_replaying, version := Version}) ->
    ok = emqx_ds_shared_sub_proto:leader_renew_stream_lease(Agent, Group, Version);
renew_lease(#{group := Group} = Data, Agent, #{
    streams := Streams, state := ?waiting_updating, version := Version, prev_version := PrevVersion
}) ->
    StreamProgresses = stream_progresses(Data, Streams),
    ok = emqx_ds_shared_sub_proto:leader_update_streams(
        Agent, Group, PrevVersion, Version, StreamProgresses
    ),
    ok = emqx_ds_shared_sub_proto:leader_renew_stream_lease(Agent, Group, PrevVersion, Version);
renew_lease(#{group := Group}, Agent, #{
    state := ?updating, version := Version, prev_version := PrevVersion
}) ->
    ok = emqx_ds_shared_sub_proto:leader_renew_stream_lease(Agent, Group, PrevVersion, Version).

%%--------------------------------------------------------------------
%% Handle stream progress updates from agent in replaying state

update_agent_stream_states(Data0, Agent, AgentStreamProgresses, Version) ->
    #{state := State, version := AgentVersion, prev_version := AgentPrevVersion} =
        AgentState0 = get_agent_state(Data0, Agent),
    case {State, Version} of
        {?waiting_updating, AgentPrevVersion} ->
            %% Stale update, ignoring
            Data0;
        {?waiting_replaying, AgentVersion} ->
            %% Agent finished updating, now replaying
            Data1 = update_stream_progresses(Data0, Agent, AgentStreamProgresses),
            AgentState1 = update_agent_timeout(AgentState0),
            AgentState2 = agent_transition_to_replaying(Agent, AgentState1),
            set_agent_state(Data1, Agent, AgentState2);
        {?replaying, AgentVersion} ->
            %% Common case, agent is replaying
            Data1 = update_stream_progresses(Data0, Agent, AgentStreamProgresses),
            AgentState1 = update_agent_timeout(AgentState0),
            set_agent_state(Data1, Agent, AgentState1);
        {OtherState, OtherVersion} ->
            ?tp(warning, unexpected_update, #{
                agent => Agent,
                update_version => OtherVersion,
                state => OtherState,
                our_agent_version => AgentVersion,
                our_agent_prev_version => AgentPrevVersion
            }),
            drop_invalidate_agent(Data0, Agent)
    end.

update_stream_progresses(
    #{stream_progresses := StreamProgresses0, stream_owners := StreamOwners} = Data,
    Agent,
    ReceivedStreamProgresses
) ->
    StreamProgresses1 = lists:foldl(
        fun(#{stream := Stream, iterator := It}, ProgressesAcc) ->
            case StreamOwners of
                #{Stream := Agent} ->
                    ProgressesAcc#{Stream => It};
                _ ->
                    ProgressesAcc
            end
        end,
        StreamProgresses0,
        ReceivedStreamProgresses
    ),
    Data#{
        stream_progresses => StreamProgresses1
    }.

clean_revoked_streams(
    Data0, #{revoked_streams := RevokedStreams0} = AgentState0, ReceivedStreamProgresses
) ->
    FinishedReportedStreams = maps:from_list(
        lists:filtermap(
            fun
                (
                    #{
                        stream := Stream,
                        use_finished := true
                    }
                ) ->
                    {true, {Stream, true}};
                (_) ->
                    false
            end,
            ReceivedStreamProgresses
        )
    ),
    {FinishedStreams, StillRevokingStreams} = lists:partition(
        fun(Stream) ->
            maps:is_key(Stream, FinishedReportedStreams)
        end,
        RevokedStreams0
    ),
    Data1 = unassign_streams(Data0, FinishedStreams),
    AgentState1 = AgentState0#{revoked_streams => StillRevokingStreams},
    {AgentState1, Data1}.

unassign_streams(#{stream_owners := StreamOwners0} = Data, Streams) ->
    StreamOwners1 = lists:foldl(
        fun(Stream, StreamOwnersAcc) ->
            maps:remove(Stream, StreamOwnersAcc)
        end,
        StreamOwners0,
        Streams
    ),
    Data#{
        stream_owners => StreamOwners1
    }.

%%--------------------------------------------------------------------
%% Handle stream progress updates from agent in updating (VersionOld -> VersionNew) state

update_agent_stream_states(Data0, Agent, AgentStreamProgresses, VersionOld, VersionNew) ->
    #{state := State, version := AgentVersion, prev_version := AgentPrevVersion} =
        AgentState0 = get_agent_state(Data0, Agent),
    case {State, VersionOld, VersionNew} of
        {?waiting_updating, AgentPrevVersion, AgentVersion} ->
            %% Client started updating
            Data1 = update_stream_progresses(Data0, Agent, AgentStreamProgresses),
            AgentState1 = update_agent_timeout(AgentState0),
            {AgentState2, Data2} = clean_revoked_streams(Data1, AgentState1, AgentStreamProgresses),
            AgentState3 =
                case AgentState2 of
                    #{revoked_streams := []} ->
                        agent_transition_to_waiting_replaying(Data1, Agent, AgentState2);
                    _ ->
                        agent_transition_to_updating(Agent, AgentState2)
                end,
            set_agent_state(Data2, Agent, AgentState3);
        {?updating, AgentPrevVersion, AgentVersion} ->
            Data1 = update_stream_progresses(Data0, Agent, AgentStreamProgresses),
            AgentState1 = update_agent_timeout(AgentState0),
            {AgentState2, Data2} = clean_revoked_streams(Data1, AgentState1, AgentStreamProgresses),
            AgentState3 =
                case AgentState2 of
                    #{revoked_streams := []} ->
                        agent_transition_to_waiting_replaying(Data1, Agent, AgentState2);
                    _ ->
                        AgentState2
                end,
            set_agent_state(Data2, Agent, AgentState3);
        {?waiting_replaying, _, AgentVersion} ->
            Data1 = update_stream_progresses(Data0, Agent, AgentStreamProgresses),
            AgentState1 = update_agent_timeout(AgentState0),
            set_agent_state(Data1, Agent, AgentState1);
        {?replaying, _, AgentVersion} ->
            Data1 = update_stream_progresses(Data0, Agent, AgentStreamProgresses),
            AgentState1 = update_agent_timeout(AgentState0),
            set_agent_state(Data1, Agent, AgentState1);
        {OtherState, OtherVersionOld, OtherVersionNew} ->
            ?tp(warning, unexpected_update, #{
                agent => Agent,
                update_version_old => OtherVersionOld,
                update_version_new => OtherVersionNew,
                state => OtherState,
                our_agent_version => AgentVersion,
                our_agent_prev_version => AgentPrevVersion
            }),
            drop_invalidate_agent(Data0, Agent)
    end.

%%--------------------------------------------------------------------
%% Agent state transitions
%%--------------------------------------------------------------------

agent_transition_to_waiting_updating(
    #{group := Group} = Data,
    Agent,
    #{state := OldState, version := Version, prev_version := undefined} = AgentState0,
    Streams,
    RevokedStreams
) ->
    ?tp(warning, shared_sub_leader_agent_state_transition, #{
        agent => Agent,
        old_state => OldState,
        new_state => ?waiting_updating
    }),
    NewVersion = next_version(Version),

    AgentState1 = AgentState0#{
        state => ?waiting_updating,
        streams => Streams,
        revoked_streams => RevokedStreams,
        prev_version => Version,
        version => NewVersion
    },
    StreamProgresses = stream_progresses(Data, Streams),
    ok = emqx_ds_shared_sub_proto:leader_update_streams(
        Agent, Group, Version, NewVersion, StreamProgresses
    ),
    AgentState1.

agent_transition_to_waiting_replaying(
    #{group := Group} = _Data, Agent, #{state := OldState, version := Version} = AgentState0
) ->
    ?tp(warning, shared_sub_leader_agent_state_transition, #{
        agent => Agent,
        old_state => OldState,
        new_state => ?waiting_replaying
    }),
    ok = emqx_ds_shared_sub_proto:leader_renew_stream_lease(Agent, Group, Version),
    AgentState0#{
        state => ?waiting_replaying,
        revoked_streams => []
    }.

agent_transition_to_initial_waiting_replaying(
    #{group := Group} = Data, Agent, InitialStreams
) ->
    ?tp(warning, shared_sub_leader_agent_state_transition, #{
        agent => Agent,
        old_state => none,
        new_state => ?waiting_replaying
    }),
    Version = 0,
    StreamProgresses = stream_progresses(Data, InitialStreams),
    Leader = this_leader(Data),
    ok = emqx_ds_shared_sub_proto:leader_lease_streams(
        Agent, Group, Leader, StreamProgresses, Version
    ),
    #{
        state => ?waiting_replaying,
        version => Version,
        prev_version => undefined,
        streams => InitialStreams,
        revoked_streams => [],
        update_deadline => now_ms() + ?AGENT_TIMEOUT
    }.

agent_transition_to_replaying(Agent, #{state := ?waiting_replaying} = AgentState) ->
    ?tp(warning, shared_sub_leader_agent_state_transition, #{
        agent => Agent,
        old_state => ?waiting_replaying,
        new_state => ?replaying
    }),
    AgentState#{
        state => ?replaying,
        prev_version => undefined
    }.

agent_transition_to_updating(Agent, #{state := ?waiting_updating} = AgentState) ->
    ?tp(warning, shared_sub_leader_agent_state_transition, #{
        agent => Agent,
        old_state => ?waiting_updating,
        new_state => ?updating
    }),
    AgentState#{state => ?updating}.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

gen_router_id() ->
    emqx_guid:to_hexstr(emqx_guid:gen()).

now_ms() ->
    erlang:system_time(millisecond).

unassigned_streams(#{stream_progresses := StreamProgresses, stream_owners := StreamOwners}) ->
    Streams = maps:keys(StreamProgresses),
    AssignedStreams = maps:keys(StreamOwners),
    Streams -- AssignedStreams.

%% Those who are not connecting or updating, i.e. not in a transient state.
replaying_agents(#{agents := AgentStates}) ->
    lists:filtermap(
        fun
            ({Agent, #{state := ?replaying}}) ->
                {true, Agent};
            (_) ->
                false
        end,
        maps:to_list(AgentStates)
    ).

desired_stream_count_per_agent(#{agents := AgentStates} = Data) ->
    desired_stream_count_per_agent(Data, maps:size(AgentStates)).

desired_stream_count_for_new_agent(#{agents := AgentStates} = Data) ->
    desired_stream_count_per_agent(Data, maps:size(AgentStates) + 1).

desired_stream_count_per_agent(#{stream_progresses := StreamProgresses}, AgentCount) ->
    case AgentCount of
        0 ->
            0;
        _ ->
            StreamCount = maps:size(StreamProgresses),
            case StreamCount rem AgentCount of
                0 ->
                    StreamCount div AgentCount;
                _ ->
                    1 + StreamCount div AgentCount
            end
    end.

stream_progresses(#{stream_progresses := StreamProgresses} = _Data, Streams) ->
    lists:map(
        fun(Stream) ->
            #{
                stream => Stream,
                iterator => maps:get(Stream, StreamProgresses)
            }
        end,
        Streams
    ).

next_version(Version) ->
    Version + 1.

shuffle(L0) ->
    L1 = lists:map(
        fun(A) ->
            {rand:uniform(), A}
        end,
        L0
    ),
    L2 = lists:sort(L1),
    {_, L} = lists:unzip(L2),
    L.

set_stream_ownership_to_agent(#{stream_owners := StreamOwners0} = Data, Agent, Streams) ->
    StreamOwners1 = lists:foldl(
        fun(Stream, Acc) ->
            Acc#{Stream => Agent}
        end,
        StreamOwners0,
        Streams
    ),
    Data#{
        stream_owners => StreamOwners1
    }.

set_agent_state(#{agents := Agents} = Data, Agent, AgentState) ->
    Data#{
        agents => Agents#{Agent => AgentState}
    }.

update_agent_timeout(AgentState) ->
    AgentState#{
        update_deadline => now_ms() + ?AGENT_TIMEOUT
    }.

get_agent_state(#{agents := Agents} = _Data, Agent) ->
    maps:get(Agent, Agents).

this_leader(_Data) ->
    self().

drop_agent(#{agents := Agents} = Data0, Agent) ->
    AgentState = get_agent_state(Data0, Agent),
    #{streams := Streams, revoked_streams := RevokedStreams} = AgentState,
    AllStreams = Streams ++ RevokedStreams,
    Data1 = unassign_streams(Data0, AllStreams),
    Data1#{agents => maps:remove(Agent, Agents)}.

invalidate_agent(#{group := Group}, Agent) ->
    ok = emqx_ds_shared_sub_proto:leader_invalidate(Agent, Group).

drop_invalidate_agent(Data0, Agent) ->
    Data1 = drop_agent(Data0, Agent),
    ok = invalidate_agent(Data1, Agent),
    Data1.

with_agent(#{agents := Agents} = Data, Agent, Fun) ->
    case Agents of
        #{Agent := _} ->
            Fun();
        _ ->
            Data
    end.
