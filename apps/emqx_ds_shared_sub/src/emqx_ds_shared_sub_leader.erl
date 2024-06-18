%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_leader).

-behaviour(gen_statem).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_persistent_message.hrl").
-include("emqx_ds_shared_sub_proto.hrl").

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

-type stream_assignment() :: #{
    prev_version := emqx_maybe:t(emqx_ds_shared_sub_proto:version()),
    version := emqx_ds_shared_sub_proto:version(),
    streams := list(emqx_ds:stream())
}.

-type data() :: #{
    group := emqx_types:group(),
    topic := emqx_types:topic(),
    %% For ds router, not an actual session_id
    router_id := binary(),
    %% TODO
    %% Persist progress
    %% TODO
    %% Implement some stats to assign evenly?
    stream_progresses := #{
        emqx_ds:stream() => emqx_ds:iterator()
    },
    agent_stream_assignments := #{
        emqx_ds_shared_sub_proto:agent() => stream_assignment()
    },
    stream_assignments := #{
        emqx_ds:stream() => emqx_ds_shared_sub_proto:agent()
    }
}.

-export_type([
    options/0,
    data/0
]).

%% States

-define(waiting_registration, waiting_registration).
-define(replaying, replaying).

%% Events

-record(register, {
    register_fun :: fun(() -> pid())
}).
-record(renew_streams, {}).
-record(renew_leases, {}).

%% Constants

%% TODO
%% Move to settings
-define(RENEW_LEASE_INTERVAL, 5000).
-define(RENEW_STREAMS_INTERVAL, 5000).

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

callback_mode() -> handle_event_function.

init([#{topic_filter := #share{group = Group, topic = Topic}} = _Options]) ->
    Data = #{
        group => Group,
        topic => Topic,
        router_id => router_id(),
        stream_progresses => #{},
        stream_assignments => #{}
    },
    {ok, ?waiting_registration, Data}.

%%--------------------------------------------------------------------
%% waiting_registration state

handle_event({call, From}, #register{register_fun = Fun}, ?waiting_registration, Data) ->
    Self = self(),
    case Fun() of
        Self ->
            {next_state, ?replaying, Data, {reply, From, {ok, Self}}};
        OtherPid ->
            {stop_and_reply, normal, {reply, From, {ok, OtherPid}}}
    end;
%%--------------------------------------------------------------------
%% repalying state
handle_event(enter, _OldState, ?replaying, #{topic := Topic, router_id := RouterId} = _Data) ->
    ok = emqx_persistent_session_ds_router:do_add_route(Topic, RouterId),
    {keep_state_and_data, [
        {state_timeout, ?RENEW_LEASE_INTERVAL, #renew_leases{}},
        {state_timeout, 0, #renew_streams{}}
    ]};
handle_event(state_timeout, #renew_streams{}, ?replaying, Data0) ->
    Data1 = renew_streams(Data0),
    {keep_state, Data1, {state_timeout, ?RENEW_STREAMS_INTERVAL, #renew_streams{}}};
handle_event(state_timeout, #renew_leases{}, ?replaying, Data0) ->
    Data1 = renew_leases(Data0),
    {keep_state, Data1, {state_timeout, ?RENEW_LEASE_INTERVAL, #renew_leases{}}};
handle_event(info, ?agent_connect_leader_match(Agent, _TopicFilter), ?replaying, Data0) ->
    Data1 = connect_agent(Data0, Agent),
    {keep_state, Data1};
handle_event(
    info, ?agent_update_stream_states_match(Agent, StreamProgresses, Version), ?replaying, Data0
) ->
    Data1 = update_agent_stream_states(Data0, Agent, StreamProgresses, Version),
    {keep_state, Data1};
%%--------------------------------------------------------------------
%% fallback
handle_event(enter, _OldState, _State, _Data) ->
    keep_state_and_data;
handle_event(Event, _Content, State, _Data) ->
    ?SLOG(warning, #{
        msg => unexpected_event,
        event => Event,
        state => State
    }),
    keep_state_and_data.

terminate(_Reason, _State, #{topic := Topic, router_id := RouterId} = _Data) ->
    ok = emqx_persistent_session_ds_router:do_delete_route(Topic, RouterId),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

renew_streams(#{stream_progresses := Progresses, topic := Topic} = Data0) ->
    TopicFilter = emqx_topic:words(Topic),
    StartTime = now_ms(),
    {_, Streams} = lists:unzip(
        emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, now_ms())
    ),
    %% TODO
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
    %% TODO
    %% Initiate reassigment
    Data0#{stream_progresses => NewProgresses}.

%% TODO
%% This just gives unassigned streams to connecting agent,
%% we need to implement actual stream (re)assignment.
connect_agent(
    #{
        group := Group,
        agent_stream_assignments := AgentStreamAssignments0,
        stream_assignments := StreamAssignments0,
        stream_progresses := StreamProgresses
    } = Data0,
    Agent
) ->
    {AgentStreamAssignments, StreamAssignments} =
        case AgentStreamAssignments0 of
            #{Agent := _} ->
                {AgentStreamAssignments0, StreamAssignments0};
            _ ->
                UnassignedStreams = unassigned_streams(Data0),
                Version = 0,
                StreamAssignment = #{
                    prev_version => undefined,
                    version => Version,
                    streams => UnassignedStreams
                },
                AgentStreamAssignments1 = AgentStreamAssignments0#{Agent => StreamAssignment},
                StreamAssignments1 = lists:foldl(
                    fun(Stream, Acc) ->
                        Acc#{Stream => Agent}
                    end,
                    StreamAssignments0,
                    UnassignedStreams
                ),
                StreamLease = lists:map(
                    fun(Stream) ->
                        #{
                            stream => Stream,
                            iterator => maps:get(Stream, StreamProgresses)
                        }
                    end,
                    UnassignedStreams
                ),
                ok = emqx_ds_shared_sub_proto:leader_lease_streams(
                    Agent, Group, StreamLease, Version
                ),
                {AgentStreamAssignments1, StreamAssignments1}
        end,
    Data0#{
        agent_stream_assignments => AgentStreamAssignments, stream_assignments => StreamAssignments
    }.

renew_leases(#{group := Group, agent_stream_assignments := AgentStreamAssignments} = Data) ->
    ok = lists:foreach(
        fun({Agent, #{version := Version}}) ->
            ok = emqx_ds_shared_sub_proto:leader_renew_stream_lease(Agent, Group, Version)
        end,
        maps:to_list(AgentStreamAssignments)
    ),
    Data.

update_agent_stream_states(
    #{
        agent_stream_assignments := AgentStreamAssignments,
        stream_assignments := StreamAssignments,
        stream_progresses := StreamProgresses0
    } = Data0,
    Agent,
    AgentStreamProgresses,
    Version
) ->
    AgentVersion = emqx_utils_maps:deep_get([Agent, version], AgentStreamAssignments, undefined),
    AgentPrevVersion = emqx_utils_maps:deep_get(
        [Agent, prev_version], AgentStreamAssignments, undefined
    ),
    case AgentVersion == Version orelse AgentPrevVersion == Version of
        false ->
            %% TODO
            %% send invalidate to agent
            Data0;
        true ->
            StreamProgresses1 = lists:foldl(
                fun(#{stream := Stream, iterator := It}, ProgressesAcc) ->
                    %% Assert Stream is assigned to Agent
                    Agent = maps:get(Stream, StreamAssignments),
                    ProgressesAcc#{Stream => It}
                end,
                StreamProgresses0,
                AgentStreamProgresses
            ),
            Data0#{stream_progresses => StreamProgresses1}
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

router_id() ->
    emqx_guid:to_hexstr(emqx_guid:gen()).

now_ms() ->
    erlang:system_time(millisecond).

unassigned_streams(#{stream_progresses := StreamProgresses, stream_assignments := StreamAssignments}) ->
    Streams = maps:keys(StreamProgresses),
    AssignedStreams = maps:keys(StreamAssignments),
    Streams -- AssignedStreams.
