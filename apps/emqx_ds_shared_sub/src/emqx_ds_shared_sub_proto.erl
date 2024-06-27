%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% TODO https://emqx.atlassian.net/browse/EMQX-12573
%% This should be wrapped with a proto_v1 module.
%% For simplicity, send as simple OTP messages for now.

-module(emqx_ds_shared_sub_proto).

-include("emqx_ds_shared_sub_proto.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    agent_connect_leader/3,
    agent_update_stream_states/4,
    agent_update_stream_states/5,

    leader_lease_streams/5,
    leader_renew_stream_lease/3,
    leader_renew_stream_lease/4,
    leader_update_streams/5,
    leader_invalidate/2
]).

-export([
    format_streams/1,
    agent/2
]).

-ifdef(TEST).
-record(agent, {
    pid :: pid(),
    id :: term()
}).
-type agent() :: #agent{}.
-else.
-type agent() :: pid().
-endif.

-type leader() :: pid().
-type topic_filter() :: emqx_persistent_session_ds:share_topic_filter().
-type group() :: emqx_types:group().
-type version() :: non_neg_integer().

-type stream_progress() :: #{
    stream := emqx_ds:stream(),
    iterator := emqx_ds:iterator()
}.

-type agent_stream_progress() :: #{
    stream := emqx_ds:stream(),
    iterator := emqx_ds:iterator(),
    use_finished := boolean()
}.

-export_type([
    agent/0,
    leader/0,
    group/0,
    version/0,
    stream_progress/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% agent -> leader messages

-spec agent_connect_leader(leader(), agent(), topic_filter()) -> ok.
agent_connect_leader(ToLeader, FromAgent, TopicFilter) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => agent_connect_leader,
        to_leader => ToLeader,
        from_agent => FromAgent,
        topic_filter => TopicFilter
    }),
    _ = erlang:send(ToLeader, ?agent_connect_leader(FromAgent, TopicFilter)),
    ok.

-spec agent_update_stream_states(leader(), agent(), list(agent_stream_progress()), version()) -> ok.
agent_update_stream_states(ToLeader, FromAgent, StreamProgresses, Version) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => agent_update_stream_states,
        to_leader => ToLeader,
        from_agent => FromAgent,
        stream_progresses => format_streams(StreamProgresses),
        version => Version
    }),
    _ = erlang:send(ToLeader, ?agent_update_stream_states(FromAgent, StreamProgresses, Version)),
    ok.

-spec agent_update_stream_states(
    leader(), agent(), list(agent_stream_progress()), version(), version()
) -> ok.
agent_update_stream_states(ToLeader, FromAgent, StreamProgresses, VersionOld, VersionNew) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => agent_update_stream_states,
        to_leader => ToLeader,
        from_agent => FromAgent,
        stream_progresses => format_streams(StreamProgresses),
        version_old => VersionOld,
        version_new => VersionNew
    }),
    _ = erlang:send(
        ToLeader, ?agent_update_stream_states(FromAgent, StreamProgresses, VersionOld, VersionNew)
    ),
    ok.

%% leader -> agent messages

-spec leader_lease_streams(agent(), group(), leader(), list(stream_progress()), version()) -> ok.
leader_lease_streams(ToAgent, OfGroup, Leader, Streams, Version) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => leader_lease_streams,
        to_agent => ToAgent,
        of_group => OfGroup,
        leader => Leader,
        streams => format_streams(Streams),
        version => Version
    }),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        agent_pid(ToAgent),
        ?leader_lease_streams(OfGroup, Leader, Streams, Version)
    ),
    ok.

-spec leader_renew_stream_lease(agent(), group(), version()) -> ok.
leader_renew_stream_lease(ToAgent, OfGroup, Version) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => leader_renew_stream_lease,
        to_agent => ToAgent,
        of_group => OfGroup,
        version => Version
    }),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        agent_pid(ToAgent),
        ?leader_renew_stream_lease(OfGroup, Version)
    ),
    ok.

-spec leader_renew_stream_lease(agent(), group(), version(), version()) -> ok.
leader_renew_stream_lease(ToAgent, OfGroup, VersionOld, VersionNew) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => leader_renew_stream_lease,
        to_agent => ToAgent,
        of_group => OfGroup,
        version_old => VersionOld,
        version_new => VersionNew
    }),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        agent_pid(ToAgent),
        ?leader_renew_stream_lease(OfGroup, VersionOld, VersionNew)
    ),
    ok.

-spec leader_update_streams(agent(), group(), version(), version(), list(stream_progress())) -> ok.
leader_update_streams(ToAgent, OfGroup, VersionOld, VersionNew, StreamsNew) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => leader_update_streams,
        to_agent => ToAgent,
        of_group => OfGroup,
        version_old => VersionOld,
        version_new => VersionNew,
        streams_new => format_streams(StreamsNew)
    }),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        agent_pid(ToAgent),
        ?leader_update_streams(OfGroup, VersionOld, VersionNew, StreamsNew)
    ),
    ok.

-spec leader_invalidate(agent(), group()) -> ok.
leader_invalidate(ToAgent, OfGroup) ->
    ?tp(warning, shared_sub_proto_msg, #{
        type => leader_invalidate,
        to_agent => ToAgent,
        of_group => OfGroup
    }),
    _ = emqx_persistent_session_ds_shared_subs_agent:send(
        agent_pid(ToAgent),
        ?leader_invalidate(OfGroup)
    ),
    ok.

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

-ifdef(TEST).
agent(Id, Pid) ->
    #agent{id = Id, pid = Pid}.

agent_pid(#agent{pid = Pid}) ->
    Pid.

-else.
agent(_Id, Pid) ->
    Pid.

agent_pid(Pid) ->
    Pid.
-endif.

format_streams(Streams) ->
    lists:map(
        fun format_stream/1,
        Streams
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

format_opaque(Opaque) ->
    erlang:phash2(Opaque).

format_stream(#{stream := Stream, iterator := Iterator} = Value) ->
    Value#{stream => format_opaque(Stream), iterator => format_opaque(Iterator)}.
