%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc A property-based testcase that verifies durable session state
%% serialization and deserialization consistency.
-module(emqx_persistent_session_ds_state_fuzzer).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include("../src/emqx_persistent_session_ds/session_internals.hrl").
-include("../src/emqx_persistent_session_ds/pmap.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(tab, ?MODULE).

%% Model state of the session.
%%
%% Note: here `committed' != `dirty'. It means "has been committed at
%% least once since the creation", and it's used by the iteration
%% test.
-record(s, {
    metadata = #{},
    subscriptions = #{},
    subscription_states = #{},
    seqnos = #{},
    streams = #{},
    ranks = #{},
    awaiting_rel = #{},
    committed = false,
    checkpointing = false
}).

%% Model state:
-record(gs, {
    ds_broken = false :: boolean(),
    sessions = #{} :: #{emqx_persistent_session_ds:id() => #s{}}
}).

%%================================================================================
%% Properties
%%================================================================================

%%================================================================================
%% Generators
%%================================================================================

-define(n_sessions, 2).

shard_gen() ->
    oneof([<<"0">>, <<"1">>, <<"100">>]).

session_id() ->
    oneof([integer_to_binary(I) || I <- lists:seq(1, ?n_sessions)]).

existing_session_id(S) ->
    oneof(maps:keys(S)).

topic() ->
    oneof([<<"foo">>, <<"bar">>, <<"foo/#">>, <<"//+/#">>, <<"#">>]).

share_tf() ->
    #share{group = binary(), topic = topic()}.

maybe_share_tf() ->
    oneof([topic(), share_tf()]).

subscription() ->
    oneof([#{}]).

session_id(#gs{sessions = S}) ->
    session_id(S);
session_id(S) when is_map(S) ->
    oneof(maps:keys(S)).

batch_size() ->
    range(1, ?n_sessions).

put_metadata() ->
    oneof([
        ?LET(
            Val,
            range(0, 100),
            {last_alive_at, set_last_alive_at, Val}
        ),
        ?LET(
            Val,
            range(0, 100),
            {created_at, set_created_at, Val}
        ),
        ?LET(
            Val,
            range(0, 16#FFFFFFFF),
            {expiry_interval, set_expiry_interval, Val}
        )
    ]).

get_metadata() ->
    oneof([
        {last_alive_at, get_last_alive_at},
        {created_at, get_created_at},
        {expiry_interval, get_expiry_interval}
    ]).

seqno() ->
    range(1, 100).

stream_id() ->
    {range(1, 10), oneof([#{}, {}])}.

subscription_gen() ->
    ?LET(
        {Id, CS, ST},
        {uniq_id(), uniq_id(), non_neg_integer()},
        #{
            id => Id,
            current_state => CS,
            start_time => ST
        }
    ).

uniq_id() ->
    %% TODO: Not so unique. However, currently we don't have code that
    %% relies on uniqueness.
    non_neg_integer().

sub_mode() ->
    union([durable, direct]).

%% Proper generator for subscription states.
sub_state_gen() ->
    oneof(
        [
            %% Without optional fields:
            ?LET(
                {Par, UQ, SO, Mode},
                {uniq_id(), boolean(), emqx_proper_types:subopts(), sub_mode()},
                #{
                    parent_subscription => Par,
                    upgrade_qos => UQ,
                    subopts => SO,
                    mode => Mode
                }
            ),
            %% With "superseded by" field:
            ?LET(
                {Par, UQ, SO, SB, Mode},
                {uniq_id(), boolean(), emqx_proper_types:subopts(), uniq_id(), sub_mode()},
                #{
                    parent_subscription => Par,
                    upgrade_qos => UQ,
                    subopts => SO,
                    superseded_by => SB,
                    mode => Mode
                }
            ),
            %% With share topic filter:
            ?LET(
                {Par, UQ, SO, STF, Mode},
                {uniq_id(), boolean(), emqx_proper_types:subopts(), share_tf(), sub_mode()},
                #{
                    parent_subscription => Par,
                    upgrade_qos => UQ,
                    subopts => SO,
                    share_topic_filter => STF,
                    mode => Mode
                }
            )
        ]
    ).

srs_gen() ->
    It =
        ?LET(
            Foo,
            integer(),
            {iter, [#{foo => Foo}]}
        ),
    #srs{
        rank_x = shard_gen(),
        rank_y = non_neg_integer(),
        it_begin = It,
        it_end = It,
        batch_size = non_neg_integer(),
        first_seqno_qos1 = non_neg_integer(),
        first_seqno_qos2 = non_neg_integer(),
        last_seqno_qos1 = non_neg_integer(),
        last_seqno_qos2 = non_neg_integer(),
        unsubscribed = boolean(),
        sub_state_id = uniq_id()
    }.

seqno_track_gen() ->
    oneof([
        ?committed(?QOS_1),
        ?committed(?QOS_2),
        ?dup(?QOS_1),
        ?dup(?QOS_2),
        ?rec,
        ?next(?QOS_1),
        ?next(?QOS_2)
    ]).

%% @doc Proper generator that returns random pmap put requests
put_req() ->
    oneof([
        %% Subscription:
        ?LET(
            {SubId, Sub},
            {maybe_share_tf(), subscription_gen()},
            {#s.subscriptions, put_subscription, SubId, Sub}
        ),
        %% Subscription state:
        ?LET(
            {SSId, SubS},
            {uniq_id(), sub_state_gen()},
            {#s.subscription_states, put_subscription_state, SSId, SubS}
        ),
        %% Seqnos
        ?LET(
            {SeqNoType, SeqNo},
            {seqno_track_gen(), non_neg_integer()},
            {#s.seqnos, put_seqno, SeqNoType, SeqNo}
        ),
        %% Stream
        ?LET(
            {Id, Stream},
            {stream_id(), srs_gen()},
            {#s.streams, put_stream, Id, Stream}
        ),
        %% Ranks
        ?LET(
            {RankKey, Rank},
            {{uniq_id(), shard_gen()}, integer()},
            {#s.ranks, put_rank, RankKey, Rank}
        ),
        %% Awaiting rel:
        ?LET(
            {PacketId, Timestamp},
            {emqx_proper_types:packet_id(), integer()},
            {#s.awaiting_rel, put_awaiting_rel, PacketId, Timestamp}
        )
    ]).

%% @doc A generic proper generator that randomly selects an existing
%% session, one of its pmaps, and a key that may or may not exist in
%% the pmap. Returns index of the pmap in #s, session id and the pmap
%% key.
pmap_type_and_key(#gs{sessions = S}) ->
    Recs = [
        #s.subscriptions, #s.subscription_states, #s.seqnos, #s.streams, #s.ranks, #s.awaiting_rel
    ],
    ?LET(
        {SessId, Idx},
        {session_id(S), oneof(Recs)},
        begin
            #{SessId := Rec} = S,
            Keys = maps:keys(element(Idx, Rec)),
            ?LET(
                Key,
                oneof([one_that_does_not_exist | Keys]),
                {Idx, SessId, Key}
            )
        end
    ).

%% @doc Proper generator that returns random pmap get requests
get_req(S) ->
    ?LET(
        {Idx, SessId, Key},
        pmap_type_and_key(S),
        [
            SessId,
            {Idx,
                case Idx of
                    #s.subscriptions -> get_subscription;
                    #s.subscription_states -> get_subscription_state;
                    #s.seqnos -> get_seqno;
                    #s.streams -> get_stream;
                    #s.ranks -> get_rank;
                    #s.awaiting_rel -> get_awaiting_rel
                end, Key}
        ]
    ).

%% @doc Proper generator that returns random pmap get requests
del_req(S) ->
    ?LET(
        {Idx, SessId, Key},
        ?SUCHTHAT({R, _, _}, pmap_type_and_key(S), R =/= #s.seqnos),
        [
            SessId,
            {Idx,
                case Idx of
                    #s.subscriptions -> del_subscription;
                    #s.subscription_states -> del_subscription_state;
                    #s.streams -> del_stream;
                    #s.ranks -> del_rank;
                    #s.awaiting_rel -> del_awaiting_rel
                end, Key}
        ]
    ).

session_id_gen() ->
    frequency([
        {5, clientid()},
        {1, <<"a/">>},
        {1, <<"a/b">>},
        {1, <<"a/+">>},
        {1, <<"a/+/#">>},
        {1, <<"#">>},
        {1, <<"+">>},
        {1, <<"/">>}
    ]).

clientid() ->
    %% empty string is not valid...
    ?SUCHTHAT(ClientId, emqx_proper_types:clientid(), ClientId =/= <<>>).

session_command(SessionId, S) ->
    #gs{ds_broken = DSBroken, sessions = Sessions} = S,
    #{SessionId := #s{checkpointing = Checkpointing}} = Sessions,
    Common =
        case DSBroken of
            false ->
                [
                    {1, {call, ?MODULE, create_new, [session_id()]}},
                    {3, {call, ?MODULE, break_ds, []}},
                    %% NOTE: during normal operation, the session
                    %% always uses async commit, which tolerates
                    %% recoverable errors. But if **sync** commit runs
                    %% out of retries, the current behavior is to
                    %% crash the session. We don't test that here.
                    {2, {call, ?MODULE, commit, [SessionId]}}
                ];
            true ->
                [{5, {call, ?MODULE, fix_ds, []}}]
        end,
    frequency(
        [
            %% Metadata:
            {3, {call, ?MODULE, put_metadata, [SessionId, put_metadata()]}},
            {3, {call, ?MODULE, get_metadata, [SessionId, get_metadata()]}},

            %% Key-value:
            {10, {call, ?MODULE, gen_put, [SessionId, put_req()]}},
            {3, {call, ?MODULE, gen_get, get_req(S)}},
            {5, {call, ?MODULE, gen_del, del_req(S)}},

            %% Async checkpoint:
            {5,
                case Checkpointing of
                    false ->
                        {call, ?MODULE, begin_checkpoint, [session_id(S)]};
                    true ->
                        {call, ?MODULE, end_checkpoint, [session_id(S)]}
                end}
            %% Commands that should not happen with a pending checkpoint or when DS is broken
            | case Checkpointing orelse DSBroken of
                false ->
                    [
                        {1, {call, ?MODULE, delete, [SessionId]}},
                        {5, {call, ?MODULE, graceful_takeover, [SessionId]}}
                        | Common
                    ];
                true ->
                    Common
            end
        ]
    ).

command(S = #gs{sessions = Sessions}) ->
    case maps:size(Sessions) > 0 of
        true ->
            ?LET(SessionId, session_id(S), session_command(SessionId, S));
        false ->
            frequency([
                {1, {call, ?MODULE, create_new, [session_id()]}}
            ])
    end.

sample(Size) ->
    proper_gen:pick(commands(?MODULE), Size).

precondition(_, _) ->
    true.

postcondition(S, {call, ?MODULE, get_metadata, [SessionId, {MetaKey, _Fun}]}, Result) ->
    #{SessionId := #s{metadata = Meta}} = S#gs.sessions,
    ?assertEqual(
        maps:get(MetaKey, Meta, undefined),
        Result,
        #{session_id => SessionId, meta => MetaKey}
    ),
    true;
postcondition(S, {call, ?MODULE, gen_get, [SessionId, {Idx, Fun, Key}]}, Result) ->
    #{SessionId := Record} = S#gs.sessions,
    ?assertEqual(
        maps:get(Key, element(Idx, Record), undefined),
        Result,
        #{session_id => SessionId, key => Key, 'fun' => Fun}
    ),
    true;
postcondition(_S, {call, ?MODULE, graceful_takeover, Args}, Result) ->
    {Saved, Restored} = Result,
    assert_state_eq(
        format_state(Saved),
        format_state(Restored),
        #{msg => inconsistent_state_after_restore, args => Args}
    ),
    true;
postcondition(#gs{ds_broken = false}, {call, ?MODULE, commit, [SessionId | _]}, _Result) ->
    Saved = emqx_persistent_session_ds:print_session(SessionId),
    Memory = get_state(SessionId),
    assert_state_eq(
        format_state(Memory),
        maps:get(s, Saved, undefined),
        committed_state_vs_memory
    ),
    %% Verify that dirty state of all pmaps is cleaned by a successful commit:
    lists:foreach(
        fun(I) ->
            #{I := #pmap{dirty = Dirty}} = Memory,
            ?assertEqual(
                #{},
                Dirty,
                #{
                    field => I,
                    session => SessionId
                }
            )
        end,
        ?sessds_pmaps
    ),
    ct:pal("** Verified state consistency for ~w", [SessionId]),
    true;
postcondition(_, _, _) ->
    true.

next_state(S, _V, {call, ?MODULE, create_new, [SessionId]}) ->
    #gs{sessions = Sessions0} = S,
    Sessions = Sessions0#{SessionId => #s{}},
    S#gs{sessions = Sessions};
next_state(S, _V, {call, ?MODULE, delete, [SessionId]}) ->
    #gs{sessions = Sessions} = S,
    S#gs{sessions = maps:remove(SessionId, Sessions)};
next_state(S, _V, {call, ?MODULE, put_metadata, [SessionId, {Key, _Fun, Val}]}) ->
    update(
        SessionId,
        #s.metadata,
        fun(Map) -> Map#{Key => Val} end,
        S
    );
next_state(S, _V, {call, ?MODULE, gen_put, [SessionId, {Idx, _Fun, Key, Val}]}) ->
    update(
        SessionId,
        Idx,
        fun(Map) -> Map#{Key => Val} end,
        S
    );
next_state(S, _V, {call, ?MODULE, gen_del, [SessionId, {Idx, _Fun, Key}]}) ->
    update(
        SessionId,
        Idx,
        fun(Map) -> maps:remove(Key, Map) end,
        S
    );
next_state(S, _V, {call, ?MODULE, begin_checkpoint, [SessionId | _]}) ->
    update(
        SessionId,
        #s.checkpointing,
        fun(_) -> true end,
        S
    );
next_state(S, _V, {call, ?MODULE, CMD, [SessionId | _]}) when
    CMD =:= end_checkpoint;
    CMD =:= commit
->
    #gs{ds_broken = DSBroken, sessions = Sessions0} = S,
    Sessions = maps:update_with(
        SessionId,
        fun(SS = #s{committed = Committed}) ->
            SS#s{checkpointing = false, committed = Committed orelse (not DSBroken)}
        end,
        Sessions0
    ),
    S#gs{sessions = Sessions};
next_state(S, _V, {call, ?MODULE, break_ds, _}) ->
    S#gs{ds_broken = true};
next_state(S, _V, {call, ?MODULE, fix_ds, _}) ->
    S#gs{ds_broken = false};
next_state(S, _V, {call, ?MODULE, _, _}) ->
    S.

initial_state() ->
    #gs{}.

%%================================================================================
%% Operations
%%================================================================================

create_new(SessionId) ->
    print_cmd("*** ~w(~p)", [?FUNCTION_NAME, SessionId]),
    {ok, S} = emqx_persistent_session_ds_state:commit(
        emqx_persistent_session_ds_state:create_new(SessionId),
        #{lifetime => new, sync => true}
    ),
    put_state(SessionId, S).

delete(SessionId) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    ok = emqx_persistent_session_ds_state:delete(SessionId, '_'),
    ets:delete(?tab, SessionId).

commit(SessionId) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    put_state(
        SessionId,
        do_commit(SessionId, #{lifetime => up, sync => true})
    ).

begin_checkpoint(SessionId) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    put_state(
        SessionId,
        do_commit(SessionId, #{lifetime => up, sync => false})
    ).

end_checkpoint(SessionId) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    #{?checkpoint_ref := Ref} = S0 = get_state(SessionId),
    case is_reference(Ref) of
        true ->
            receive
                ?ds_tx_commit_reply(Ref, Reply) ->
                    {ok, S} = emqx_persistent_session_ds_state:on_commit_reply(Reply, S0),
                    put_state(SessionId, S)
            after 5_000 ->
                error({timeout_waiting_for_checkpoint, S0})
            end;
        false ->
            %% Record wasn't dirty, checkpoint wasn't initiated
            ok
    end.

graceful_takeover(SessionId) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    PrevS = do_commit(SessionId, #{lifetime => terminate, sync => true}),
    {ok, S} = do_takeover(SessionId),
    _ = put_state(SessionId, S),
    %% Return a tuple containing previous and new state:
    {PrevS, S}.

put_metadata(SessionId, {MetaKey, Fun, Value}) ->
    print_cmd("*** ~p(~p, ~w, ~w)", [?FUNCTION_NAME, SessionId, MetaKey, Value]),
    S = apply(emqx_persistent_session_ds_state, Fun, [Value, get_state(SessionId)]),
    put_state(SessionId, S).

get_metadata(SessionId, {MetaKey, Fun}) ->
    print_cmd("*** ~p(~p, ~w)", [?FUNCTION_NAME, SessionId, MetaKey]),
    apply(emqx_persistent_session_ds_state, Fun, [get_state(SessionId)]).

gen_put(SessionId, {Idx, Fun, Key, Value}) ->
    print_cmd("*** ~p(~p, ~w, ~w, ~w)", [?FUNCTION_NAME, SessionId, pmap_name(Idx), Key, Value]),
    S = apply(emqx_persistent_session_ds_state, Fun, [Key, Value, get_state(SessionId)]),
    put_state(SessionId, S).

gen_del(SessionId, {Idx, Fun, Key}) ->
    print_cmd("*** ~p(~p, ~w, ~w)", [?FUNCTION_NAME, SessionId, pmap_name(Idx), Key]),
    S = apply(emqx_persistent_session_ds_state, Fun, [Key, get_state(SessionId)]),
    put_state(SessionId, S).

gen_get(SessionId, {Idx, Fun, Key}) ->
    print_cmd("*** ~p(~p, ~w, ~w)", [?FUNCTION_NAME, SessionId, pmap_name(Idx), Key]),
    apply(emqx_persistent_session_ds_state, Fun, [Key, get_state(SessionId)]).

break_ds() ->
    print_cmd("*** ~p()", [?FUNCTION_NAME]),
    #{cbm := Mod} = emqx_dsch:get_db_runtime(?DURABLE_SESSION_STATE_DB),
    meck:new(Mod, [no_history, passthrough]),
    meck:expect(Mod, otx_prepare_tx, fun(_, _, _, _, _) -> ?err_rec(injected) end),
    ok.

fix_ds() ->
    print_cmd("*** ~p()", [?FUNCTION_NAME]),
    #{cbm := Mod} = emqx_dsch:get_db_runtime(?DURABLE_SESSION_STATE_DB),
    meck:unload(Mod),
    ok.

%%================================================================================
%% Misc.
%%================================================================================

do_commit(SessionId, Opts) ->
    S0 = get_state(SessionId),
    ?tp_span(
        test_commit,
        #{id => SessionId, s => S0, opts => Opts},
        begin
            %% Commit previous state normally:
            {ok, S} = emqx_persistent_session_ds_state:commit(S0, Opts),
            S
        end
    ).

do_takeover(SessionId) ->
    ?tp_span(
        test_takeover,
        #{sessid => SessionId},
        maybe
            {ok, S} ?= emqx_persistent_session_ds_state:open(SessionId),
            emqx_persistent_session_ds_state:commit(S, #{lifetime => takeover, sync => true})
        end
    ).

assert_state_eq(Expected, Got, Comment) ->
    ExcludeFields = exclude_fields(),
    case maps:without(ExcludeFields, Expected) =:= maps:without(ExcludeFields, Got) of
        true ->
            ok;
        false ->
            Diff = diff_maps(Expected, Got),
            ct:pal(
                error,
                "*** States are not equal:~n"
                " Expected:~n   ~p~n"
                " Got:~n   ~p~n"
                " Diff:~n   ~p~n"
                " Comment: ~p",
                [Expected, Got, Diff, Comment]
            ),
            error(Comment)
    end.

diff_maps(Expected, Got) ->
    Fields = lists:usort(maps:keys(Expected) ++ maps:keys(Got)),
    lists:foldl(
        fun(Key, Acc) ->
            case {Expected, Got} of
                {#{Key := V}, #{Key := V}} ->
                    Acc;
                {#{Key := Ve}, #{Key := Vg}} ->
                    Acc#{
                        Key =>
                            #{
                                expected => Ve,
                                got => Vg
                            }
                    };
                {#{Key := Ve}, _} ->
                    #{expected => Ve};
                {_, #{Key := Vg}} ->
                    #{got => Vg}
            end
        end,
        #{},
        Fields
    ).

exclude_fields() ->
    [?collection_dirty, ?collection_guard, checkpoint_n, checkpoint_ref].

update(SessionId, Key, Fun, S = #gs{sessions = Sessions0}) ->
    Sessions = maps:update_with(
        SessionId,
        fun(SS) ->
            setelement(Key, SS, Fun(erlang:element(Key, SS)))
        end,
        Sessions0
    ),
    S#gs{sessions = Sessions}.

get_state(SessionId) ->
    case ets:lookup(?tab, SessionId) of
        [{_, S}] ->
            S;
        [] ->
            error({not_found, SessionId})
    end.

put_state(SessionId, S) ->
    Prev = ets:lookup(?tab, SessionId),
    true = ets:insert(?tab, {SessionId, S}),
    %% Return previous state:
    case Prev of
        [] ->
            undefined;
        [{_, Rec}] ->
            Rec
    end.

sut_state() ->
    ets:tab2list(?tab).

init(Heir) ->
    _ = ets:new(?tab, [named_table, public, {keypos, 1}, {heir, Heir, sut_state_tab}]),
    ok = emqx_persistent_session_ds_state:open_db().

clean() ->
    ets:delete(?tab),
    try
        fix_ds()
    catch
        _:_ -> ok
    end,
    ok = emqx_ds:drop_db(sessions).

format_state(undefined) ->
    undefined;
format_state(Rec) ->
    emqx_persistent_session_ds_state:format(Rec).

pmap_name(Idx) ->
    lists:nth(Idx - 1, record_info(fields, s)).

print_cmd(Fmt, Args) ->
    Str = lists:flatten(io_lib:format(Fmt, Args)),
    ?tp(Str, #{}),
    ct:pal(Str).
