%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc A property-based testcase that verifies durable session state
%% serialization and deserialization consistency.
-module(emqx_persistent_session_ds_state_fuzzer).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/src/emqx_persistent_session_ds/session_internals.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

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
    committed = false
}).

%% Model state:
-type state() :: #{emqx_persistent_session_ds:id() => #s{}}.

%%================================================================================
%% Properties
%%================================================================================

%%================================================================================
%% Generators
%%================================================================================

-define(n_sessions, 10).

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

session_id(S) ->
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
pmap_type_and_key(S) ->
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

command(S) ->
    case maps:size(S) > 0 of
        true ->
            frequency([
                %% Global CRUD operations:
                {1, {call, ?MODULE, create_new, [session_id()]}},
                {1, {call, ?MODULE, delete, [session_id(S)]}},
                {5, {call, ?MODULE, graceful_takeover, [session_id(S)]}},
                {2, {call, ?MODULE, commit, [session_id(S)]}},
                {2, {call, ?MODULE, async_checkpoint, [session_id(S)]}},

                %% Metadata:
                {3, {call, ?MODULE, put_metadata, [session_id(S), put_metadata()]}},
                {3, {call, ?MODULE, get_metadata, [session_id(S), get_metadata()]}},

                %% Key-value:
                {3, {call, ?MODULE, gen_put, [session_id(S), put_req()]}},
                {5, {call, ?MODULE, gen_get, get_req(S)}},
                {3, {call, ?MODULE, gen_del, del_req(S)}},

                %% Getters:
                %% FIXME:
                {0, {call, ?MODULE, iterate_sessions, [batch_size()]}}
            ]);
        false ->
            frequency([
                {1, {call, ?MODULE, create_new, [session_id()]}},
                %% FIXME:
                {0, {call, ?MODULE, iterate_sessions, [batch_size()]}}
            ])
    end.

sample(Size) ->
    proper_gen:pick(commands(?MODULE), Size).

precondition(_, _) ->
    true.

postcondition(S, {call, ?MODULE, iterate_sessions, [_]}, Result) ->
    {Sessions, _} = lists:unzip(Result),
    %% No lingering sessions:
    ?assertMatch([], Sessions -- maps:keys(S)),
    %% All committed sessions are visited by the iterator:
    CommittedSessions = lists:sort([K || {K, #s{committed = true}} <- maps:to_list(S)]),
    ?assertMatch([], CommittedSessions -- Sessions),
    true;
postcondition(S, {call, ?MODULE, get_metadata, [SessionId, {MetaKey, _Fun}]}, Result) ->
    #{SessionId := #s{metadata = Meta}} = S,
    ?assertEqual(
        maps:get(MetaKey, Meta, undefined),
        Result,
        #{session_id => SessionId, meta => MetaKey}
    ),
    true;
postcondition(S, {call, ?MODULE, gen_get, [SessionId, {Idx, Fun, Key}]}, Result) ->
    #{SessionId := Record} = S,
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
postcondition(_, _, _) ->
    true.

next_state(S, _V, {call, ?MODULE, create_new, [SessionId]}) ->
    S#{SessionId => #s{}};
next_state(S, _V, {call, ?MODULE, delete, [SessionId]}) ->
    maps:remove(SessionId, S);
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
next_state(S, _V, {call, ?MODULE, commit, [SessionId]}) ->
    update(
        SessionId,
        #s.committed,
        fun(_) -> true end,
        S
    );
next_state(S, _V, {call, ?MODULE, _, _}) ->
    S.

initial_state() ->
    #{}.

%%================================================================================
%% Operations
%%================================================================================

create_new(SessionId) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    S = emqx_persistent_session_ds_state:commit(
        emqx_persistent_session_ds_state:create_new(SessionId),
        #{lifetime => new, sync => true}
    ),
    put_state(SessionId, S).

delete(SessionId) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    emqx_persistent_session_ds_state:delete(SessionId),
    ets:delete(?tab, SessionId).

commit(SessionId) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    put_state(
        SessionId,
        do_commit(SessionId, #{lifetime => up, sync => true})
    ).

async_checkpoint(SessionId) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    put_state(
        SessionId,
        do_commit(SessionId, #{lifetime => up, sync => false})
    ).

graceful_takeover(SessionId) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    PrevS = do_commit(SessionId, #{lifetime => terminate, sync => true}),
    {ok, S} = do_takeover(SessionId),
    _ = put_state(SessionId, S),
    %% Return a tuple containing previous and new state:
    {PrevS, S}.

put_metadata(SessionId, {MetaKey, Fun, Value}) ->
    print_cmd("*** ~p(~p, ~p, ~p)", [?FUNCTION_NAME, SessionId, MetaKey, Value]),
    S = apply(emqx_persistent_session_ds_state, Fun, [Value, get_state(SessionId)]),
    put_state(SessionId, S).

get_metadata(SessionId, {MetaKey, Fun}) ->
    print_cmd("*** ~p(~p, ~p)", [?FUNCTION_NAME, SessionId, MetaKey]),
    apply(emqx_persistent_session_ds_state, Fun, [get_state(SessionId)]).

gen_put(SessionId, {Idx, Fun, Key, Value}) ->
    print_cmd("*** ~p(~p, ~p, ~p, ~p)", [?FUNCTION_NAME, SessionId, pmap_name(Idx), Key, Value]),
    S = apply(emqx_persistent_session_ds_state, Fun, [Key, Value, get_state(SessionId)]),
    put_state(SessionId, S).

gen_del(SessionId, {Idx, Fun, Key}) ->
    print_cmd("*** ~p(~p, ~p, ~p)", [?FUNCTION_NAME, SessionId, pmap_name(Idx), Key]),
    S = apply(emqx_persistent_session_ds_state, Fun, [Key, get_state(SessionId)]),
    put_state(SessionId, S).

gen_get(SessionId, {Idx, Fun, Key}) ->
    print_cmd("*** ~p(~p, ~p, ~p)", [?FUNCTION_NAME, SessionId, pmap_name(Idx), Key]),
    apply(emqx_persistent_session_ds_state, Fun, [Key, get_state(SessionId)]).

iterate_sessions(BatchSize) ->
    print_cmd("*** ~p(~p)", [?FUNCTION_NAME, BatchSize]),
    Fun = fun F(It0) ->
        case emqx_persistent_session_ds_state:session_iterator_next(It0, BatchSize) of
            {[], _} ->
                [];
            {Sessions, It} ->
                Sessions ++ F(It)
        end
    end,
    Fun(emqx_persistent_session_ds_state:make_session_iterator()).

%%================================================================================
%% Misc.
%%================================================================================

do_commit(SessionId, Opts) ->
    S = get_state(SessionId),
    ?tp_span(
        test_commit,
        #{id => SessionId, s => S, opts => Opts},
        %% Commit previous state normally:
        emqx_persistent_session_ds_state:commit(S, Opts)
    ).

do_takeover(SessionId) ->
    ?tp_span(
        test_takeover,
        #{sessid => SessionId},
        maybe
            {ok, S} ?= emqx_persistent_session_ds_state:open(SessionId),
            {ok, emqx_persistent_session_ds_state:commit(S, #{lifetime => takeover, sync => true})}
        end
    ).

assert_state_eq(Expected, Got, Comment) ->
    ExcludeFields = [dirty, guard, checkpoint_ref],
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

update(SessionId, Key, Fun, S) ->
    maps:update_with(
        SessionId,
        fun(SS) ->
            setelement(Key, SS, Fun(erlang:element(Key, SS)))
        end,
        S
    ).

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
