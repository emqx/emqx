%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_persistent_session_ds_state_tests).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(tab, ?MODULE).
-define(DB, emqx_persistent_session).

%% Note: here `committed' != `dirty'. It means "has been committed at
%% least once since the creation", and it's used by the iteration
%% test.
-record(s, {subs = #{}, metadata = #{}, streams = #{}, seqno = #{}, committed = false}).

-type state() :: #{emqx_persistent_session_ds:id() => #s{}}.

-define(metadata_domain, metadata).
-define(metadata_domain_bin, <<"metadata">>).
-define(subscription_domain, subscription).
-define(subscription_state_domain, subscription_state).
-define(stream_domain, stream).
-define(rank_domain, rank).
-define(seqno_domain, seqno).
-define(awaiting_rel_domain, awaiting_rel).

%%================================================================================
%% Properties
%%================================================================================

seqno_proper_test_() ->
    Props = [prop_consistency()],
    Opts = [{numtests, 10}, {to_file, user}, {max_size, 100}],
    {timeout, 300, [?_assert(proper:quickcheck(Prop, Opts)) || Prop <- Props]}.

prop_consistency() ->
    ?FORALL(
        Cmds,
        commands(?MODULE),
        begin
            init(),
            {_History, State, Result} = run_commands(?MODULE, Cmds),
            clean(),
            ?WHENFAIL(
                io:format(
                    user,
                    "Operations: ~p~nState: ~p\nResult: ~p~n",
                    [Cmds, State, Result]
                ),
                aggregate(command_names(Cmds), Result =:= ok)
            )
        end
    ).

-ifdef(STORE_STATE_IN_DS).
%% Verifies that our internal keys generated for stream keys preserve the order relation
%% between them.
stream_order_internal_keys_proper_test_() ->
    Props = [prop_stream_order_internal_keys()],
    Opts = [{numtests, 100}, {to_file, user}, {max_size, 100}],
    {timeout, 300, [?_assert(proper:quickcheck(Prop, Opts)) || Prop <- Props]}.

prop_stream_order_internal_keys() ->
    ?FORALL(
        {Id, Streams0},
        {session_id(), list({non_neg_integer(), value_gen(), stream_state()})},
        try
            init(),
            Streams = lists:uniq(Streams0),
            StreamKeys = [{R, S} || {R, S, _SS} <- Streams],
            ExpectedRanks = lists:sort([R || {R, _S, _SS} <- Streams]),
            S = lists:foldl(
                fun({R, S, SS}, Acc) ->
                    emqx_persistent_session_ds_state:put_stream({R, S}, SS, Acc)
                end,
                emqx_persistent_session_ds_state:create_new(Id),
                Streams
            ),
            RevRanks = emqx_persistent_session_ds_state:fold_streams(
                fun({R, _S}, _SS, Acc) -> [R | Acc] end,
                [],
                S
            ),
            Ranks = lists:reverse(RevRanks),
            ?WHENFAIL(
                io:format(
                    user,
                    "Expected ranks:\n  ~p\nRanks:\n  ~p\nStream keys:\n  ~p\n",
                    [ExpectedRanks, Ranks, StreamKeys]
                ),
                ExpectedRanks =:= Ranks
            )
        after
            clean()
        end
    ).
%% -ifdef(STORE_STATE_IN_DS).
-endif.

%%================================================================================
%% Generators
%%================================================================================

-define(n_sessions, 10).

session_id() ->
    oneof([integer_to_binary(I) || I <- lists:seq(1, ?n_sessions)]).

topic() ->
    oneof([<<"foo">>, <<"bar">>, <<"foo/#">>, <<"//+/#">>]).

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
        )
    ]).

get_metadata() ->
    oneof([
        {last_alive_at, get_last_alive_at},
        {created_at, get_created_at}
    ]).

seqno_track() ->
    range(0, 1).

seqno() ->
    range(1, 100).

-ifdef(STORE_STATE_IN_DS).
stream_id() ->
    {range(1, 3), oneof([#{}, {}])}.
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
stream_id() ->
    %% Note: this does not match the stream id type used in practice, which is a
    %% `{emqx_persistent_session_ds:subscription_id(), emqx_ds:stream()}'
    range(1, 1).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

stream_state() ->
    oneof([#{}]).

put_req() ->
    oneof([
        ?LET(
            {Id, Stream},
            {stream_id(), stream_state()},
            {#s.streams, put_stream, Id, Stream}
        ),
        ?LET(
            {Track, Seqno},
            {seqno_track(), seqno()},
            {#s.seqno, put_seqno, Track, Seqno}
        ),
        ?LET(
            {Topic, Subscription},
            {topic(), subscription()},
            {#s.subs, put_subscription, Topic, Subscription}
        )
    ]).

get_req() ->
    oneof([
        {#s.streams, get_stream, stream_id()},
        {#s.seqno, get_seqno, seqno_track()},
        {#s.subs, get_subscription, topic()}
    ]).

del_req() ->
    oneof([
        {#s.streams, del_stream, stream_id()},
        {#s.subs, del_subscription, topic()}
    ]).

value_gen() ->
    oneof([#{}, loose_tuple(oneof([range(1, 3), binary()]))]).

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

domain_gen() ->
    oneof([
        ?metadata_domain,
        ?subscription_domain,
        ?subscription_state_domain,
        ?stream_domain,
        ?rank_domain,
        ?seqno_domain,
        ?awaiting_rel_domain
    ]).

key_gen(?metadata_domain) ->
    <<"metadata">>;
key_gen(?stream_domain) ->
    ?LET(
        {Rank, X},
        {integer(), integer()},
        <<Rank:64, X:64>>
    );
key_gen(_) ->
    integer().

command(S) ->
    case maps:size(S) > 0 of
        true ->
            frequency([
                %% Global CRUD operations:
                {1, {call, ?MODULE, create_new, [session_id()]}},
                {1, {call, ?MODULE, delete, [session_id(S)]}},
                {2, {call, ?MODULE, reopen, [session_id(S)]}},
                {2, {call, ?MODULE, commit, [session_id(S)]}},

                %% Metadata:
                {3, {call, ?MODULE, put_metadata, [session_id(S), put_metadata()]}},
                {3, {call, ?MODULE, get_metadata, [session_id(S), get_metadata()]}},

                %% Key-value:
                {3, {call, ?MODULE, gen_put, [session_id(S), put_req()]}},
                {3, {call, ?MODULE, gen_get, [session_id(S), get_req()]}},
                {3, {call, ?MODULE, gen_del, [session_id(S), del_req()]}},

                %% Getters:
                {1, {call, ?MODULE, iterate_sessions, [batch_size()]}}
            ]);
        false ->
            frequency([
                {1, {call, ?MODULE, create_new, [session_id()]}},
                {1, {call, ?MODULE, iterate_sessions, [batch_size()]}}
            ])
    end.

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
    put_state(SessionId, emqx_persistent_session_ds_state:create_new(SessionId)).

delete(SessionId) ->
    emqx_persistent_session_ds_state:delete(SessionId),
    ets:delete(?tab, SessionId).

commit(SessionId) ->
    put_state(SessionId, emqx_persistent_session_ds_state:commit(get_state(SessionId))).

reopen(SessionId) ->
    _ = emqx_persistent_session_ds_state:commit(get_state(SessionId)),
    {ok, S} = emqx_persistent_session_ds_state:open(SessionId),
    put_state(SessionId, S).

put_metadata(SessionId, {_MetaKey, Fun, Value}) ->
    S = apply(emqx_persistent_session_ds_state, Fun, [Value, get_state(SessionId)]),
    put_state(SessionId, S).

get_metadata(SessionId, {_MetaKey, Fun}) ->
    apply(emqx_persistent_session_ds_state, Fun, [get_state(SessionId)]).

gen_put(SessionId, {_Idx, Fun, Key, Value}) ->
    S = apply(emqx_persistent_session_ds_state, Fun, [Key, Value, get_state(SessionId)]),
    put_state(SessionId, S).

gen_del(SessionId, {_Idx, Fun, Key}) ->
    S = apply(emqx_persistent_session_ds_state, Fun, [Key, get_state(SessionId)]),
    put_state(SessionId, S).

gen_get(SessionId, {_Idx, Fun, Key}) ->
    apply(emqx_persistent_session_ds_state, Fun, [Key, get_state(SessionId)]).

iterate_sessions(BatchSize) ->
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
    ets:insert(?tab, {SessionId, S}).

-ifdef(STORE_STATE_IN_DS).
init() ->
    _ = ets:new(?tab, [named_table, public, {keypos, 1}]),
    mria:start(),
    {ok, _} = application:ensure_all_started(emqx_ds_backends),
    Dir = binary_to_list(filename:join(["/tmp", emqx_guid:to_hexstr(emqx_guid:gen())])),
    persistent_term:put({?MODULE, data_dir}, Dir),
    application:set_env(emqx_durable_storage, db_data_dir, Dir),
    Defaults = #{
        backend => builtin_local,
        append_only => false,
        atomic_batches => true,
        storage =>
            {emqx_ds_storage_bitfield_lts, #{
                topic_index_bytes => 4,
                epoch_bits => 10,
                bits_per_topic_level => 64
            }},
        n_shards => 16,
        n_sites => 1,
        replication_factor => 3,
        replication_options => #{}
    },
    ok = emqx_persistent_session_ds_state:open_db(Defaults),
    ok.
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
init() ->
    _ = ets:new(?tab, [named_table, public, {keypos, 1}]),
    mria:start(),
    emqx_persistent_session_ds_state:create_tables().
%% END ifdef(STORE_STATE_IN_DS).
-endif.

-ifdef(STORE_STATE_IN_DS).
clean() ->
    ets:delete(?tab),
    emqx_ds:drop_db(?DB),
    application:stop(emqx_ds_backends),
    application:stop(emqx_ds_builtin_local),
    mria:stop(),
    mria_mnesia:delete_schema(),
    Dir = persistent_term:get({?MODULE, data_dir}),
    persistent_term:erase({?MODULE, data_dir}),
    ok = file:del_dir_r(Dir),
    ok.
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
clean() ->
    ets:delete(?tab),
    mria:stop(),
    mria_mnesia:delete_schema().
%% END ifdef(STORE_STATE_IN_DS).
-endif.
