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

%% @doc A property-based testcase that verifies durable session state
%% serialization and deserialization consistency.
-module(emqx_persistent_session_ds_state_fuzzer).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/src/emqx_persistent_session_ds/session_internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(tab, ?MODULE).

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
    committed = false
}).

-type state() :: #{emqx_persistent_session_ds:id() => #s{}}.

%%================================================================================
%% Properties
%%================================================================================

%%================================================================================
%% Generators
%%================================================================================

-define(n_sessions, 3).

shard_gen() ->
    oneof([<<"0">>, <<"1">>, <<"100">>]).

session_id() ->
    oneof([integer_to_binary(I) || I <- lists:seq(1, ?n_sessions)]).

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

stream_id() ->
    {range(1, 3), oneof([#{}, {}])}.

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
    %% FIXME: nondeterministic hack, however some code _DOES_ require
    %% IDs to be unique.
    exactly(erlang:unique_integer()).

%% Proper generator for subscription states.
sub_state_gen() ->
    SubOpts =
        ?LET(
            {NL, QoS, RaP, SubId},
            {boolean(), range(?QOS_0, ?QOS_2), boolean(), integer()},
            #{nl => NL, qos => QoS, rap => RaP, subid => SubId}
        ),
    oneof(
        [
            %% Without optional fields:
            ?LET(
                {Par, UQ, SO},
                {uniq_id(), boolean(), SubOpts},
                #{
                    parent_subscription => Par,
                    upgrade_qos => UQ,
                    subopts => SO
                }
            ),
            %% With "superseded by" field:
            ?LET(
                {Par, UQ, SO, SB},
                {uniq_id(), boolean(), SubOpts, uniq_id()},
                #{
                    parent_subscription => Par,
                    upgrade_qos => UQ,
                    subopts => SO,
                    superseded_by => SB
                }
            ),
            %% With share topic filter:
            ?LET(
                {Par, UQ, SO, STF},
                {uniq_id(), boolean(), SubOpts, share_tf()},
                #{
                    parent_subscription => Par,
                    upgrade_qos => UQ,
                    subopts => SO,
                    share_topic_filter => STF
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

seqno_type_gen() ->
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
            {seqno_type_gen(), non_neg_integer()},
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
        )
    ]).

%% @doc A generic proper generator that randomly selects an existing
%% session, one of its pmaps, and a key that may or may not exist in
%% the pmap. Returns index of the pmap in #s, session id and the pmap
%% key.
pmap_type_and_key(S) ->
    Recs = [#s.subscriptions, #s.subscription_states, #s.seqnos, #s.streams, #s.ranks],
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
                    #s.ranks -> get_rank
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
                    #s.ranks -> del_rank
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
                {5, {call, ?MODULE, reopen, [session_id(S)]}},
                {2, {call, ?MODULE, commit, [session_id(S)]}},

                %% Metadata:
                {3, {call, ?MODULE, put_metadata, [session_id(S), put_metadata()]}},
                {3, {call, ?MODULE, get_metadata, [session_id(S), get_metadata()]}},

                %% Key-value:
                {3, {call, ?MODULE, gen_put, [session_id(S), put_req()]}},
                {1, {call, ?MODULE, gen_get, get_req(S)}},
                {2, {call, ?MODULE, gen_del, del_req(S)}},

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
postcondition(_S, {call, ?MODULE, reopen, Args}, Result) ->
    {Saved, Restored} = Result,
    ?assertEqual(
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
    ct:pal("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    S = emqx_persistent_session_ds_state:create_new(SessionId),
    put_state(
        SessionId,
        emqx_persistent_session_ds_state:commit(S, #{lifetime => new})
    ).

delete(SessionId) ->
    ct:pal("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    emqx_persistent_session_ds_state:delete(SessionId),
    ets:delete(?tab, SessionId).

commit(SessionId) ->
    ct:pal("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    put_state(
        SessionId,
        emqx_persistent_session_ds_state:commit(get_state(SessionId))
    ).

reopen(SessionId) ->
    ct:pal("*** ~p(~p)", [?FUNCTION_NAME, SessionId]),
    _ = emqx_persistent_session_ds_state:commit(
        get_state(SessionId),
        #{lifetime => takeover}
    ),
    {ok, S} = emqx_persistent_session_ds_state:open(SessionId),
    %% Return a tuple containing previous and new state:
    {put_state(SessionId, S), S}.

put_metadata(SessionId, {MetaKey, Fun, Value}) ->
    ct:pal("*** ~p(~p, ~p, ~p)", [?FUNCTION_NAME, SessionId, MetaKey, Value]),
    S = apply(emqx_persistent_session_ds_state, Fun, [Value, get_state(SessionId)]),
    put_state(SessionId, S).

get_metadata(SessionId, {MetaKey, Fun}) ->
    ct:pal("*** ~p(~p, ~p)", [?FUNCTION_NAME, SessionId, MetaKey]),
    apply(emqx_persistent_session_ds_state, Fun, [get_state(SessionId)]).

gen_put(SessionId, {Idx, Fun, Key, Value}) ->
    ct:pal("*** ~p(~p, ~p, ~p, ~p)", [?FUNCTION_NAME, SessionId, Idx, Key, Value]),
    S = apply(emqx_persistent_session_ds_state, Fun, [Key, Value, get_state(SessionId)]),
    put_state(SessionId, S).

gen_del(SessionId, {Idx, Fun, Key}) ->
    ct:pal("*** ~p(~p, ~p, ~p)", [?FUNCTION_NAME, SessionId, Idx, Key]),
    S = apply(emqx_persistent_session_ds_state, Fun, [Key, get_state(SessionId)]),
    put_state(SessionId, S).

gen_get(SessionId, {Idx, Fun, Key}) ->
    ct:pal("*** ~p(~p, ~p, ~p)", [?FUNCTION_NAME, SessionId, Idx, Key]),
    apply(emqx_persistent_session_ds_state, Fun, [Key, get_state(SessionId)]).

iterate_sessions(BatchSize) ->
    ct:pal("*** ~p(~p)", [?FUNCTION_NAME, BatchSize]),
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
    ok = emqx_persistent_session_ds_state:open_db(#{n_sites => 1, n_shards => 1}).

clean() ->
    ets:delete(?tab),
    ok = emqx_ds:drop_db(sessions).

format_state(undefined) ->
    undefined;
format_state(Rec) ->
    emqx_persistent_session_ds_state:format(Rec).
