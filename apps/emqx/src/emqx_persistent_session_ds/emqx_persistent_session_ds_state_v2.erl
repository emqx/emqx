%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module implements second version of durable encoding of
%% the session state.
%%
%% Data layout in DS DB. Note: /foo denotes individual stream, |bar
%% means wildcard stream:
%%
%% g|<sessid>                       -- Session guard
%%
%% d|<sessid>/meta|createdat        -- PMaps
%%                |last_alive_at
%%                |...
%%           /subscriptions|1
%%                         |12
%%                         |...
%%           /...|...
%%
%% o|<sessid>                       -- Offline data, aka misc. stuff
%%                                     for the dashboard that we don't
%%                                     in the state
%%
-module(emqx_persistent_session_ds_state_v2).

-feature(maybe_expr, enable).

%% API:
-export([
    open/3,
    delete/2,
    commit/3,

    set_offline_info/3,

    lts_threshold_cb/2,
    pmap_topic/4
]).

-export_type([]).

-include_lib("emqx/src/emqx_tracepoints.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("pmap.hrl").
-include("session_internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(DB, ?DURABLE_SESSION_STATE).

%% Topic roots:
-define(top_data, <<"d">>).
-define(top_guard, <<"g">>).
-define(top_offline_info, <<"o">>).
%% Pmap topics:
-define(top_meta, <<"mta">>).
-define(top_sub, <<"sub">>).
-define(top_sstate, <<"sst">>).
-define(top_seqno, <<"sn">>).
-define(top_stream, <<"st">>).
-define(top_rank, <<"rnk">>).
-define(top_awaiting_rel, <<"arl">>).

%%================================================================================
%% API functions
%%================================================================================

%% @doc Open a session
%% TODO: combine opening with updating metadata to make it a one transaction
-spec open(emqx_ds:generation(), emqx_types:clientid(), boolean()) ->
    {ok, emqx_persistent_session_ds_state:t()} | undefined.
open(Generation, ClientId, Verify) ->
    {ok, _TXSerial, Result} = emqx_ds:trans(
        #{db => ?DB, generation => Generation, owner => ClientId},
        fun() -> open_tx(Generation, ClientId, Verify) end
    ),
    Result.

-spec commit(
    emqx_ds:generation(),
    emqx_persistent_session_ds_state:t(),
    emqx_persistent_session_ds_state:commit_opts()
) ->
    emqx_persistent_session_ds_state:t().
commit(_Generation, Rec = #{?dirty := false}, #{lifetime := L}) when L =/= takeover, L =/= new ->
    Rec;
commit(
    Generation,
    Rec0 = #{
        ?id := ClientId,
        ?guard := Guard0,
        ?metadata := Metadata,
        ?subscriptions := Subs,
        ?subscription_states := SubStates,
        ?streams := Streams,
        ?seqnos := SeqNos,
        ?ranks := Ranks,
        ?awaiting_rel := AwaitingRels
    },
    #{lifetime := Lifetime}
) ->
    NewGuard = Lifetime =:= takeover orelse Lifetime =:= new,
    Result =
        emqx_ds:trans(
            #{db => ?DB, owner => ClientId, generation => Generation},
            fun() ->
                %% Generate a new guard if needed:
                NewGuard andalso
                    write_guard(ClientId, ?ds_tx_serial),
                %% Ensure continuity of the session:
                assert_guard(ClientId, Guard0),
                Rec0#{
                    ?metadata := pmap_commit(ClientId, Metadata),
                    ?subscriptions := pmap_commit(ClientId, Subs),
                    ?subscription_states := pmap_commit(ClientId, SubStates),
                    ?streams := pmap_commit(ClientId, Streams),
                    ?seqnos := pmap_commit(ClientId, SeqNos),
                    ?ranks := pmap_commit(ClientId, Ranks),
                    ?awaiting_rel := pmap_commit(ClientId, AwaitingRels),
                    ?unset_dirty
                }
            end
        ),
    case Result of
        {ok, TXSerial, Rec} when NewGuard ->
            %% This is a new incarnation of the client. Update the
            %% guard:
            Rec#{?guard := TXSerial};
        {ok, _, Rec} ->
            Rec;
        {error, unrecoverable, {precondition_failed, Conflict}} when
            Lifetime =:= terminate
        ->
            %% Don't interrupt graceful channel shut down even when
            %% guard is invalidated:
            ?tp(warning, ?sessds_takeover_conflict, #{id => ClientId, conflict => Conflict}),
            Rec0;
        {error, Class, Reason} ->
            error(
                {failed_to_commit_session, #{
                    id => ClientId,
                    lifetime => Lifetime,
                    Class => Reason
                }}
            )
    end.

-spec set_offline_info(
    emqx_ds:generation(),
    emqx_types:client_id(),
    term()
) ->
    ok.
set_offline_info(Generation, ClientId, Data) ->
    emqx_ds:trans(
        #{db => ?DB, owner => ClientId, generation => Generation},
        fun() ->
            emqx_ds:tx_blob_write([?top_offline_info, ClientId], term_to_binary(Data))
        end
    ).

-spec delete(emqx_ds:generation(), emqx_persistent_session_ds_state:t()) -> ok.
delete(
    Generation,
    #{
        ?id := ClientId,
        ?guard := Guard,
        ?metadata := Metadata,
        ?subscriptions := Subs,
        ?subscription_states := SubStates,
        ?streams := Streams,
        ?seqnos := SeqNos,
        ?ranks := Ranks,
        ?awaiting_rel := AwaitingRels
    }
) ->
    {ok, _, _} =
        emqx_ds:trans(
            #{db => ?DB, owner => ClientId, generation => Generation},
            fun() ->
                pmap_delete(ClientId, Metadata),
                pmap_delete(ClientId, Subs),
                pmap_delete(ClientId, SubStates),
                pmap_delete(ClientId, Streams),
                pmap_delete(ClientId, SeqNos),
                pmap_delete(ClientId, Ranks),
                pmap_delete(ClientId, AwaitingRels),
                emqx_ds:tx_del_topic([?top_offline_info, ClientId]),

                assert_guard(ClientId, Guard),
                delete_guard(ClientId)
            end
        ),
    ok.

%% @doc LTS trie wildcard threshold function
lts_threshold_cb(0, _Parent) ->
    %% Don't create a common stream for topics adjacent to the root:
    infinity;
lts_threshold_cb(1, ?top_guard) ->
    %% Always create a unified stream for session guards, since
    %% iteration over guards is used to enumerate sessions:
    0;
lts_threshold_cb(1, ?top_data) ->
    %% Create a unified stream for pmaps' roots. Even though we never
    %% iterate over pmaps that belong to different sessions, all LTS
    %% nodes are kept in RAM at all time. So if we don't unify the
    %% sessions' data, we'll end up with a number of objects per
    %% session, dead or alive, stuck in RAM.
    0;
lts_threshold_cb(3, Parent) when
    Parent =:= ?top_meta;
    Parent =:= ?top_sub;
    Parent =:= ?top_sstate;
    Parent =:= ?top_seqno;
    Parent =:= ?top_stream;
    Parent =:= ?top_rank;
    Parent =:= ?top_awaiting_rel
->
    %% Always create a unified stream for data that belongs to a certain group:
    %% E.g. `d/<sessionid>/stm/<stream_key>'
    0;
lts_threshold_cb(_, _) ->
    infinity.

%%================================================================================
%% Internal functions
%%================================================================================

open_tx(Generation, ClientId, Verify) ->
    Shard = emqx_ds:shard_of(?DB, ClientId),
    case guard(ClientId, Shard, Generation) of
        undefined ->
            undefined;
        Guard ->
            Ret = #{
                ?id => ClientId,
                ?guard => Guard,
                ?dirty => false,
                ?metadata => pmap_restore(?metadata, Shard, ClientId),
                ?subscriptions => pmap_restore(?subscriptions, Shard, ClientId),
                ?subscription_states => pmap_restore(?subscription_states, Shard, ClientId),
                ?seqnos => pmap_restore(?seqnos, Shard, ClientId),
                ?streams => pmap_restore(?streams, Shard, ClientId),
                ?ranks => pmap_restore(?ranks, Shard, ClientId),
                ?awaiting_rel => pmap_restore(?awaiting_rel, Shard, ClientId)
            },
            case Verify of
                false ->
                    {ok, Ret};
                true ->
                    %% Verify that guard hasn't changed while we were
                    %% scanning the pmaps:
                    assert_guard(ClientId, Guard),
                    {ok, Ret}
            end
    end.

%% == Operations with the guard ==

%% @doc Read the guard
-spec guard(emqx_persistent_session_ds:id(), emqx_ds:rank_x(), emqx_ds:generation()) ->
    binary() | undefined.
guard(ClientId, Shard, Generation) ->
    case
        emqx_ds:dirty_read(?DB, [?top_guard, ClientId], #{shard => Shard, generation => Generation})
    of
        [{_Topic, Guard}] ->
            Guard;
        [] ->
            undefined
    end.

-spec assert_guard(
    emqx_persistent_session_ds:id(),
    emqx_persistent_session_ds_state:guard() | undefined
) -> ok.
assert_guard(ClientId, undefined) ->
    emqx_ds:tx_blob_assert_not([?top_guard, ClientId]);
assert_guard(ClientId, Guard) when is_binary(Guard) ->
    emqx_ds:tx_blob_assert([?top_guard, ClientId], Guard).

-spec write_guard(emqx_persistent_session_ds:id(), binary() | ?ds_tx_serial) -> ok.
write_guard(ClientId, Guard) ->
    emqx_ds:tx_blob_write([?top_guard, ClientId], Guard).

-spec delete_guard(emqx_persistent_session_ds:id()) -> ok.
delete_guard(ClientId) ->
    emqx_ds:tx_del_topic([?top_guard, ClientId]).

%% == Operations over PMaps ==

-spec pmap_commit(
    emqx_persistent_session_ds:id(),
    emqx_persistent_session_ds_state:pmap(K, V)
) ->
    emqx_persistent_session_ds_state:pmap(K, V).
pmap_commit(
    ClientId, Pmap = #pmap{name = Name, dirty = Dirty, cache = Cache}
) ->
    maps:foreach(
        fun
            (_Key, {del, Topic}) ->
                emqx_ds:tx_del_topic(Topic);
            (Key, dirty) ->
                #{Key := Val} = Cache,
                write_pmap_kv(Name, ClientId, Key, Val)
        end,
        Dirty
    ),
    Pmap#pmap{
        dirty = #{}
    }.

-spec pmap_restore(atom(), emqx_ds:rank_x(), emqx_persistent_session_ds:id()) ->
    emqx_persistent_session_ds_state:pmap(_, _).
pmap_restore(Name, Shard, ClientId) ->
    Cache = emqx_ds:fold_topic(
        fun(_Slab, _Stream, _DSKey, {Topic, Payload}, Acc) ->
            KeyBin = lists:last(Topic),
            {Key, Val} = deser_pmap_kv(Name, KeyBin, Payload),
            Acc#{Key => Val}
        end,
        #{},
        ?DB,
        pmap_topic(Name, ClientId, '+'),
        0,
        #{shard => Shard}
    ),
    #pmap{
        name = Name,
        cache = Cache
    }.

-spec pmap_delete(
    emqx_persistent_session_ds:id(), emqx_persistent_session_ds_state:pmap(_, _)
) ->
    ok.
pmap_delete(ClientId, #pmap{name = Name}) ->
    emqx_ds:tx_del_topic(pmap_topic(Name, ClientId, '+')).

%% == Operations over PMap KV pairs ==

%% @doc Write a single key-value pair that belongs to a pmap:
-spec write_pmap_kv(atom(), emqx_persistent_session_ds:id(), _, _) -> emqx_ds:kv_pair().
write_pmap_kv(Name, ClientId, Key, Val) ->
    emqx_ds:tx_blob_write(
        pmap_topic(Name, ClientId, Key, Val),
        ser_payload(Name, Key, Val)
    ).

%% @doc Deserialize a single key-value pair that belongs to a pmap:
-spec deser_pmap_kv(atom(), binary(), binary()) -> {_Key, _Val}.
%% deser_pmap_kv(?subscriptions, _Key, Bin) ->
%%     {_TopicFilter, _Val} = maps:take(tf, binary_to_term(Bin));
deser_pmap_kv(Name, Key, Bin) ->
    {deser_pmap_key(Name, Key), deser_payload(Name, Bin)}.

%% Pmap key (topic):
-spec pmap_topic(atom(), emqx_persistent_session_ds:id(), _, _) -> emqx_ds:topic().
pmap_topic(Name, ClientId, Key, Val) ->
    pmap_topic(Name, ClientId, ser_pmap_key(Name, Key, Val)).

%% @doc Return topic that is used as a key when contents of the pmap
%% are stored in DS:
pmap_topic(Name, ClientId, Key) ->
    X =
        case Name of
            ?metadata -> ?top_meta;
            ?subscriptions -> ?top_sub;
            ?subscription_states -> ?top_sstate;
            ?seqnos -> ?top_seqno;
            ?streams -> ?top_stream;
            ?ranks -> ?top_rank;
            ?awaiting_rel -> ?top_awaiting_rel
        end,
    [?top_data, ClientId, X, Key].

ser_pmap_key(?metadata, MetaKey, _Val) ->
    atom_to_binary(MetaKey, latin1);
ser_pmap_key(_, Key, _Val) ->
    term_to_binary(Key).

deser_pmap_key(?metadata, Key) ->
    binary_to_atom(Key, latin1);
deser_pmap_key(_, Key) ->
    binary_to_term(Key).

%% Payload (de)serialization:

ser_payload(_Name, _Key, Val) ->
    term_to_binary(Val).

deser_payload(_, Bin) ->
    binary_to_term(Bin).

%% Misc.
