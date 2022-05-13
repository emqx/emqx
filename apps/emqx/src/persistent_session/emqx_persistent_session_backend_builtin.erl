%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_backend_builtin).

-include("emqx.hrl").
-include_lib("typerefl/include/types.hrl").
-include("emqx_persistent_session.hrl").

-export([
    create_tables/0,
    first_message_id/0,
    next_message_id/1,
    delete_message/1,
    first_session_message/0,
    next_session_message/1,
    delete_session_message/1,
    put_session_store/1,
    delete_session_store/1,
    lookup_session_store/1,
    put_session_message/1,
    put_message/1,
    get_message/1,
    ro_transaction/1
]).

-type mria_table_type() :: ram_copies | disc_copies | rocksdb_copies.

-define(IS_ETS(BACKEND), (BACKEND =:= ram_copies orelse BACKEND =:= disc_copies)).

create_tables() ->
    SessStoreBackend = table_type(session),
    ok = mria:create_table(?SESSION_STORE, [
        {type, set},
        {rlog_shard, ?PERSISTENT_SESSION_SHARD},
        {storage, SessStoreBackend},
        {record_name, session_store},
        {attributes, record_info(fields, session_store)},
        {storage_properties, storage_properties(?SESSION_STORE, SessStoreBackend)}
    ]),

    SessMsgBackend = table_type(session_messages),
    ok = mria:create_table(?SESS_MSG_TAB, [
        {type, ordered_set},
        {rlog_shard, ?PERSISTENT_SESSION_SHARD},
        {storage, SessMsgBackend},
        {record_name, session_msg},
        {attributes, record_info(fields, session_msg)},
        {storage_properties, storage_properties(?SESS_MSG_TAB, SessMsgBackend)}
    ]),

    MsgBackend = table_type(messages),
    ok = mria:create_table(?MSG_TAB, [
        {type, ordered_set},
        {rlog_shard, ?PERSISTENT_SESSION_SHARD},
        {storage, MsgBackend},
        {record_name, message},
        {attributes, record_info(fields, message)},
        {storage_properties, storage_properties(?MSG_TAB, MsgBackend)}
    ]).

first_session_message() ->
    mnesia:dirty_first(?SESS_MSG_TAB).

next_session_message(Key) ->
    mnesia:dirty_next(?SESS_MSG_TAB, Key).

first_message_id() ->
    mnesia:dirty_first(?MSG_TAB).

next_message_id(Key) ->
    mnesia:dirty_next(?MSG_TAB, Key).

delete_message(Key) ->
    mria:dirty_delete(?MSG_TAB, Key).

delete_session_message(Key) ->
    mria:dirty_delete(?SESS_MSG_TAB, Key).

put_session_store(SS) ->
    mria:dirty_write(?SESSION_STORE, SS).

delete_session_store(ClientID) ->
    mria:dirty_delete(?SESSION_STORE, ClientID).

lookup_session_store(ClientID) ->
    case mnesia:dirty_read(?SESSION_STORE, ClientID) of
        [] -> none;
        [SS] -> {value, SS}
    end.

put_session_message(SessMsg) ->
    mria:dirty_write(?SESS_MSG_TAB, SessMsg).

put_message(Msg) ->
    mria:dirty_write(?MSG_TAB, Msg).

get_message(MsgId) ->
    case mnesia:read(?MSG_TAB, MsgId) of
        [] -> error({msg_not_found, MsgId});
        [Msg] -> Msg
    end.

ro_transaction(Fun) ->
    {atomic, Res} = mria:ro_transaction(?PERSISTENT_SESSION_SHARD, Fun),
    Res.

-spec storage_properties(?SESSION_STORE | ?SESS_MSG_TAB | ?MSG_TAB, mria_table_type()) -> term().
storage_properties(?SESSION_STORE, Backend) when ?IS_ETS(Backend) ->
    [{ets, [{read_concurrency, true}]}];
storage_properties(_, Backend) when ?IS_ETS(Backend) ->
    [
        {ets, [
            {read_concurrency, true},
            {write_concurrency, true}
        ]}
    ];
storage_properties(_, _) ->
    [].

-spec table_type(atom()) -> mria_table_type().
table_type(Table) ->
    DiscPersistence = emqx_config:get([?cfg_root, on_disc]),
    RamCache = get_overlayed(Table, ram_cache),
    case {DiscPersistence, RamCache} of
        {true, true} ->
            disc_copies;
        {true, false} ->
            rocksdb_copies;
        {false, _} ->
            ram_copies
    end.

-spec get_overlayed(atom(), on_disc | ram_cache) -> boolean().
get_overlayed(Table, Suffix) ->
    Default = emqx_config:get([?cfg_root, Suffix]),
    emqx_config:get([?cfg_root, backend, Table, Suffix], Default).
