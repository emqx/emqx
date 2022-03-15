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

-module(emqx_persistent_session_mnesia_disc_backend).

-include("emqx.hrl").
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

create_tables() ->
    ok = mria:create_table(?SESSION_STORE_DISC, [
        {type, set},
        {rlog_shard, ?PERSISTENT_SESSION_SHARD},
        {storage, disc_copies},
        {record_name, session_store},
        {attributes, record_info(fields, session_store)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),

    ok = mria:create_table(?SESS_MSG_TAB_DISC, [
        {type, ordered_set},
        {rlog_shard, ?PERSISTENT_SESSION_SHARD},
        {storage, disc_copies},
        {record_name, session_msg},
        {attributes, record_info(fields, session_msg)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),

    ok = mria:create_table(?MSG_TAB_DISC, [
        {type, ordered_set},
        {rlog_shard, ?PERSISTENT_SESSION_SHARD},
        {storage, disc_copies},
        {record_name, message},
        {attributes, record_info(fields, message)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]).

first_session_message() ->
    mnesia:dirty_first(?SESS_MSG_TAB_DISC).

next_session_message(Key) ->
    mnesia:dirty_next(?SESS_MSG_TAB_DISC, Key).

first_message_id() ->
    mnesia:dirty_first(?MSG_TAB_DISC).

next_message_id(Key) ->
    mnesia:dirty_next(?MSG_TAB_DISC, Key).

delete_message(Key) ->
    mria:dirty_delete(?MSG_TAB_DISC, Key).

delete_session_message(Key) ->
    mria:dirty_delete(?SESS_MSG_TAB_DISC, Key).

put_session_store(SS) ->
    mria:dirty_write(?SESSION_STORE_DISC, SS).

delete_session_store(ClientID) ->
    mria:dirty_delete(?SESSION_STORE_DISC, ClientID).

lookup_session_store(ClientID) ->
    case mnesia:dirty_read(?SESSION_STORE_DISC, ClientID) of
        [] -> none;
        [SS] -> {value, SS}
    end.

put_session_message(SessMsg) ->
    mria:dirty_write(?SESS_MSG_TAB_DISC, SessMsg).

put_message(Msg) ->
    mria:dirty_write(?MSG_TAB_DISC, Msg).

get_message(MsgId) ->
    case mnesia:read(?MSG_TAB_DISC, MsgId) of
        [] -> error({msg_not_found, MsgId});
        [Msg] -> Msg
    end.

ro_transaction(Fun) ->
    {atomic, Res} = mria:ro_transaction(?PERSISTENT_SESSION_SHARD, Fun),
    Res.
