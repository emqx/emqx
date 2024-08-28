%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exclusive_subscription).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[exclusive]").

%% Mnesia bootstrap
-export([create_tables/0]).

%% For upgrade
-export([on_add_module/0, on_delete_module/0]).

-export([
    check_subscribe/2,
    unsubscribe/2,
    dirty_lookup_cid/1,
    clear/0
]).

%% Internal exports (RPC)
-export([
    try_subscribe/2
]).

-record(exclusive_subscription, {
    topic :: emqx_types:topic(),
    %% Note: before 5.8.0 this field data type is `emqx_types:clientid()`
    cid :: emqx_types:cid() | emqx_types:clientid()
}).

-define(TAB, emqx_exclusive_subscription).
-define(EXCLUSIVE_SHARD, emqx_exclusive_shard).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

create_tables() ->
    StoreProps = [
        {ets, [
            {read_concurrency, true},
            {write_concurrency, true}
        ]}
    ],
    ok = mria:create_table(?TAB, [
        {rlog_shard, ?EXCLUSIVE_SHARD},
        {type, set},
        {storage, ram_copies},
        {record_name, exclusive_subscription},
        {attributes, record_info(fields, exclusive_subscription)},
        {storage_properties, StoreProps}
    ]),
    [?TAB].

%%--------------------------------------------------------------------
%% Upgrade
%%--------------------------------------------------------------------

on_add_module() ->
    mria:wait_for_tables(create_tables()).

on_delete_module() ->
    clear().

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
-spec check_subscribe(emqx_types:clientinfo(), emqx_types:topic()) ->
    allow | deny.
check_subscribe(ClientInfo, Topic) ->
    CId = emqx_mtns:cid(ClientInfo),
    case mria:transaction(?EXCLUSIVE_SHARD, fun ?MODULE:try_subscribe/2, [CId, Topic]) of
        {atomic, Res} ->
            Res;
        {aborted, Reason} ->
            ?SLOG(warning, #{
                msg => "check_subscribe_aborted",
                topic => Topic,
                reason => Reason,
                clientid => maps:get(clientid, ClientInfo)
            }),
            deny
    end.

unsubscribe(Topic, #{is_exclusive := true}) ->
    _ = mria:transaction(?EXCLUSIVE_SHARD, fun mnesia:delete/1, [{?TAB, Topic}]),
    ok;
unsubscribe(_Topic, _SubOpts) ->
    ok.

-spec dirty_lookup_cid(emqx_types:topic()) -> emqx_types:cid() | emqx_types:clientid() | undefined.
dirty_lookup_cid(Topic) ->
    case mnesia:dirty_read(?TAB, Topic) of
        [#exclusive_subscription{cid = CId}] ->
            CId;
        _ ->
            undefined
    end.

clear() ->
    mria:clear_table(?TAB).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
try_subscribe(CId, Topic) ->
    case mnesia:wread({?TAB, Topic}) of
        [] ->
            mnesia:write(
                ?TAB,
                #exclusive_subscription{
                    cid = CId,
                    topic = Topic
                },
                write
            ),
            allow;
        [#exclusive_subscription{cid = CId, topic = Topic}] ->
            %% Fixed the issue-13476
            %% In this feature, the user must manually call `unsubscribe` to release the lock,
            %% but sometimes the node may go down for some reason,
            %% then the client will reconnect to this node and resubscribe.
            %% We need to allow resubscription, otherwise the lock will never be released.
            allow;
        [_] ->
            deny
    end.
