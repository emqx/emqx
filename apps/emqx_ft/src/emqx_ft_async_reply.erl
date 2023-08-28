%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_async_reply).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([
    create_tables/0,
    info/0
]).

-export([
    register/3,
    take_by_mref/1,
    with_new_packet/3,
    deregister_all/1
]).

-type channel_pid() :: pid().
-type mon_ref() :: reference().
-type timer_ref() :: reference().
-type packet_id() :: emqx_types:packet_id().

%% packets waiting for async workers

-define(MON_TAB, emqx_ft_async_mons).
-define(MON_KEY(MRef), ?MON_KEY(self(), MRef)).
-define(MON_KEY(ChannelPid, MRef), {ChannelPid, MRef}).

%% async worker monitors by packet ids

-define(PACKET_TAB, emqx_ft_async_packets).
-define(PACKET_KEY(PacketId), ?PACKET_KEY(self(), PacketId)).
-define(PACKET_KEY(ChannelPid, PacketId), {ChannelPid, PacketId}).

%%--------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

-spec create_tables() -> ok.
create_tables() ->
    EtsOptions = [
        named_table,
        public,
        ordered_set,
        {read_concurrency, true},
        {write_concurrency, true}
    ],
    _ = ets:new(?MON_TAB, EtsOptions),
    _ = ets:new(?PACKET_TAB, EtsOptions),
    ok.

-spec register(packet_id(), mon_ref(), timer_ref()) -> ok.
register(PacketId, MRef, TRef) ->
    _ = ets:insert(?PACKET_TAB, {?PACKET_KEY(PacketId), MRef}),
    _ = ets:insert(?MON_TAB, {?MON_KEY(MRef), PacketId, TRef}),
    ok.

-spec with_new_packet(packet_id(), fun(() -> any()), any()) -> any().
with_new_packet(PacketId, Fun, Default) ->
    case ets:member(?PACKET_TAB, ?PACKET_KEY(PacketId)) of
        true -> Default;
        false -> Fun()
    end.

-spec take_by_mref(mon_ref()) -> {ok, packet_id(), timer_ref()} | not_found.
take_by_mref(MRef) ->
    case ets:take(?MON_TAB, ?MON_KEY(MRef)) of
        [{_, PacketId, TRef}] ->
            _ = ets:delete(?PACKET_TAB, ?PACKET_KEY(PacketId)),
            {ok, PacketId, TRef};
        [] ->
            not_found
    end.

-spec deregister_all(channel_pid()) -> ok.
deregister_all(ChannelPid) ->
    ok = deregister_packets(ChannelPid),
    ok = deregister_mons(ChannelPid),
    ok.

-spec info() -> {non_neg_integer(), non_neg_integer()}.
info() ->
    {ets:info(?MON_TAB, size), ets:info(?PACKET_TAB, size)}.

%%--------------------------------------------------------------------
%% Internal
%%-------------------------------------------------------------------

deregister_packets(ChannelPid) when is_pid(ChannelPid) ->
    MS = [{{?PACKET_KEY(ChannelPid, '_'), '_'}, [], [true]}],
    _ = ets:select_delete(?PACKET_TAB, MS),
    ok.

deregister_mons(ChannelPid) ->
    MS = [{{?MON_KEY(ChannelPid, '_'), '_', '_'}, [], [true]}],
    _ = ets:select_delete(?MON_TAB, MS),
    ok.
