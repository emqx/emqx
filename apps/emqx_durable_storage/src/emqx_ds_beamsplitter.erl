%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_beamsplitter).

%% API:
-export([dispatch_v2/4]).

%% internal exports:
-export([]).

-export_type([pack/0, destination/0]).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_ds.hrl").
-include("emqx_ds_beamformer.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type flags() :: integer().

-type dispatch_mask() :: emqx_ds_dispatch_mask:encoding().

-type destination() :: ?DESTINATION(
    pid(), reference(), emqx_ds:sub_seqno(), dispatch_mask(), flags(), _Iterator
).

-type pack() ::
    [{emqx_ds:message_key(), emqx_types:message()}]
    | end_of_stream
    | emqx_ds:error(_).

%%================================================================================
%% API functions
%%================================================================================

%% @doc Note: first version of dispatch was implemented in
%% `emqx_ds_beamformer' module
-spec dispatch_v2(emqx_ds:db(), Pack, Destinations, map()) -> ok when
    Pack :: pack(),
    Destinations :: [destination()].
dispatch_v2(DB, Pack, Destinations, _Misc) ->
    %% TODO: paralellize fanout? Perhaps sharding messages in the DB
    %% is already sufficient.
    ?tp(emqx_ds_beamsplitter_dispatch, #{pack => Pack, destinations => Destinations}),
    T0 = erlang:monotonic_time(microsecond),
    lists:foreach(
        fun(?DESTINATION(Client, SubRef, SeqNo, Mask, Flags, EndIterator)) ->
            {Size, Payload} = mk_payload(Pack, Mask, EndIterator),
            Client !
                #ds_sub_reply{
                    ref = SubRef,
                    payload = Payload,
                    size = Size,
                    seqno = SeqNo,
                    lagging = (Flags band ?DISPATCH_FLAG_LAGGING) > 0,
                    stuck = (Flags band ?DISPATCH_FLAG_STUCK) > 0
                }
        end,
        Destinations
    ),
    emqx_ds_builtin_metrics:observe_beamsplitter_fanout_time(
        emqx_ds_builtin_metrics:metric_id([{db, DB}]),
        erlang:monotonic_time(microsecond) - T0
    ),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

mk_payload(end_of_stream, _, _) ->
    {1, {ok, end_of_stream}};
mk_payload(Err = {error, _, _}, _, _) ->
    {1, Err};
mk_payload(Pack, Mask, EndIterator) when is_list(Pack) ->
    {Size, Msgs} = emqx_ds_dispatch_mask:filter_and_size(Pack, Mask),
    {Size, {ok, EndIterator, Msgs}}.
