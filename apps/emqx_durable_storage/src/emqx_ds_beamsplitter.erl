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
-export([dispatch_v2/2]).

%% internal exports:
-export([]).

-export_type([destination/0]).

-include("emqx_ds.hrl").
-include("emqx_ds_beamformer.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type flags() :: integer().

-type dispatch_mask() :: emqx_ds_dispatch_mask:encoding().

-type destination() :: ?DESTINATION(
    pid(), reference(), _UserData, emqx_ds:sub_seqno(), dispatch_mask(), flags(), _Iterator
).

%%================================================================================
%% API functions
%%================================================================================

%% @doc Note: first version of dispatch was implemented in
%% `emqx_ds_beamformer' module
-spec dispatch_v2(Pack, Destinations) -> ok when
    Pack ::
        [{emqx_ds:message_key(), emqx_types:message()}]
        | end_of_stream
        | emqx_ds:error(_),
    Destinations :: [destination()].
dispatch_v2(Pack, Destinations) ->
    %% TODO: paralellize fanout? Perhaps sharding messages in the DB
    %% is already sufficient.
    lists:foreach(
        fun(?DESTINATION(Client, SubRef, ItKey, SeqNo, Mask, Flags, EndIterator)) ->
            Payload = mk_payload(Pack, Mask, EndIterator),
            Client !
                #poll_reply{
                    ref = SubRef,
                    userdata = ItKey,
                    payload = Payload,
                    seqno = SeqNo,
                    lagging = (Flags band ?DISPATCH_FLAG_LAGGING) > 0,
                    stuck = (Flags band ?DISPATCH_FLAG_STUCK) > 0
                }
        end,
        Destinations
    ).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

mk_payload(end_of_stream, _, _) ->
    {ok, end_of_stream};
mk_payload(Err = {error, _, _}, _, _) ->
    Err;
mk_payload(Pack, Mask, EndIterator) when is_list(Pack) ->
    Msgs = emqx_ds_dispatch_mask:filter(Pack, Mask),
    {ok, EndIterator, Msgs}.
