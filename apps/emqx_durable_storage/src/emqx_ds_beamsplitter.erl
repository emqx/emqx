%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_beamsplitter).

%% API:
-export([dispatch_v3/5]).

%% internal exports:
-export([]).

-export_type([pack_v3/0, destination/0]).

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

-type pack_v3() :: [emqx_ds:payload()] | end_of_stream | emqx_ds:error(_).

%%================================================================================
%% API functions
%%================================================================================

%% @doc Third version of the API dropped DSKeys from the pack
-spec dispatch_v3(
    emqx_ds:db(), emqx_ds_payload_transform:schema(), pack_v3(), [destination()], map()
) ->
    ok.
dispatch_v3(DB, PTrans, Pack0, Destinations, _Misc) ->
    %% TODO: paralellize fanout? Perhaps sharding messages in the DB
    %% is already sufficient.
    ?tp_ignore_side_effects_in_prod(emqx_ds_beamsplitter_dispatch, #{
        pack => Pack0, destinations => Destinations
    }),
    T0 = erlang:monotonic_time(microsecond),
    Pack =
        case is_list(Pack0) of
            true -> emqx_ds_payload_transform:deser_batch(PTrans, Pack0);
            false -> Pack0
        end,
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
