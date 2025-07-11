%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_will).

-behaviour(emqx_durable_timer).

%% API:
-export([init/0]).
-export([on_connect/4, on_disconnect/2, delete/1]).

%% behavior callbacks:
-export([durable_timer_type/0, handle_durable_timeout/2, timer_introduced_in/0]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include("emqx.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

-spec init() -> ok.
init() ->
    emqx_durable_timer:register_type(?MODULE).

-doc """
This function is called by the session when a new channel connects.
It updates the will message for the session.

Side effects:

- Previous will message associated with the session is deleted.

- If the new will message is present, it is checked whether the client
  is authorized to publish it.

- If it client is allowed to publish the will message, a durable timer is created
  that will publish the will message if the node goes down abruptly ("failsafe" timer is used).
""".
-spec on_connect(
    emqx_types:clientid(),
    emqx_types:clientinfo(),
    emqx_types:message() | undefined,
    non_neg_integer() | undefined
) ->
    ok | emqx_ds:error(_).
on_connect(ClientId, _ClientInfo, undefined, _WillDelay) ->
    emqx_durable_timer:cancel(durable_timer_type(), ClientId);
on_connect(ClientId, _ClientInfo, Msg = #message{}, WillDelay) when is_integer(WillDelay) ->
    %% FIXME: check authorization
    MsgBin = emqx_ds_msg_serializer:serialize(asn1, Msg),
    emqx_durable_timer:dead_hand(durable_timer_type(), ClientId, MsgBin, WillDelay).

-doc """
This function is called by the session when the channel disconnects.

Side effects:

- If DISCONNECT ReasonCode is 0, the current will message is deleted.
""".
-spec on_disconnect(emqx_types:clientid(), atom()) -> ok.
on_disconnect(_ClientId, _Reason) ->
    ok.

-spec delete(emqx_types:clientid()) -> ok.
delete(_ClientId) ->
    ok.

%%================================================================================
%% behavior callbacks
%%================================================================================

durable_timer_type() -> 16#3ABE_0000.

timer_introduced_in() -> "6.0.0.".

handle_durable_timeout(Key, Value) ->
    ?tp(warning, fixme_durable_will, #{key => Key, value => Value}).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
