%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_will).

-behaviour(emqx_durable_timer).

%% API:
-export([init/0]).
-export([on_connect/4, on_disconnect/4, clear/1]).

%% behavior callbacks:
-export([durable_timer_type/0, handle_durable_timeout/2, timer_introduced_in/0]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
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

- If the client is allowed to publish the will message, a durable timer is created
  that will publish the will message if the node goes down abruptly ("dead hand" timer is used).
""".
-spec on_connect(
    emqx_types:clientid(),
    emqx_types:clientinfo(),
    non_neg_integer(),
    emqx_types:message() | undefined
) ->
    ok | emqx_ds:error(_).
on_connect(ClientId, ClientInfo, SessExpiryMS, MaybeWillMsg) ->
    case check(ClientInfo, SessExpiryMS, MaybeWillMsg) of
        {ok, Delay, MsgBin} ->
            emqx_durable_timer:dead_hand(durable_timer_type(), ClientId, MsgBin, Delay);
        undefined ->
            clear(ClientId)
    end.

-doc """
This function is called by the session when the channel disconnects.

Side effects:

- If DISCONNECT ReasonCode is 0, the current will message is deleted.

- Otherwise, authorization checks run to verify that client is eligible to publish to the will topic.
  If the check is successful, a regular durable timer is started (this removes the dead hand timer).
""".
-spec on_disconnect(
    emqx_types:clientid(),
    emqx_types:clientinfo(),
    non_neg_integer(),
    emqx_types:message() | undefined
) -> ok.
on_disconnect(ClientId, ClientInfo, SessExpiryMS, MaybeWillMsg) ->
    case check(ClientInfo, SessExpiryMS, MaybeWillMsg) of
        {ok, Delay, MsgBin} ->
            warn_timeout(
                emqx_durable_timer:apply_after(durable_timer_type(), ClientId, MsgBin, Delay)
            );
        _ ->
            clear(ClientId)
    end.

-spec clear(emqx_types:clientid()) -> ok.
clear(ClientId) ->
    emqx_durable_timer:cancel(durable_timer_type(), ClientId).

%%================================================================================
%% behavior callbacks
%%================================================================================

durable_timer_type() -> 16#3ABE0000.

timer_introduced_in() -> "6.0.0".

handle_durable_timeout(_Key, MsgBin) ->
    Msg = emqx_ds_msg_serializer:deserialize(asn1, MsgBin),
    _ = emqx_broker:publish(Msg),
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

check(_ClientInfo, _SessExpiryMS, undefined) ->
    undefined;
check(ClientInfo, SessExpiryMS, WillMsg0) ->
    case emqx_channel:prepare_will_message_for_publishing(ClientInfo, WillMsg0) of
        {ok, WillMsg} ->
            WillDelay = min(timer:seconds(emqx_channel:will_delay_interval(WillMsg)), SessExpiryMS),
            MsgBin = emqx_ds_msg_serializer:serialize(asn1, WillMsg),
            {ok, WillDelay, MsgBin};
        {error, _} ->
            undefined
    end.

warn_timeout(ok) ->
    ok;
warn_timeout(?err_unrec(commit_timeout)) ->
    ?tp(warning, "durable_will_timer_commit_timeout", #{}),
    ok;
warn_timeout(Err) ->
    Err.
