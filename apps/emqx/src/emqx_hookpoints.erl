%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_hookpoints).

-type callback_result() :: stop | any().
-type fold_callback_result(Acc) :: {stop, Acc} | {ok, Acc} | stop | any().

-export_type([
    fold_callback_result/1,
    callback_result/0
]).

-export([
    default_hookpoints/0,
    register_hookpoints/0,
    register_hookpoints/1,
    verify_hookpoint/1
]).

%%-----------------------------------------------------------------------------
%% Hookpoints
%%-----------------------------------------------------------------------------

-define(HOOKPOINTS, [
    'client.connect',
    'client.connack',
    'client.connected',
    'client.disconnected',
    'client.authorize',
    'client.check_authz_complete',
    'client.authenticate',
    'client.subscribe',
    'client.unsubscribe',
    'client.timeout',
    'client.monitored_process_down',
    'session.created',
    'session.subscribed',
    'session.unsubscribed',
    'session.resumed',
    'session.discarded',
    'session.takenover',
    'session.terminated',
    'message.publish',
    'message.puback',
    'message.dropped',
    'message.delivered',
    'message.acked',
    'delivery.dropped',
    'delivery.completed',
    'cm.channel.unregistered',
    'tls_handshake.psk_lookup',

    %% This is a deprecated hookpoint renamed to 'client.authorize'
    %% However, our template plugin used this hookpoint before its 5.1.0 version,
    %% so we keep it here
    'client.check_acl'
]).

%%-----------------------------------------------------------------------------
%% Callbacks
%%-----------------------------------------------------------------------------

%% Callback definitions are given for documentation purposes.
%% Each hook callback implementation can also accept any number of custom arguments
%% after the mandatory ones.
%%
%% By default, callbacks are executed in the channel process context.

-callback 'client.connect'(emqx_types:conninfo(), Props) ->
    fold_callback_result(Props)
when
    Props :: emqx_types:properties().

-callback 'client.connack'(emqx_types:conninfo(), _Reason :: atom(), Props) ->
    fold_callback_result(Props)
when
    Props :: emqx_types:properties().

-callback 'client.connected'(emqx_types:clientinfo(), emqx_types:conninfo()) -> callback_result().

-callback 'client.disconnected'(emqx_types:clientinfo(), _Reason :: atom(), emqx_types:conninfo()) ->
    callback_result().

-callback 'client.authorize'(
    emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic(), allow | deny
) ->
    fold_callback_result(#{result := allow | deny, from => term()}).

-callback 'client.check_authz_complete'(
    emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic(), allow | deny, _From :: term()
) ->
    callback_result().

-callback 'client.authenticate'(emqx_types:clientinfo(), ignore) ->
    fold_callback_result(
        ignore
        | ok
        | {ok, map()}
        | {ok, map(), binary()}
        | {continue, map()}
        | {continue, binary(), map()}
        | {error, term()}
    ).

-callback 'client.subscribe'(emqx_types:clientinfo(), emqx_types:properties(), TopicFilters) ->
    fold_callback_result(TopicFilters)
when
    TopicFilters :: list({emqx_topic:topic(), map()}).

-callback 'client.unsubscribe'(emqx_types:clientinfo(), emqx_types:properties(), TopicFilters) ->
    fold_callback_result(TopicFilters)
when
    TopicFilters :: list({emqx_topic:topic(), map()}).

-callback 'client.timeout'(_TimerReference :: reference(), _Msg :: term(), Replies) ->
    fold_callback_result(Replies)
when
    Replies :: emqx_channel:replies().

-callback 'client.monitored_process_down'(
    _MonitorRef :: reference(), _Pid :: pid(), _Reason :: term(), Replies
) ->
    fold_callback_result(Replies)
when
    Replies :: emqx_channel:replies().

-callback 'session.created'(emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    callback_result().

-callback 'session.subscribed'(emqx_types:clientinfo(), emqx_types:topic(), emqx_types:subopts()) ->
    callback_result().

-callback 'session.unsubscribed'(emqx_types:clientinfo(), emqx_types:topic(), emqx_types:subopts()) ->
    callback_result().

-callback 'session.resumed'(emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    callback_result().

-callback 'session.discarded'(emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    callback_result().

-callback 'session.takenover'(emqx_types:clientinfo(), _SessionInfo :: emqx_types:infos()) ->
    callback_result().

-callback 'session.terminated'(
    emqx_types:clientinfo(), _Reason :: atom(), _SessionInfo :: emqx_types:infos()
) -> callback_result().

-callback 'message.publish'(Msg) ->
    fold_callback_result(Msg)
when
    Msg :: emqx_types:message().

-callback 'message.puback'(
    emqx_types:packet_id(),
    emqx_types:message(),
    emqx_types:publish_result(),
    emqx_types:reason_code()
) ->
    fold_callback_result(undefined | emqx_types:reason_code()).

-callback 'message.dropped'(emqx_types:message(), #{node => node()}, _Reason :: atom()) ->
    callback_result().

-callback 'message.delivered'(emqx_types:clientinfo(), Msg) -> fold_callback_result(Msg) when
    Msg :: emqx_types:message().

-callback 'message.acked'(emqx_types:clientinfo(), emqx_types:message()) -> callback_result().

-callback 'delivery.dropped'(emqx_types:clientinfo(), emqx_types:message(), _Reason :: atom()) ->
    callback_result().

-callback 'delivery.completed'(emqx_types:message(), #{
    session_birth_time := emqx_utils_calendar:epoch_millisecond(), clientid := emqx_types:clientid()
}) ->
    callback_result().

%% NOTE
%% Executed out of channel process context
-callback 'cm.channel.unregistered'(_ChanPid :: pid()) -> callback_result().

%% NOTE
%% Executed out of channel process context
-callback 'tls_handshake.psk_lookup'(emqx_tls_psk:psk_identity(), normal) ->
    fold_callback_result(
        {ok, _SharedSecret :: binary()}
        | {error, term()}
        | normal
    ).

%%-----------------------------------------------------------------------------
%% API
%%-----------------------------------------------------------------------------

default_hookpoints() ->
    ?HOOKPOINTS.

register_hookpoints() ->
    register_hookpoints(default_hookpoints()).

register_hookpoints(HookPoints) ->
    persistent_term:put(?MODULE, maps:from_keys(HookPoints, true)).

verify_hookpoint(HookPoint) when is_binary(HookPoint) -> ok;
verify_hookpoint(HookPoint) ->
    case maps:is_key(HookPoint, registered_hookpoints()) of
        true ->
            ok;
        false ->
            error({invalid_hookpoint, HookPoint})
    end.

%%-----------------------------------------------------------------------------
%% Internal API
%%-----------------------------------------------------------------------------

registered_hookpoints() ->
    persistent_term:get(?MODULE, #{}).
