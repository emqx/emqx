%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("logger.hrl").

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
    verify_hookpoint/1,
    registered_hookpoints/0
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
    'client.check_authn_complete',
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
    'message.transformation_failed',
    'schema.validation_failed',
    'message.delivered',
    'message.acked',
    'delivery.dropped',
    'delivery.completed',
    'cm.channel.unregistered',
    'tls_handshake.psk_lookup'
]).

%% Our template plugin used this hookpoints before its 5.1.0 version,
%% so we keep them here
-define(DEPRECATED_HOOKPOINTS, [
    %% This is a deprecated hookpoint renamed to 'client.authorize'
    'client.check_acl',
    %% Misspelled hookpoint
    'session.takeovered'
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
    TopicFilters :: list({emqx_types:topic(), map()}).

-callback 'client.unsubscribe'(emqx_types:clientinfo(), emqx_types:properties(), TopicFilters) ->
    fold_callback_result(TopicFilters)
when
    TopicFilters :: list({emqx_types:topic(), map()}).

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

-callback 'schema.validation_failed'(emqx_types:message(), #{node => node()}, _Ctx :: map()) ->
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

%% Binary hookpoint names are dynamic and used for bridges
-type registered_hookpoint() :: atom().
-type registered_hookpoint_status() :: valid | deprecated.

-spec default_hookpoints() -> #{registered_hookpoint() => registered_hookpoint_status()}.
default_hookpoints() ->
    maps:merge(
        maps:from_keys(?HOOKPOINTS, valid),
        maps:from_keys(?DEPRECATED_HOOKPOINTS, deprecated)
    ).

-spec register_hookpoints() -> ok.
register_hookpoints() ->
    register_hookpoints(default_hookpoints()).

-spec register_hookpoints(
    [registered_hookpoint()] | #{registered_hookpoint() => registered_hookpoint_status()}
) -> ok.
register_hookpoints(HookPoints) when is_list(HookPoints) ->
    register_hookpoints(maps:from_keys(HookPoints, valid));
register_hookpoints(HookPoints) when is_map(HookPoints) ->
    persistent_term:put(?MODULE, HookPoints).

-spec verify_hookpoint(registered_hookpoint() | binary()) -> ok | no_return().
verify_hookpoint(HookPoint) when is_binary(HookPoint) -> ok;
verify_hookpoint(HookPoint) ->
    case maps:get(HookPoint, registered_hookpoints(), invalid) of
        valid -> ok;
        deprecated -> ?SLOG(warning, #{msg => deprecated_hookpoint, hookpoint => HookPoint});
        invalid -> error({invalid_hookpoint, HookPoint})
    end.

-spec registered_hookpoints() -> #{registered_hookpoint() => registered_hookpoint_status()}.
registered_hookpoints() ->
    persistent_term:get(?MODULE, #{}).
