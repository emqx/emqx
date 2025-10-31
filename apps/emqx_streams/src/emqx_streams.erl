%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams).

-include("emqx_streams_internal.hrl").

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-export([register_hooks/0, unregister_hooks/0]).

-export([
    on_message_publish/1,
    on_session_subscribed/3,
    on_session_unsubscribed/3
]).

%%

-spec register_hooks() -> ok.
register_hooks() ->
    %% FIXME: prios
    ok = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_HIGHEST),
    ok = emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_LOWEST).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}).

%%

on_message_publish(#message{topic = _Topic = <<"$sdisp/", _/binary>>} = Message) ->
    ?tp_debug(streams_on_message_publish, #{topic => _Topic}),
    on_shard_disp_message(Message);
on_message_publish(_Message) ->
    ok.

on_session_subscribed(ClientInfo, Topic = <<"$sdisp/", _/binary>>, _SubOpts) ->
    ?tp_debug(streams_on_on_session_subscribed, #{topic => Topic, subopts => _SubOpts}),
    on_shard_disp_subscription(ClientInfo, Topic);
on_session_subscribed(_ClientInfo, _Topic, _SubOpts) ->
    ok.

on_session_unsubscribed(ClientInfo, Topic = <<"$sdisp/", _/binary>>, _SubOpts) ->
    ?tp_debug(streams_on_on_session_unsubscribed, #{topic => Topic, subopts => _SubOpts}),
    on_shard_disp_unsubscription(ClientInfo, Topic);
on_session_unsubscribed(_ClientInfo, _Topic, _SubOpts) ->
    ok.

%%------------------------------------------------------------------------------
%% Shard Dispatch
%%------------------------------------------------------------------------------
%%
%% === Protocol
%%
%% The goal of the protocol is to:
%%  * Distribute shards of a stream among consumers.
%%  * Redistribute shards when existing consumers go away and new ones come in.
%%  * Ensure eventually well-balanced distribution of shards.
%%    - According to implementation-defined metric, e.g.: consumer load, locality,
%%      client responsiveness.
%%
%% Stream consumers are protocol participants. Shard dispatch protocol is separate
%% from stream consumption. Participants are expected to cooperate, there's no
%% measures against subversion.
%%
%% Shard dispatch is facilitated through MQTT subscriptions and messaging to special
%% topics rooted at `$sdisp/`.
%%
%% 1) Stream consumer announces itself through subscription to special topic.
%%    Topic: `$sdisp/consumer/<stream>/#`.
%%     Opts: QoS=0/1
%%    Broker registers stream consumer by its ClientID. If Broker already knows
%%    this consumer (even if there are shards already allocated to it), Broker
%%    always assumes that the consumer starts anew.
%%    Such minimal SUBCRIBE packet essentially means Consumer speaks current, 1.0
%%    version of the protocol.
%%
%% 2) Each time broker has a shard to dispatch to this consumer, a message is sent.
%%      Message Topic: `$sdisp/consumer/<stream>/lease/<shard>/<last-offset>`
%%    Message Payload: <none>
%%    This message represents _proposal_, shard distribution does not change yet.
%%
%% 3) Stream consumer accepts the proposal by publishing a message.
%%    Message Topic: `$sdisp/progress/<stream>/<shard>/<offset>`
%%      Message QoS: 1
%%    Broker allocates specified shard to this consumer. Consumer should expect to
%%    receive `PUBACK(SUCCESS)`, before that shard should still be considered
%%    unallocated. If `PUBACK(ERROR)` is received, the allocation was refused.
%%    Strictly speaking, Consumer may attempt to publish such message w/o receiving
%%    proposal first, it's up to Broker to allow that.
%%
%% 4) Broker may attempt to redistribute shards, in this case a message is sent.
%%      Message Topic: `$sdisp/consumer/<stream>/release/<shard>`
%%    Message Payload: <none>
%%
%% 5) Stream consumer respects such request by publishing another message.
%%    Message Topic: `$sdisp/release/<stream>/<shard>/<offset>`
%%      Message QoS: 1
%%    Similarly, only `PUBACK(SUCCESS)` means specified shard is unallocated.
%%    Only when Broker receives _release_ message, shard is considered unallocated.
%%    Obviously, a misbehaving consumer may hold onto shards indefinitely. In this
%%    case one option Broker has is to forcefully shut down consumer's connection.
%%
%% *) Moreover, Consumers must publish same _progress_ messages each time another
%%    batch of messages in the stream shard was processed.
%%    Message Topic: `$sdisp/progress/<stream>/<shard>/<new-offset>`
%%      Message QoS: 1
%%    Once again, `PUBACK(SUCCESS)` means progress is saved. `PUBACK(ERROR)`s are
%%    possible and should be handled accordingly.
%%
%% For _progress_ messages, `PUBACK`s communicate logic error conditions: invalid
%% stream / shard, offset going backwards, shard allocation conflicts. Broker should
%% handle _progress_ messages idempontently.
%%
%% Progress messages may in theory also serve as heartbeats, it's unclear however
%% how useful it is when there's MQTT Keep-Alive mechanism.

on_shard_disp_subscription(ClientInfo, Topic) ->
    ok.

on_shard_disp_unsubscription(ClientInfo, Topic) ->
    ok.

on_shard_disp_message(Message) ->
    ok.
