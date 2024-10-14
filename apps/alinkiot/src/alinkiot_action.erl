%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 11. 3月 2023 下午8:05
%%%-------------------------------------------------------------------
-module(alinkiot_action).
-author("yqfclid").

%% for #message{} record
%% no need for this include if we call emqx_message:to_map/1 to convert it to a map
-include_lib("emqx/include/emqx.hrl").

%% for logging
-include_lib("emqx/include/logger.hrl").

-export([
    load/1,
    unload/0
]).

%% Client Lifecircle Hooks
-export([
    on_client_connect/3,
    on_client_connack/4,
    on_client_connected/3,
    on_client_disconnected/4,
    on_client_authenticate/3,
    on_client_authorize/5,
    on_client_subscribe/4,
    on_client_unsubscribe/4
]).

%% Session Lifecircle Hooks
-export([
    on_session_created/3,
    on_session_subscribed/4,
    on_session_unsubscribed/4,
    on_session_resumed/3,
    on_session_discarded/3,
    on_session_takeovered/3,
    on_session_terminated/4
]).

%% Message Pubsub Hooks
-export([
    on_message_publish/2,
    on_message_delivered/3,
    on_message_acked/3,
    on_message_dropped/4
]).

%% Called when the plugin application start
load(Env) ->
    hook('client.connect',      {?MODULE, on_client_connect, [Env]}),
    hook('client.connack',      {?MODULE, on_client_connack, [Env]}),
    hook('client.connected',    {?MODULE, on_client_connected, [Env]}),
    hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
    hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),
    hook('client.authorize',    {?MODULE, on_client_authorize, [Env]}),
    hook('client.check_acl',    {?MODULE, on_client_check_acl, [Env]}),
    hook('client.subscribe',    {?MODULE, on_client_subscribe, [Env]}),
    hook('client.unsubscribe',  {?MODULE, on_client_unsubscribe, [Env]}),
    hook('session.created',     {?MODULE, on_session_created, [Env]}),
    hook('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}),
    hook('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}),
    hook('session.resumed',     {?MODULE, on_session_resumed, [Env]}),
    hook('session.discarded',   {?MODULE, on_session_discarded, [Env]}),
    hook('session.takeovered',  {?MODULE, on_session_takeovered, [Env]}),
    hook('session.terminated',  {?MODULE, on_session_terminated, [Env]}),
    hook('message.publish',     {?MODULE, on_message_publish, [Env]}),
    hook('message.delivered',   {?MODULE, on_message_delivered, [Env]}),
    hook('message.acked',       {?MODULE, on_message_acked, [Env]}),
    hook('message.dropped',     {?MODULE, on_message_dropped, [Env]}).

%%--------------------------------------------------------------------
%% Client LifeCircle Hooks
%%--------------------------------------------------------------------

on_client_connect(_ConnInfo, Props, _Env) ->
    %% this is to demo the usage of EMQX's structured-logging macro
    %% * Recommended to always have a `msg` field,
    %% * Use underscore instead of space to help log indexers,
    %% * Try to use static fields
    {ok, Props}.

on_client_connack(_ConnInfo = #{clientid := _ClientId}, _Rc, Props, _Env) ->
    {ok, Props}.

on_client_connected(_ClientInfo = #{clientid := _ClientId}, _ConnInfo, _Env) ->
    ok.

on_client_disconnected(_ClientInfo = #{clientid := _ClientId}, _ReasonCode, _ConnInfo, _Env) ->
    ok.

on_client_authenticate(_ClientInfo = #{clientid := _ClientId}, Result, _Env) ->
    {ok, Result}.

on_client_authorize(_ClientInfo = #{clientid := _ClientId}, _PubSub, _Topic, Result, _Env) ->
    {ok, Result}.

on_client_subscribe(#{clientid := _ClientId}, _Properties, TopicFilters, _Env) ->
    {ok, TopicFilters}.

on_client_unsubscribe(#{clientid := _ClientId}, _Properties, TopicFilters, _Env) ->
    {ok, TopicFilters}.

%%--------------------------------------------------------------------
%% Session LifeCircle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := _ClientId}, _SessInfo, _Env) ->
    ok.

on_session_subscribed(#{clientid := _ClientId}, _Topic, _SubOpts, _Env) ->
    ok.

on_session_unsubscribed(#{clientid := _ClientId}, _Topic, _Opts, _Env) ->
    ok.

on_session_resumed(#{clientid := _ClientId}, _SessInfo, _Env) ->
    ok.

on_session_discarded(_ClientInfo = #{clientid := _ClientId}, _SessInfo, _Env) ->
    ok.

on_session_takeovered(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) is takeovered. Session Info: ~p~n", [ClientId, SessInfo]).

on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
    io:format("Session(~s) is terminated due to ~p~nSession Info: ~p~n",
        [ClientId, Reason, SessInfo]).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    #message{id = Id,
        qos = Qos,
        flags = Flags,
        headers = Headers,
        topic = Topic,
        payload = Payload,
        timestamp = Time} = Message,
    MessageMap = #{
        id => Id,
        qos => Qos,
        flags => Flags,
        headers => Headers,
        topic => Topic,
        payload => Payload,
        time => Time
    },
    NMessageMap = alinkiot_hooks:run_fold('message.publish', [], MessageMap),
    NMessage =
        #message{
            id = maps:get(id, NMessageMap),
            qos = maps:get(qos, NMessageMap),
            flags = maps:get(flags, NMessageMap),
            headers = maps:get(headers, NMessageMap),
            topic = maps:get(topic, NMessageMap),
            payload = maps:get(payload, NMessageMap),
            timestamp = maps:get(timestamp, NMessageMap)
        },
    {ok, NMessage}.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
    ok;
on_message_dropped(_Message, _By = #{node := _Node}, _Reason, _Env) ->
    ok.

on_message_delivered(_ClientInfo = #{clientid := _ClientId}, Message, _Env) ->
    {ok, Message}.

on_message_acked(_ClientInfo = #{clientid := _ClientId}, _Message, _Env) ->
    ok.

%% Called when the plugin application stop
unload() ->
    unhook('client.connect',      {?MODULE, on_client_connect}),
    unhook('client.connack',      {?MODULE, on_client_connack}),
    unhook('client.connected',    {?MODULE, on_client_connected}),
    unhook('client.disconnected', {?MODULE, on_client_disconnected}),
    unhook('client.authenticate', {?MODULE, on_client_authenticate}),
    unhook('client.authorize',    {?MODULE, on_client_authorize}),
    unhook('client.check_acl',    {?MODULE, on_client_check_acl}),
    unhook('client.subscribe',    {?MODULE, on_client_subscribe}),
    unhook('client.unsubscribe',  {?MODULE, on_client_unsubscribe}),
    unhook('session.created',     {?MODULE, on_session_created}),
    unhook('session.subscribed',  {?MODULE, on_session_subscribed}),
    unhook('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
    unhook('session.resumed',     {?MODULE, on_session_resumed}),
    unhook('session.discarded',   {?MODULE, on_session_discarded}),
    unhook('session.takeovered',  {?MODULE, on_session_takeovered}),
    unhook('session.terminated',  {?MODULE, on_session_terminated}),
    unhook('message.publish',     {?MODULE, on_message_publish}),
    unhook('message.delivered',   {?MODULE, on_message_delivered}),
    unhook('message.acked',       {?MODULE, on_message_acked}),
    unhook('message.dropped',     {?MODULE, on_message_dropped}).

hook(HookPoint, MFA) ->
    %% use highest hook priority so this module's callbacks
    %% are evaluated before the default hooks in EMQX
    emqx_hooks:add(HookPoint, MFA).

unhook(HookPoint, MFA) ->
    emqx_hooks:del(HookPoint, MFA).
