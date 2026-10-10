%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_hookcb).

%% API
-export([
    register_hooks/0,
    unregister_hooks/0
]).

%% Hook callbacks
-export([
    on_message_publish/1,
    on_message_delivered/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_a2a_registry_internal.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-define(MESSAGE_PUBLISH_HOOK, {?MODULE, on_message_publish, []}).
-define(MESSAGE_DELIVERED_HOOK, {?MODULE, on_message_delivered, []}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

register_hooks() ->
    ok = emqx_hooks:add('message.publish', ?MESSAGE_PUBLISH_HOOK, ?HP_RETAINER + 1),
    ok = emqx_hooks:add('message.delivered', ?MESSAGE_DELIVERED_HOOK, ?HP_RETAINER - 1),
    ok.

unregister_hooks() ->
    ok = emqx_hooks:del('message.publish', ?MESSAGE_PUBLISH_HOOK),
    ok = emqx_hooks:del('message.delivered', ?MESSAGE_DELIVERED_HOOK),
    ok.

%%------------------------------------------------------------------------------
%% Hook callbacks
%%------------------------------------------------------------------------------

on_message_publish(Msg = #message{}) ->
    case emqx_a2a_registry_config:is_enabled() of
        false ->
            {ok, Msg};
        true ->
            do_on_message_publish(Msg)
    end.

do_on_message_publish(Msg = #message{topic = Topic, flags = Flags}) ->
    IsRetained = maps:get(retain, Flags, false),
    case parse_a2a_discovery_topic(Topic) of
        error ->
            {ok, Msg};
        {ok, _Namespace, Id} when IsRetained ->
            validate_card_message(Msg, Id);
        {ok, _Namespace, Id} ->
            ?tp(warning, "a2a_registry_non_retained_card_message", #{
                parsed_id => Id
            }),
            #message{headers = Headers} = Msg,
            {stop, Msg#message{headers = Headers#{allow_publish => false}}}
    end.

on_message_delivered(_ClientInfo, #message{} = Msg0) ->
    case emqx_a2a_registry_config:is_enabled() of
        false ->
            {ok, Msg0};
        true ->
            do_on_message_delivered(Msg0)
    end.

do_on_message_delivered(Msg0) ->
    Msg = maybe_augment_message_metadata(Msg0),
    {ok, Msg}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

parse_a2a_discovery_topic(Topic) ->
    emqx_a2a_registry_utils:parse_a2a_discovery_topic(Topic).

validate_card_message(Msg, Id) ->
    maybe
        ok ?= validate_id(Id),
        ok ?= validate_publishing_agent_clientid(Msg, Id),
        ok ?= validate_card_schema(Msg),
        ?tp(debug, "a2a_registry_writing_card", #{id => Id}),
        {ok, Msg}
    else
        {error, Reason} ->
            ?tp(warning, "a2a_registry_invalid_card_message", #{
                reason => Reason,
                parsed_id => Id
            }),
            #message{headers = Headers} = Msg,
            {stop, Msg#message{headers = Headers#{allow_publish => false}}}
    end.

validate_id(Id) ->
    {OrgId, UnitId, AgentId} = Id,
    emqx_a2a_registry_utils:validate_id(OrgId, UnitId, AgentId).

validate_publishing_agent_clientid(#message{from = From} = _Msg, Id) ->
    {OrgId, UnitId, AgentId} = Id,
    case emqx_topic:words(From) of
        [OrgId, UnitId, AgentId] ->
            ok;
        _ ->
            {error, {bad_clientid, From, Id}}
    end.

validate_card_schema(#message{payload = <<"">>} = _Msg) ->
    %% deleting retained message
    ok;
validate_card_schema(#message{payload = Payload} = _Msg) ->
    emqx_a2a_registry_utils:validate_card_schema(Payload).

maybe_augment_message_metadata(#message{topic = Topic} = Msg0) ->
    case parse_a2a_discovery_topic(Topic) of
        {ok, _Namespace, _Id} ->
            augment_message_metadata(Msg0);
        error ->
            Msg0
    end.

augment_message_metadata(Msg0) ->
    ClientId = emqx_message:from(Msg0),
    Props0 = emqx_message:get_header(properties, Msg0, #{}),
    UserProperties0 = maps:get('User-Property', Props0, []),
    UserProperties1 = proplists:delete(?A2A_PROP_STATUS_KEY, UserProperties0),
    UserProperties2 = proplists:delete(?A2A_PROP_STATUS_SOURCE_KEY, UserProperties1),
    Status = emqx_a2a_registry:lookup_agent_status(ClientId),
    UserProperties = [
        {?A2A_PROP_STATUS_KEY, Status},
        {?A2A_PROP_STATUS_SOURCE_KEY, <<"emqx">>}
        | UserProperties2
    ],
    Props = maps:put('User-Property', UserProperties, Props0),
    emqx_message:set_header(properties, Props, Msg0).
