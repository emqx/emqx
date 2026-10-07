%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_spb_hookcb).

%% API
-export([
    register_hooks/0,
    unregister_hooks/0
]).

%% Hook callbacks
-export([
    on_message_publish_alias_mapping/1,
    on_message_publish_spb_aware/1
]).

%%------------------------------------------------------------------------------
%% Type definitions
%%------------------------------------------------------------------------------

-include("emqx_schema_registry_internal_spb.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-define(ALIAS_HOOK, {?MODULE, on_message_publish_alias_mapping, []}).
-define(SPB_AWARE_HOOK, {?MODULE, on_message_publish_spb_aware, []}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec register_hooks() -> ok.
register_hooks() ->
    ok = emqx_hooks:add('message.publish', ?ALIAS_HOOK, ?HP_SCHEMA_REGISTRY_SPB),
    ok = emqx_hooks:add('message.publish', ?SPB_AWARE_HOOK, ?HP_LOWEST),
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok = emqx_hooks:del('message.publish', ?ALIAS_HOOK),
    ok = emqx_hooks:del('message.publish', ?SPB_AWARE_HOOK),
    ok.

%%------------------------------------------------------------------------------
%% Hook callbacks
%%------------------------------------------------------------------------------

-spec on_message_publish_alias_mapping(emqx_types:message()) -> ok.
on_message_publish_alias_mapping(#message{} = Message) ->
    case emqx_schema_registry_config:is_alias_mapping_enabled() andalso is_client_process() of
        true ->
            do_on_message_publish_alias_mapping(Message);
        false ->
            ok
    end.

-spec on_message_publish_spb_aware(emqx_types:message()) -> ok.
on_message_publish_spb_aware(#message{} = Message) ->
    case emqx_schema_registry_config:is_spb_awareness_enabled() andalso is_client_process() of
        true ->
            do_on_message_publish_spb_aware(Message);
        false ->
            ok
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

%% Alias mappings are per-publisher state kept in the process dictionary; only maintain
%% them when the hook runs in an MQTT client's own channel process, identified by the
%% `{clientid, _}' proc label set in `emqx_logger:set_metadata_clientid/1'.
%% Bridge/delayed/system publishes share a process across publishers and must not
%% populate or read the cache.
is_client_process() ->
    case proc_lib:get_label(self()) of
        {clientid, _} -> true;
        _ -> false
    end.

do_on_message_publish_alias_mapping(#message{} = Message) ->
    Topic = unmounted_topic(Message),
    case emqx_schema_registry_spb_state:parse_spb_topic(Topic) of
        {ok, #nbirth{} = BirthMsg} ->
            emqx_schema_registry_spb_state:register_aliases(Message, BirthMsg);
        {ok, #dbirth{} = BirthMsg} ->
            emqx_schema_registry_spb_state:register_aliases(Message, BirthMsg);
        {ok, #ndata{} = DataMsg} ->
            emqx_schema_registry_spb_state:load_aliases(DataMsg);
        {ok, #ddata{} = DataMsg} ->
            emqx_schema_registry_spb_state:load_aliases(DataMsg);
        _ ->
            ok
    end.

do_on_message_publish_spb_aware(#message{} = Message) ->
    Topic = unmounted_topic(Message),
    case emqx_schema_registry_spb_state:parse_spb_topic(Topic) of
        {ok, #nbirth{} = BirthMsg} ->
            emqx_schema_registry_spb_state:publish_birth_msg(Message, BirthMsg);
        {ok, #dbirth{} = BirthMsg} ->
            emqx_schema_registry_spb_state:publish_birth_msg(Message, BirthMsg);
        _ ->
            ok
    end.

unmounted_topic(#message{topic = Topic0, extra = #{mountpoint := Mountpoint}}) ->
    emqx_mountpoint:unmount(Mountpoint, Topic0);
unmounted_topic(#message{topic = Topic}) ->
    Topic.
