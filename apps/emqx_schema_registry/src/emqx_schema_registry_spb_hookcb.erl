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
    on_message_publish/1
]).

%%------------------------------------------------------------------------------
%% Type definitions
%%------------------------------------------------------------------------------

-include("emqx_schema_registry_internal_spb.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-define(MSG_PUBLISH_HOOK, {?MODULE, on_message_publish, []}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec register_hooks() -> ok.
register_hooks() ->
    ok = emqx_hooks:add('message.publish', ?MSG_PUBLISH_HOOK, ?HP_SCHEMA_REGISTRY_SPB),
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok = emqx_hooks:del('message.publish', ?MSG_PUBLISH_HOOK),
    ok.

%%------------------------------------------------------------------------------
%% Hook callbacks
%%------------------------------------------------------------------------------

-spec on_message_publish(emqx_types:message()) -> ok.
on_message_publish(#message{topic = Topic} = Message) ->
    case
        is_spb_message_topic(Topic) andalso
            emqx_schema_registry_config:is_alias_mapping_enabled() andalso
            is_client_process()
    of
        true ->
            do_on_message_publish(Message);
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

%% True when the third topic level is one that `parse_spb_topic/1' accepts and a fourth
%% level follows. This is a cheap pre-check that never rejects a topic the parser accepts.
is_spb_message_topic(Topic) ->
    is_spb_message_topic(Topic, 0).

is_spb_message_topic(<<"/", Rest/binary>>, 1) ->
    is_spb_message_type(Rest);
is_spb_message_topic(<<"/", Rest/binary>>, Level) ->
    is_spb_message_topic(Rest, Level + 1);
is_spb_message_topic(<<_, Rest/binary>>, Level) ->
    is_spb_message_topic(Rest, Level);
is_spb_message_topic(<<>>, _Level) ->
    false.

is_spb_message_type(<<"NBIRTH/", _/binary>>) -> true;
is_spb_message_type(<<"NDATA/", _/binary>>) -> true;
is_spb_message_type(<<"DBIRTH/", _/binary>>) -> true;
is_spb_message_type(<<"DDATA/", _/binary>>) -> true;
is_spb_message_type(_) -> false.

do_on_message_publish(#message{topic = Topic} = Message) ->
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
