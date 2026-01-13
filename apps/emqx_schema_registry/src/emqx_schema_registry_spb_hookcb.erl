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
    on_message_publish/1,
    on_session_disconnected/2
]).

%%------------------------------------------------------------------------------
%% Type definitions
%%------------------------------------------------------------------------------

-include("emqx_schema_registry_internal_spb.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-define(MSG_PUBLISH_HOOK, {?MODULE, on_message_publish, []}).
-define(SESSION_DISCONNECTED_HOOK, {?MODULE, on_session_disconnected, []}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec register_hooks() -> ok.
register_hooks() ->
    ok = emqx_hooks:add('message.publish', ?MSG_PUBLISH_HOOK, ?HP_SCHEMA_REGISTRY_SPB),
    ok = emqx_hooks:add(
        'session.disconnected', ?SESSION_DISCONNECTED_HOOK, ?HP_SCHEMA_REGISTRY_SPB
    ),
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok = emqx_hooks:del('message.publish', ?MSG_PUBLISH_HOOK),
    ok = emqx_hooks:del('session.disconnected', ?SESSION_DISCONNECTED_HOOK),
    ok.

%%------------------------------------------------------------------------------
%% Hook callbacks
%%------------------------------------------------------------------------------

-spec on_message_publish(emqx_types:message()) -> ok.
on_message_publish(#message{} = Message) ->
    case emqx_schema_registry_config:is_alias_mapping_enabled() of
        true ->
            do_on_message_publish(Message);
        false ->
            ok
    end.

-spec on_session_disconnected(emqx_types:clientinfo(), emqx_types:infos()) -> ok.
on_session_disconnected(_ClientInfo, _SessionInfo) ->
    emqx_schema_registry_spb_state:delete_all_mappings_in_pd().

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

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
