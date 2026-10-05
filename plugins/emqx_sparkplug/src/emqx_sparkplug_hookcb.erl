%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_sparkplug_hookcb).

%% API
-export([
    register_hooks/0,
    unregister_hooks/0
]).

%% Hook callbacks
-export([on_message_publish/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_schema_registry/include/emqx_schema_registry_internal_spb.hrl").

-define(MSG_PUBLISH_HOOK, {?MODULE, on_message_publish, []}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec register_hooks() -> ok.
register_hooks() ->
    ok = emqx_hooks:add('message.publish', ?MSG_PUBLISH_HOOK, ?HP_LOWEST),
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok = emqx_hooks:del('message.publish', ?MSG_PUBLISH_HOOK),
    ok.

%%------------------------------------------------------------------------------
%% Hook callbacks
%%------------------------------------------------------------------------------

-spec on_message_publish(emqx_types:message()) -> ok.
on_message_publish(#message{} = Message) ->
    case is_client_process() of
        true ->
            do_on_message_publish(Message);
        false ->
            ok
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

is_client_process() ->
    case proc_lib:get_label(self()) of
        {clientid, _} -> true;
        _ -> false
    end.

do_on_message_publish(#message{topic = Topic} = Message) ->
    case emqx_schema_registry_spb_state:parse_spb_topic(Topic) of
        {ok, #nbirth{} = BirthMsg} ->
            emqx_sparkplug:publish_nbirth(BirthMsg, Message);
        {ok, #dbirth{} = BirthMsg} ->
            emqx_sparkplug:publish_dbirth(BirthMsg, Message);
        _ ->
            ok
    end.
