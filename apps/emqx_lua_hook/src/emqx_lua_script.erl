%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_lua_script).

-include("emqx_lua_hook.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-export([ register_on_message_publish/2
        , register_on_client_connected/2
        , register_on_client_disconnected/2
        , register_on_client_subscribe/2
        , register_on_client_unsubscribe/2
        , register_on_message_acked/2
        , register_on_message_delivered/2
        , register_on_session_subscribed/2
        , register_on_session_unsubscribed/2
        , register_on_client_authenticate/2
        , register_on_client_check_acl/2
        , unregister_hooks/1
        ]).

-export([ on_client_connected/4
        , on_client_disconnected/5
        , on_client_authenticate/4
        , on_client_check_acl/6
        , on_client_subscribe/5
        , on_client_unsubscribe/5
        , on_session_subscribed/5
        , on_session_unsubscribed/5
        , on_message_publish/3
        , on_message_delivered/4
        , on_message_acked/4
        ]).

-define(EMPTY_USERNAME, <<"">>).

-define(HOOK_ADD(A, B),      emqx:hook(A, B)).
-define(HOOK_DEL(A, B),      emqx:unhook(A, B)).

register_on_client_connected(ScriptName, LuaState) ->
    ?HOOK_ADD('client.connected', {?MODULE, on_client_connected, [ScriptName, LuaState]}).

register_on_client_disconnected(ScriptName, LuaState) ->
    ?HOOK_ADD('client.disconnected', {?MODULE, on_client_disconnected, [ScriptName, LuaState]}).

register_on_client_authenticate(ScriptName, LuaState) ->
    ?HOOK_ADD('client.authenticate', {?MODULE, on_client_authenticate, [ScriptName, LuaState]}).

register_on_client_check_acl(ScriptName, LuaState) ->
    ?HOOK_ADD('client.check_acl', {?MODULE, on_client_check_acl, [ScriptName, LuaState]}).

register_on_client_subscribe(ScriptName, LuaState) ->
    ?HOOK_ADD('client.subscribe', {?MODULE, on_client_subscribe, [ScriptName, LuaState]}).

register_on_client_unsubscribe(ScriptName, LuaState) ->
    ?HOOK_ADD('client.unsubscribe', {?MODULE, on_client_unsubscribe, [ScriptName, LuaState]}).

register_on_session_subscribed(ScriptName, LuaState) ->
    ?HOOK_ADD('session.subscribed', {?MODULE, on_session_subscribed, [ScriptName, LuaState]}).

register_on_session_unsubscribed(ScriptName, LuaState) ->
    ?HOOK_ADD('session.unsubscribed', {?MODULE, on_session_unsubscribed, [ScriptName, LuaState]}).

register_on_message_publish(ScriptName, LuaState) ->
    ?HOOK_ADD('message.publish', {?MODULE, on_message_publish, [ScriptName, LuaState]}).

register_on_message_delivered(ScriptName, LuaState) ->
    ?HOOK_ADD('message.delivered', {?MODULE, on_message_delivered, [ScriptName, LuaState]}).

register_on_message_acked(ScriptName, LuaState) ->
    ?HOOK_ADD('message.acked', {?MODULE, on_message_acked, [ScriptName, LuaState]}).

unregister_hooks({ScriptName, LuaState}) ->
    ?HOOK_DEL('client.connected',     {?MODULE, on_client_connected,     [ScriptName, LuaState]}),
    ?HOOK_DEL('client.disconnected',  {?MODULE, on_client_disconnected,  [ScriptName, LuaState]}),
    ?HOOK_DEL('client.authenticate',  {?MODULE, on_client_authenticate,  [ScriptName, LuaState]}),
    ?HOOK_DEL('client.check_acl',     {?MODULE, on_client_check_acl,     [ScriptName, LuaState]}),
    ?HOOK_DEL('client.subscribe',     {?MODULE, on_client_subscribe,     [ScriptName, LuaState]}),
    ?HOOK_DEL('client.unsubscribe',   {?MODULE, on_client_unsubscribe,   [ScriptName, LuaState]}),
    ?HOOK_DEL('session.subscribed',   {?MODULE, on_session_subscribed,   [ScriptName, LuaState]}),
    ?HOOK_DEL('session.unsubscribed', {?MODULE, on_session_unsubscribed, [ScriptName, LuaState]}),
    ?HOOK_DEL('message.publish',      {?MODULE, on_message_publish,      [ScriptName, LuaState]}),
    ?HOOK_DEL('message.delivered',    {?MODULE, on_message_delivered,    [ScriptName, LuaState]}),
    ?HOOK_DEL('message.acked',        {?MODULE, on_message_acked,        [ScriptName, LuaState]}).

on_client_connected(ClientInfo = #{clientid := ClientId, username := Username},
                    ConnInfo, _ScriptName, LuaState) ->
    ?LOG(debug, "Client(~s) connected, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
                [ClientId, ClientInfo, ConnInfo]),
    case catch luerl:call_function([on_client_connected], [ClientId, Username], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_client_connected(), which has syntax error, St=~p", [St]),
            ok;
        {_Result, _St} ->
            ok;
        Other ->
            ?LOG(error, "Lua function on_client_connected() caught exception, ~p", [Other]),
            ok
    end.

on_client_disconnected(ClientInfo = #{clientid := ClientId, username := Username},
                       ReasonCode, ConnInfo, _ScriptName, LuaState) ->
    ?LOG(debug, "Client(~s) disconnected due to ~p, ClientInfo:~n~p~n, ConnInfo:~n~p~n",
                [ClientId, ReasonCode, ClientInfo, ConnInfo]),
    case catch luerl:call_function([on_client_disconnected], [ClientId, Username, ReasonCode], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_client_disconnected(), which has syntax error, St=~p", [St]),
            ok;
        {_Result, _St} ->
            ok;
        Other ->
            ?LOG(error, "Lua function on_client_disconnected() caught exception, ~p", [Other]),
            ok
    end.

on_client_authenticate(#{clientid := ClientId,
                         username := Username,
                         peerhost := Peerhost,
                         password := Password}, Result, _ScriptName, LuaState) ->
    case catch luerl:call_function([on_client_authenticate],
                                   [ClientId, Username, inet:ntoa(Peerhost), Password], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_client_authenticate(), which has syntax error, St=~p", [St]),
            ok;
        {[<<"ignore">>], _St} ->
            ok;
        {[<<"ok">>], _St} ->
            {stop, Result#{auth_result => success}};
        Other ->
            ?LOG(error, "Lua function on_client_authenticate() caught exception, ~p", [Other]),
            ok
    end.

on_client_check_acl(#{clientid := ClientId,
                      username := Username,
                      peerhost := Peerhost,
                      password := Password}, Topic, PubSub, _Result, _ScriptName, LuaState) ->
    case catch luerl:call_function([on_client_check_acl], [ClientId, Username, inet:ntoa(Peerhost), Password, Topic, PubSub], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_client_check_acl(), which has syntax error, St=~p", [St]),
            ok;
        {[<<"ignore">>],_St} ->
            ok;
        {[<<"allow">>], _St} ->
            {stop, allow};
        {[<<"deny">>], _St} ->
            {stop, deny};
        Other ->
            ?LOG(error, "Lua function on_client_check_acl() caught exception, ~p", [Other]),
            ok
    end.

on_client_subscribe(#{clientid := ClientId, username := Username}, _Properties, TopicFilters, _ScriptName, LuaState) ->
    NewTopicFilters =
        lists:foldr(fun(TopicFilter, Acc) ->
                        case on_client_subscribe_single(ClientId, Username, TopicFilter, LuaState) of
                            false ->
                                {Topic, Opts} = TopicFilter,
                                [{Topic, Opts#{deny_subscription => true}} | Acc];
                            NewTopicFilter ->
                                [NewTopicFilter | Acc]
                        end
                    end, [], TopicFilters),
    case NewTopicFilters of
        [] -> stop;
        _ -> {ok, NewTopicFilters}
    end.

on_client_subscribe_single(_ClientId, _Username, TopicFilter = {<<$$, _Rest/binary>>, _SubOpts}, _LuaState) ->
    %% ignore topics starting with $
    TopicFilter;
on_client_subscribe_single(ClientId, Username, TopicFilter = {Topic, SubOpts}, LuaState) ->
    ?LOG(debug, "hook client(~s/~s) will subscribe: ~p~n", [ClientId, Username, Topic]),
    case catch luerl:call_function([on_client_subscribe], [ClientId, Username, Topic], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_client_subscribe(), which has syntax error, St=~p", [St]),
            TopicFilter;
        {[false], _St} ->
            false;   % cancel this topic's subscription
        {[NewTopic], _St} ->
            ?LOG(debug, "LUA function on_client_subscribe() return ~p", [NewTopic]),
            {NewTopic, SubOpts};  % modify topic
        Other ->
            ?LOG(error, "Lua function on_client_subscribe() caught exception, ~p", [Other]),
            TopicFilter
    end.

on_client_unsubscribe(#{clientid := ClientId, username := Username}, _Properties, TopicFilters, _ScriptName, LuaState) ->
    NewTopicFilters =
        lists:foldr(fun(TopicFilter, Acc) ->
                        case on_client_unsubscribe_single(ClientId, Username, TopicFilter, LuaState) of
                            false -> Acc;
                            NewTopicFilter -> [NewTopicFilter | Acc]
                        end
                    end, [], TopicFilters),
    case NewTopicFilters of
        [] -> stop;
        _ -> {ok, NewTopicFilters}
    end.

on_client_unsubscribe_single(_ClientId, _Username, TopicFilter = {<<$$, _Rest/binary>>, _SubOpts}, _LuaState) ->
    %% ignore topics starting with $
    TopicFilter;
on_client_unsubscribe_single(ClientId, Username, TopicFilter = {Topic, SubOpts}, LuaState) ->
    ?LOG(debug, "hook client(~s/~s) unsubscribe ~p~n", [ClientId, Username, Topic]),
    case catch luerl:call_function([on_client_unsubscribe], [ClientId, Username, Topic], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_client_unsubscribe(), which has syntax error, St=~p", [St]),
            TopicFilter;
        {[false], _St} ->
            false;   % cancel this topic's unsubscription
        {[NewTopic], _} ->
            ?LOG(debug, "Lua function on_client_unsubscribe() return ~p", [NewTopic]),
            {NewTopic, SubOpts};  % modify topic
        Other ->
            ?LOG(error, "Topic=~p, lua function on_client_unsubscribe() caught exception, ~p", [Topic, Other]),
            TopicFilter
    end.

on_session_subscribed(#{}, <<$$, _Rest/binary>>, _SubOpts, _ScriptName, _LuaState) ->
    %% ignore topics starting with $
    ok;
on_session_subscribed(#{clientid := ClientId, username := Username},
                      Topic, SubOpts, _ScriptName, LuaState) ->
    ?LOG(debug, "Session(~s/s) subscribed ~s with subopts: ~p~n", [ClientId, Username, Topic, SubOpts]),
    case catch luerl:call_function([on_session_subscribed], [ClientId, Username, Topic], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_session_subscribed(), which has syntax error, St=~p", [St]),
            ok;
        {_Result, _St} ->
            ok;
        Other ->
            ?LOG(error, "Topic=~p, lua function on_session_subscribed() caught exception, ~p", [Topic, Other]),
            ok
    end.

on_session_unsubscribed(#{}, <<$$, _Rest/binary>>, _SubOpts, _ScriptName, _LuaState) ->
    %% ignore topics starting with $
    ok;
on_session_unsubscribed(#{clientid := ClientId, username := Username},
                        Topic, _SubOpts, _ScriptName, LuaState) ->
    ?LOG(debug, "Session(~s/~s) unsubscribed ~s~n", [ClientId, Username, Topic]),
    case catch luerl:call_function([on_session_unsubscribed], [ClientId, Username, Topic], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_session_unsubscribed(), which has syntax error, St=~p", [St]),
            ok;
        {_Result, _St} ->
            ok;
        Other ->
            ?LOG(error, "Topic=~p, lua function on_session_unsubscribed() caught exception, ~p", [Topic, Other]),
            ok
    end.

on_message_publish(Message = #message{topic = <<$$, _Rest/binary>>}, _ScriptName, _LuaState) ->
    %% ignore topics starting with $
    {ok, Message};
on_message_publish(Message = #message{from = ClientId,
                                      qos = QoS,
                                      flags = Flags = #{retain := Retain},
                                      topic = Topic,
                                      payload = Payload,
                                      headers = Headers},
                   _ScriptName, LuaState) ->
    Username = maps:get(username, Headers, ?EMPTY_USERNAME),
    ?LOG(debug, "Publish ~s~n", [emqx_message:format(Message)]),
    case catch luerl:call_function([on_message_publish], [ClientId, Username, Topic, Payload, QoS, Retain], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_message_publish(), which has syntax error, St=~p", [St]),
            {ok, Message};
        {[false], _St} ->
            ?LOG(debug, "Lua function on_message_publish() returned false, setting allow_publish header to false", []),
            {stop, Message#message{headers = Headers#{allow_publish => false}}};
        {[NewTopic, NewPayload, NewQos, NewRetain], _St} ->
            ?LOG(debug, "Lua function on_message_publish() returned ~p", [{NewTopic, NewPayload, NewQos, NewRetain}]),
            {ok, Message#message{topic = NewTopic, payload = NewPayload,
                                 qos = round(NewQos), flags = Flags#{retain => to_retain(NewRetain)}}};
        Other ->
            ?LOG(error, "Topic=~p, lua function on_message_publish() caught exception, ~p", [Topic, Other]),
            {ok, Message}
    end.

on_message_delivered(#{}, #message{topic = <<$$, _Rest/binary>>}, _ScriptName, _LuaState) ->
    %% ignore topics starting with $
    ok;
on_message_delivered(#{clientid := ClientId, username := Username},
                   Message = #message{topic = Topic, payload = Payload, qos = QoS, flags = Flags = #{retain := Retain}},
                   _ScriptName, LuaState) ->
    ?LOG(debug, "Message delivered to client(~s): ~s~n",
                [ClientId, emqx_message:format(Message)]),
    case catch luerl:call_function([on_message_delivered], [ClientId, Username, Topic, Payload, QoS, Retain], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_message_delivered(), which has syntax error, St=~p", [St]),
            ok;
        {[false], _St} ->
            ok;
        {[NewTopic, NewPayload, NewQos, NewRetain], _St} ->
            {ok, Message#message{topic = NewTopic, payload = NewPayload,
                                 qos = round(NewQos), flags = Flags#{retain => to_retain(NewRetain)}}};
        Other ->
            ?LOG(error, "Topic=~p, lua function on_message_delivered() caught exception, ~p", [Topic, Other]),
            ok
    end.

on_message_acked(#{}, #message{topic = <<$$, _Rest/binary>>}, _ScriptName, _LuaState) ->
    %% ignore topics starting with $
    ok;
on_message_acked(#{clientid := ClientId, username := Username},
                 Message = #message{topic = Topic, payload = Payload, qos = QoS, flags = #{retain := Retain}}, _ScriptName, LuaState) ->
    ?LOG(debug, "Message acked by client(~s): ~s~n",
                [ClientId, emqx_message:format(Message)]),
    case catch luerl:call_function([on_message_acked], [ClientId, Username, Topic, Payload, QoS, Retain], LuaState) of
        {'EXIT', St} ->
            ?LOG(error, "Failed to execute function on_message_acked(), which has syntax error, St=~p", [St]),
            ok;
        {_Result, _St} ->
            ok;
        Other ->
            ?LOG(error, "Topic=~p, lua function on_message_acked() caught exception, ~p", [Topic, Other]),
            ok
    end.

to_retain(0) -> false;
to_retain(1) -> true;
to_retain("true") -> true;
to_retain("false") -> false;
to_retain(<<"true">>) -> true;
to_retain(<<"false">>) -> false;
to_retain(true) -> true;
to_retain(false) -> false;
to_retain(Num) when is_float(Num) ->
    case round(Num) of 0 -> false; _ -> true end.
