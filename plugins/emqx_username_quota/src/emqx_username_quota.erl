%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota).

-export([
    hook/0,
    unhook/0,
    on_client_authenticate/2,
    on_session_created/2,
    on_session_resumed/2,
    on_session_discarded/2,
    on_session_terminated/3,
    register_session/2,
    unregister_session/2,
    session_count/1,
    reset/0
]).

-include_lib("emqx/include/emqx_hooks.hrl").

-define(AUTHN_HOOK, {?MODULE, on_client_authenticate, []}).
-define(SESSION_CREATED_HOOK, {?MODULE, on_session_created, []}).
-define(SESSION_RESUMED_HOOK, {?MODULE, on_session_resumed, []}).
-define(SESSION_DISCARDED_HOOK, {?MODULE, on_session_discarded, []}).
-define(SESSION_TERMINATED_HOOK, {?MODULE, on_session_terminated, []}).
-define(INVALID_USER_OR_CLIENT(Username, ClientId), (Username =:= <<>> orelse ClientId =:= <<>>)).

hook() ->
    ok = emqx_hooks:put('client.authenticate', ?AUTHN_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:put('session.created', ?SESSION_CREATED_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:put('session.resumed', ?SESSION_RESUMED_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:put('session.discarded', ?SESSION_DISCARDED_HOOK, ?HP_HIGHEST),
    emqx_hooks:put('session.terminated', ?SESSION_TERMINATED_HOOK, ?HP_HIGHEST).

unhook() ->
    ok = emqx_hooks:del('client.authenticate', ?AUTHN_HOOK),
    ok = emqx_hooks:del('session.created', ?SESSION_CREATED_HOOK),
    ok = emqx_hooks:del('session.resumed', ?SESSION_RESUMED_HOOK),
    ok = emqx_hooks:del('session.discarded', ?SESSION_DISCARDED_HOOK),
    emqx_hooks:del('session.terminated', ?SESSION_TERMINATED_HOOK).

on_client_authenticate(ClientInfo, DefaultResult) ->
    Username = username(ClientInfo),
    ClientId = clientid(ClientInfo),
    case ?INVALID_USER_OR_CLIENT(Username, ClientId) of
        true ->
            DefaultResult;
        false ->
            case should_allow(Username, ClientId) of
                true -> DefaultResult;
                false -> {stop, {error, quota_exceeded}}
            end
    end.

on_session_created(ClientInfo, _SessionInfo) ->
    maybe_register(ClientInfo).

on_session_resumed(ClientInfo, _SessionInfo) ->
    maybe_register(ClientInfo).

on_session_discarded(ClientInfo, _SessionInfo) ->
    maybe_unregister(ClientInfo).

on_session_terminated(ClientInfo, _Reason, _SessionInfo) ->
    maybe_unregister(ClientInfo).

register_session(Username, ClientId) ->
    _ = emqx_username_quota_state:add(Username, ClientId, self()),
    ok.

unregister_session(Username, ClientId) ->
    emqx_username_quota_state:del_client(Username, ClientId).

session_count(Username) ->
    emqx_username_quota_state:count(Username).

reset() ->
    emqx_username_quota_state:reset().

should_allow(Username, ClientId) ->
    case emqx_username_quota_state:is_known_client(Username, ClientId) of
        {true, _Node} ->
            true;
        false ->
            Max = emqx_username_quota_state:get_effective_limit(Username),
            Count = emqx_username_quota_state:count(Username),
            Max =:= nolimit orelse Count < Max
    end.

maybe_register(ClientInfo) ->
    Username = username(ClientInfo),
    ClientId = clientid(ClientInfo),
    case ?INVALID_USER_OR_CLIENT(Username, ClientId) of
        true -> ok;
        false -> emqx_username_quota_pool:add(Username, ClientId, self())
    end.

maybe_unregister(ClientInfo) ->
    Username = username(ClientInfo),
    ClientId = clientid(ClientInfo),
    case ?INVALID_USER_OR_CLIENT(Username, ClientId) of
        true -> ok;
        false -> emqx_username_quota_state:del_client(Username, ClientId)
    end.

username(ClientInfo) ->
    emqx_utils_conv:bin(maps:get(username, ClientInfo, <<>>)).

clientid(ClientInfo) ->
    emqx_utils_conv:bin(maps:get(clientid, ClientInfo, <<>>)).
