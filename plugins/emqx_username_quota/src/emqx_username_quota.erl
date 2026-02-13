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

-include_lib("emqx_plugin_helper/include/emqx_hooks.hrl").

-define(AUTHN_HOOK, {?MODULE, on_client_authenticate, []}).
-define(SESSION_CREATED_HOOK, {?MODULE, on_session_created, []}).
-define(SESSION_RESUMED_HOOK, {?MODULE, on_session_resumed, []}).
-define(SESSION_DISCARDED_HOOK, {?MODULE, on_session_discarded, []}).
-define(SESSION_TERMINATED_HOOK, {?MODULE, on_session_terminated, []}).
-define(MAX_SESSIONS_PER_USERNAME, 100).
-define(SESSIONS_TAB, emqx_username_quota_sessions).
-define(COUNTS_TAB, emqx_username_quota_counts).

-spec hook() -> ok.
hook() ->
    ok = emqx_hooks:put('client.authenticate', ?AUTHN_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:put('session.created', ?SESSION_CREATED_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:put('session.resumed', ?SESSION_RESUMED_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:put('session.discarded', ?SESSION_DISCARDED_HOOK, ?HP_HIGHEST),
    emqx_hooks:put('session.terminated', ?SESSION_TERMINATED_HOOK, ?HP_HIGHEST).

-spec unhook() -> ok.
unhook() ->
    ok = emqx_hooks:del('client.authenticate', ?AUTHN_HOOK),
    ok = emqx_hooks:del('session.created', ?SESSION_CREATED_HOOK),
    ok = emqx_hooks:del('session.resumed', ?SESSION_RESUMED_HOOK),
    ok = emqx_hooks:del('session.discarded', ?SESSION_DISCARDED_HOOK),
    emqx_hooks:del('session.terminated', ?SESSION_TERMINATED_HOOK).

-spec on_client_authenticate(map(), term()) -> term().
on_client_authenticate(ClientInfo, DefaultResult) ->
    Username = to_bin(maps:get(username, ClientInfo, undefined)),
    ClientId = to_bin(maps:get(clientid, ClientInfo, undefined)),
    case Username =:= <<>> orelse ClientId =:= <<>> of
        true ->
            DefaultResult;
        false ->
            case
                is_registered(Username, ClientId) orelse
                    session_count(Username) < ?MAX_SESSIONS_PER_USERNAME
            of
                true -> DefaultResult;
                false -> {stop, {error, quota_exceeded}}
            end
    end.

-spec on_session_created(map(), term()) -> ok.
on_session_created(ClientInfo, _SessionInfo) ->
    maybe_register(ClientInfo).

-spec on_session_resumed(map(), term()) -> ok.
on_session_resumed(ClientInfo, _SessionInfo) ->
    maybe_register(ClientInfo).

-spec on_session_discarded(map(), term()) -> ok.
on_session_discarded(ClientInfo, _SessionInfo) ->
    maybe_unregister(ClientInfo).

-spec on_session_terminated(map(), term(), term()) -> ok.
on_session_terminated(ClientInfo, _Reason, _SessionInfo) ->
    maybe_unregister(ClientInfo).

-spec register_session(binary(), binary()) -> ok.
register_session(Username, ClientId) ->
    ensure_tables(),
    Key = {Username, ClientId},
    case ets:insert_new(?SESSIONS_TAB, {Key, true}) of
        true ->
            _ = ets:update_counter(?COUNTS_TAB, Username, {2, 1}, {Username, 0}),
            ok;
        false ->
            ok
    end.

-spec unregister_session(binary(), binary()) -> ok.
unregister_session(Username, ClientId) ->
    ensure_tables(),
    Key = {Username, ClientId},
    case ets:take(?SESSIONS_TAB, Key) of
        [] ->
            ok;
        [_] ->
            NewCount = ets:update_counter(?COUNTS_TAB, Username, {2, -1}, {Username, 0}),
            case NewCount =< 0 of
                true -> true = ets:delete(?COUNTS_TAB, Username);
                false -> ok
            end,
            ok
    end.

-spec session_count(binary()) -> non_neg_integer().
session_count(Username) ->
    ensure_tables(),
    case ets:lookup(?COUNTS_TAB, Username) of
        [{Username, Count}] when Count > 0 -> Count;
        _ -> 0
    end.

-spec reset() -> ok.
reset() ->
    ensure_tables(),
    true = ets:delete_all_objects(?SESSIONS_TAB),
    true = ets:delete_all_objects(?COUNTS_TAB),
    ok.

maybe_register(ClientInfo) ->
    Username = to_bin(maps:get(username, ClientInfo, undefined)),
    ClientId = to_bin(maps:get(clientid, ClientInfo, undefined)),
    case Username =:= <<>> orelse ClientId =:= <<>> of
        true -> ok;
        false -> register_session(Username, ClientId)
    end.

maybe_unregister(ClientInfo) ->
    Username = to_bin(maps:get(username, ClientInfo, undefined)),
    ClientId = to_bin(maps:get(clientid, ClientInfo, undefined)),
    case Username =:= <<>> orelse ClientId =:= <<>> of
        true -> ok;
        false -> unregister_session(Username, ClientId)
    end.

is_registered(Username, ClientId) ->
    ensure_tables(),
    ets:member(?SESSIONS_TAB, {Username, ClientId}).

to_bin(undefined) ->
    <<>>;
to_bin(Value) when is_binary(Value) ->
    Value;
to_bin(Value) when is_atom(Value) ->
    atom_to_binary(Value);
to_bin(Value) when is_list(Value) ->
    iolist_to_binary(Value).

ensure_tables() ->
    _ =
        case ets:info(?SESSIONS_TAB) of
            undefined ->
                ensure_table(?SESSIONS_TAB);
            _ ->
                ?SESSIONS_TAB
        end,
    _ =
        case ets:info(?COUNTS_TAB) of
            undefined ->
                ensure_table(?COUNTS_TAB);
            _ ->
                ?COUNTS_TAB
        end,
    ok.

ensure_table(Tab) ->
    try ets:new(Tab, [named_table, set, public, {read_concurrency, true}]) of
        _ -> Tab
    catch
        error:badarg -> Tab
    end.
