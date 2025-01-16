%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_access_control).

-include("emqx.hrl").
-include("emqx_access_control.hrl").
-include("logger.hrl").

-export([
    authenticate/1,
    authorize/3,
    format_action/1
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
%% TEST
-endif.

-define(TRACE_RESULT(Label, Result, Reason), begin
    ?TRACE(Label, ?AUTHN_TRACE_TAG, #{
        result => (Result),
        reason => (Reason)
    }),
    Result
end).

-define(DEFAULT_AUTH_RESULT_PT_KEY, {?MODULE, default_authn_result}).

-ifdef(TEST).
-define(DEFAULT_AUTH_RESULT, ok).
-else.
-define(DEFAULT_AUTH_RESULT, ignore).
-endif.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec authenticate(emqx_types:clientinfo()) ->
    {ok, map()}
    | {ok, map(), binary()}
    | {continue, map()}
    | {continue, binary(), map()}
    | {error, not_authorized}.
authenticate(Credential) ->
    %% pre-hook quick authentication or
    %% if auth backend returning nothing but just 'ok'
    %% it means it's not a superuser, or there is no way to tell.
    NotSuperUser = #{is_superuser => false},
    case pre_hook_authenticate(Credential) of
        ok ->
            on_authentication_complete_success(Credential, NotSuperUser, anonymous),
            {ok, NotSuperUser};
        continue ->
            case run_hooks('client.authenticate', [Credential], default_authn_result()) of
                ignore ->
                    on_authentication_complete_no_hooks(Credential, NotSuperUser);
                ok ->
                    on_authentication_complete_success(Credential, NotSuperUser, ok),
                    {ok, NotSuperUser};
                {ok, AuthResult} = OkResult ->
                    on_authentication_complete_success(Credential, AuthResult, ok),
                    OkResult;
                {ok, AuthResult, _AuthData} = OkResult ->
                    on_authentication_complete_success(Credential, AuthResult, ok),
                    OkResult;
                {error, Reason} = Error ->
                    on_authentication_complete_error(Credential, Reason),
                    Error;
                {continue, _AuthCache} = IncompleteResult ->
                    IncompleteResult;
                {continue, _AuthData, _AuthCache} = IncompleteResult ->
                    IncompleteResult;
                Other ->
                    ?SLOG(error, #{
                        msg => "unknown_authentication_result_format",
                        result => Other
                    }),
                    on_authentication_complete_error(Credential, not_authorized),
                    {error, not_authorized}
            end;
        {error, Reason} = Error ->
            on_authentication_complete_error(Credential, Reason),
            Error
    end.

%% @doc Check Authorization
-spec authorize(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic()) ->
    allow | deny.
authorize(ClientInfo, Action, <<"$delayed/", Data/binary>> = RawTopic) ->
    case binary:split(Data, <<"/">>) of
        [_, Topic] ->
            authorize(ClientInfo, Action, Topic);
        _ ->
            ?SLOG(warning, #{
                msg => "invalid_delayed_topic_format",
                expected_example => "$delayed/1/t/foo",
                got => RawTopic
            }),
            inc_authz_metrics(deny),
            deny
    end;
authorize(ClientInfo, Action, Topic) ->
    Result =
        case emqx_authz_cache:is_enabled(Topic) of
            true -> check_authorization_cache(ClientInfo, Action, Topic);
            false -> do_authorize(ClientInfo, Action, Topic)
        end,
    inc_authz_metrics(Result),
    Result.

%% @doc Get default authentication result.
%% The default result is used when none of the authentication hooks
%% handled the authentication.
%%
%% In a release, only the restrictive result is used.
%% That is, if authn is enabled for a listener and there is no
%% result from the hooks (or no hooks) then we deny the connection.
%%
%% In tests, we use the permissive result to avoid setting up
%% authentication for numerous tests.

-spec default_authn_result() -> ok | ignore.
default_authn_result() ->
    persistent_term:get(?DEFAULT_AUTH_RESULT_PT_KEY, ?DEFAULT_AUTH_RESULT).

-ifdef(TEST).

-spec set_default_authn_permissive() -> ok.
set_default_authn_permissive() ->
    set_default_auth_result(ok).

-spec set_default_authn_restrictive() -> ok.
set_default_authn_restrictive() ->
    set_default_auth_result(ignore).

set_default_auth_result(Result) ->
    _ = persistent_term:put(?DEFAULT_AUTH_RESULT_PT_KEY, Result),
    ok.

%% TEST
-endif.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

-spec pre_hook_authenticate(emqx_types:clientinfo()) ->
    ok | continue | {error, not_authorized}.
pre_hook_authenticate(#{enable_authn := false}) ->
    ?TRACE_RESULT("pre_hook_authenticate", ok, enable_authn_false);
pre_hook_authenticate(#{enable_authn := quick_deny_anonymous} = Credential) ->
    case is_username_defined(Credential) of
        true ->
            continue;
        false ->
            ?TRACE_RESULT("pre_hook_authenticate", {error, not_authorized}, enable_authn_false)
    end;
pre_hook_authenticate(_) ->
    continue.

is_username_defined(#{username := undefined}) -> false;
is_username_defined(#{username := <<>>}) -> false;
is_username_defined(#{username := _Username}) -> true;
is_username_defined(_) -> false.

check_authorization_cache(ClientInfo, Action, Topic) ->
    case emqx_authz_cache:get_authz_cache(Action, Topic) of
        not_found ->
            inc_authz_metrics(cache_miss),
            AuthzResult = do_authorize(ClientInfo, Action, Topic),
            emqx_authz_cache:put_authz_cache(Action, Topic, AuthzResult),
            AuthzResult;
        AuthzResult ->
            emqx_hooks:run(
                'client.check_authz_complete',
                [ClientInfo, Action, Topic, AuthzResult, cache]
            ),
            inc_authz_metrics(cache_hit),
            AuthzResult
    end.

do_authorize(ClientInfo, Action, Topic) ->
    NoMatch = emqx:get_config([authorization, no_match], allow),
    Default = #{result => NoMatch, from => default},
    case run_hooks('client.authorize', [ClientInfo, Action, Topic], Default) of
        AuthzResult = #{result := Result} when Result == allow; Result == deny ->
            From = maps:get(from, AuthzResult, unknown),
            ok = log_result(Topic, Action, From, Result),
            emqx_hooks:run(
                'client.check_authz_complete',
                [ClientInfo, Action, Topic, Result, From]
            ),
            Result;
        Other ->
            ?SLOG(error, #{
                msg => "unknown_authorization_return_format",
                expected_example => "#{result => allow, from => default}",
                got => Other
            }),
            emqx_hooks:run(
                'client.check_authz_complete',
                [ClientInfo, Action, Topic, deny, unknown_return_format]
            ),
            deny
    end.

log_result(Topic, Action, From, Result) ->
    LogMeta = fun() ->
        #{
            topic => Topic,
            action => format_action(Action),
            source => format_from(From)
        }
    end,
    do_log_result(Action, Result, LogMeta).

do_log_result(_Action, allow, LogMeta) ->
    ?SLOG(info, (LogMeta())#{msg => "authorization_permission_allowed"}, #{tag => "AUTHZ"});
do_log_result(?AUTHZ_PUBLISH_MATCH_MAP(_, _), deny, LogMeta) ->
    %% for publish action, we do not log permission deny at warning level here
    %% because it will be logged as cannot_publish_to_topic_due_to_not_authorized
    ?SLOG(info, (LogMeta())#{msg => "authorization_permission_denied"}, #{tag => "AUTHZ"});
do_log_result(_, deny, LogMeta) ->
    ?SLOG_THROTTLE(
        warning,
        (LogMeta())#{msg => authorization_permission_denied},
        #{tag => "AUTHZ"}
    ).

%% @private Format authorization rules source.
format_from(default) ->
    "'authorization.no_match' config";
format_from(unknown) ->
    "'client.authorize' hook callback";
format_from(Type) ->
    Type.

%% @doc Format enriched action info for logging.
format_action(?AUTHZ_SUBSCRIBE_MATCH_MAP(QoS)) ->
    "SUBSCRIBE(" ++ format_qos(QoS) ++ ")";
format_action(?AUTHZ_PUBLISH_MATCH_MAP(QoS, Retain)) ->
    "PUBLISH(" ++ format_qos(QoS) ++ "," ++ format_retain_flag(Retain) ++ ")".

format_qos(QoS) ->
    "Q" ++ integer_to_list(QoS).

format_retain_flag(true) ->
    "R1";
format_retain_flag(false) ->
    "R0".

-compile({inline, [run_hooks/3]}).
run_hooks(Name, Args, Acc) ->
    ok = emqx_metrics:inc(Name),
    emqx_hooks:run_fold(Name, Args, Acc).

-compile({inline, [inc_authz_metrics/1]}).
inc_authz_metrics(allow) ->
    emqx_metrics:inc('authorization.allow');
inc_authz_metrics(deny) ->
    emqx_metrics:inc('authorization.deny');
inc_authz_metrics(cache_hit) ->
    emqx_metrics:inc('authorization.cache_hit');
inc_authz_metrics(cache_miss) ->
    emqx_metrics:inc('authorization.cache_miss').

inc_authn_metrics(error) ->
    emqx_metrics:inc('authentication.failure');
inc_authn_metrics(ok) ->
    emqx_metrics:inc('authentication.success');
inc_authn_metrics(anonymous) ->
    emqx_metrics:inc('client.auth.anonymous'),
    emqx_metrics:inc('authentication.success.anonymous'),
    emqx_metrics:inc('authentication.success').

on_authentication_complete_no_hooks(#{enable_authn := false} = Credential, Extra) ->
    on_authentication_complete_success(Credential, Extra, anonymous),
    {ok, Extra};
on_authentication_complete_no_hooks(Credential, _Extra) ->
    on_authentication_complete_error(Credential, no_authn_hooks),
    {error, not_authorized}.

on_authentication_complete_error(Credential, Reason) ->
    emqx_hooks:run(
        'client.check_authn_complete',
        [
            Credential,
            #{
                reason_code => Reason
            }
        ]
    ),
    inc_authn_metrics(error).
on_authentication_complete_success(Credential, Result, Type) ->
    emqx_hooks:run(
        'client.check_authn_complete',
        [
            Credential,
            Result#{
                reason_code => success,
                is_anonymous => (Type =:= anonymous)
            }
        ]
    ),
    inc_authn_metrics(Type).
