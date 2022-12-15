%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("logger.hrl").

-export([
    authenticate/1,
    authorize/3
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
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
    case emqx_authentication:pre_hook_authenticate(Credential) of
        ok ->
            inc_authn_metrics(anonymous),
            {ok, NotSuperUser};
        continue ->
            case run_hooks('client.authenticate', [Credential], ignore) of
                ignore ->
                    inc_authn_metrics(anonymous),
                    {ok, NotSuperUser};
                ok ->
                    inc_authn_metrics(ok),
                    {ok, NotSuperUser};
                {ok, _AuthResult} = OkResult ->
                    inc_authn_metrics(ok),
                    OkResult;
                {ok, _AuthResult, _AuthData} = OkResult ->
                    inc_authn_metrics(ok),
                    OkResult;
                {error, _Reason} = Error ->
                    inc_authn_metrics(error),
                    Error;
                %% {continue, AuthCache} | {continue, AuthData, AuthCache}
                Other ->
                    Other
            end;
        {error, _Reason} = Error ->
            inc_authn_metrics(error),
            Error
    end.

%% @doc Check Authorization
-spec authorize(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic()) ->
    allow | deny.
authorize(ClientInfo, PubSub, <<"$delayed/", Data/binary>> = RawTopic) ->
    case binary:split(Data, <<"/">>) of
        [_, Topic] ->
            authorize(ClientInfo, PubSub, Topic);
        _ ->
            ?SLOG(warning, #{
                msg => "invalid_delayed_topic_format",
                expected_example => "$delayed/1/t/foo",
                got => RawTopic
            }),
            inc_authz_metrics(deny),
            deny
    end;
authorize(ClientInfo, PubSub, Topic) ->
    Result =
        case emqx_authz_cache:is_enabled() of
            true -> check_authorization_cache(ClientInfo, PubSub, Topic);
            false -> do_authorize(ClientInfo, PubSub, Topic)
        end,
    inc_authz_metrics(Result),
    Result.

check_authorization_cache(ClientInfo, PubSub, Topic) ->
    case emqx_authz_cache:get_authz_cache(PubSub, Topic) of
        not_found ->
            AuthzResult = do_authorize(ClientInfo, PubSub, Topic),
            emqx_authz_cache:put_authz_cache(PubSub, Topic, AuthzResult),
            AuthzResult;
        AuthzResult ->
            emqx:run_hook(
                'client.check_authz_complete',
                [ClientInfo, PubSub, Topic, AuthzResult, cache]
            ),
            inc_authz_metrics(cache_hit),
            AuthzResult
    end.

do_authorize(ClientInfo, PubSub, Topic) ->
    NoMatch = emqx:get_config([authorization, no_match], allow),
    Default = #{result => NoMatch, from => default},
    case run_hooks('client.authorize', [ClientInfo, PubSub, Topic], Default) of
        AuthzResult = #{result := Result} when Result == allow; Result == deny ->
            From = maps:get(from, AuthzResult, unknown),
            emqx:run_hook(
                'client.check_authz_complete',
                [ClientInfo, PubSub, Topic, Result, From]
            ),
            Result;
        Other ->
            ?SLOG(error, #{
                msg => "unknown_authorization_return_format",
                expected_example => "#{result => allow, from => default}",
                got => Other
            }),
            emqx:run_hook(
                'client.check_authz_complete',
                [ClientInfo, PubSub, Topic, deny, unknown_return_format]
            ),
            deny
    end.

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
    emqx_metrics:inc('authorization.cache_hit').

inc_authn_metrics(error) ->
    emqx_metrics:inc('authentication.failure');
inc_authn_metrics(ok) ->
    emqx_metrics:inc('authentication.success');
inc_authn_metrics(anonymous) ->
    emqx_metrics:inc('authentication.success.anonymous'),
    emqx_metrics:inc('authentication.success').
