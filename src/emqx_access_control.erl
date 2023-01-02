%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([authenticate/1]).

-export([ check_acl/3
        ]).

-type(result() :: #{auth_result := emqx_types:auth_result(),
                    anonymous := boolean()
                   }).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(authenticate(emqx_types:clientinfo()) -> {ok, result()} | {error, term()}).
authenticate(ClientInfo = #{zone := Zone}) ->
    ok = emqx_metrics:inc('client.authenticate'),
    Username = maps:get(username, ClientInfo, undefined),
    {MaybeStop, AuthResult} = default_auth_result(Username, Zone),
    case MaybeStop of
        stop ->
            return_auth_result(AuthResult);
        continue ->
            return_auth_result(emqx_hooks:run_fold('client.authenticate', [ClientInfo], AuthResult))
    end.

%% @doc Check ACL
-spec(check_acl(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic())
      -> allow | deny).
check_acl(ClientInfo, PubSub, Topic) ->
    Result = case emqx_acl_cache:is_enabled() of
        true  -> check_acl_cache(ClientInfo, PubSub, Topic);
        false -> do_check_acl(ClientInfo, PubSub, Topic)
    end,
    inc_acl_metrics(Result),
    Result.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
%% ACL
check_acl_cache(ClientInfo, PubSub, Topic) ->
    case emqx_acl_cache:get_acl_cache(PubSub, Topic) of
        not_found ->
            AclResult = do_check_acl(ClientInfo, PubSub, Topic),
            emqx_acl_cache:put_acl_cache(PubSub, Topic, AclResult),
            AclResult;
        AclResult ->
            inc_acl_metrics(cache_hit),
            emqx:run_hook('client.check_acl_complete', [ClientInfo, PubSub, Topic, AclResult, true]),
            AclResult
    end.

do_check_acl(ClientInfo = #{zone := Zone}, PubSub, Topic) ->
    Default = emqx_zone:get_env(Zone, acl_nomatch, deny),
    ok = emqx_metrics:inc('client.check_acl'),
    Result = case emqx_hooks:run_fold('client.check_acl', [ClientInfo, PubSub, Topic], Default) of
                allow  -> allow;
                _Other -> deny
             end,
    emqx:run_hook('client.check_acl_complete', [ClientInfo, PubSub, Topic, Result, false]),
    Result.

-compile({inline, [inc_acl_metrics/1]}).
inc_acl_metrics(allow) ->
    emqx_metrics:inc('client.acl.allow');
inc_acl_metrics(deny) ->
    emqx_metrics:inc('client.acl.deny');
inc_acl_metrics(cache_hit) ->
    emqx_metrics:inc('client.acl.cache_hit').

%% Auth
default_auth_result(Username, Zone) ->
    IsAnonymous = (Username =:= undefined orelse Username =:= <<>>),
    AllowAnonymous = emqx_zone:get_env(Zone, allow_anonymous, false),
    Bypass = emqx_zone:get_env(Zone, bypass_auth_plugins, false),
    %% the `anonymous` field in auth result does not mean the client is
    %% connected without username, but if the auth result is based on
    %% allowing anonymous access.
    IsResultBasedOnAllowAnonymous =
        case AllowAnonymous of
            true -> true;
            _ -> false
        end,
    Result = case AllowAnonymous of
                 true -> #{auth_result => success, anonymous => IsResultBasedOnAllowAnonymous};
                 _ -> #{auth_result => not_authorized, anonymous => IsResultBasedOnAllowAnonymous}
             end,
    case {IsAnonymous, AllowAnonymous} of
        {true, false_quick_deny} ->
            {stop, Result};
        _ when Bypass ->
            {stop, Result};
        _ ->
            {continue, Result}
    end.

-compile({inline, [return_auth_result/1]}).
return_auth_result(AuthResult = #{auth_result := success}) ->
    inc_auth_success_metrics(AuthResult),
    {ok, AuthResult};
return_auth_result(AuthResult) ->
    emqx_metrics:inc('client.auth.failure'),
    {error, maps:get(auth_result, AuthResult, unknown_error)}.

-compile({inline, [inc_auth_success_metrics/1]}).
inc_auth_success_metrics(AuthResult) ->
    is_anonymous(AuthResult) andalso
        emqx_metrics:inc('client.auth.success.anonymous'),
    emqx_metrics:inc('client.auth.success').

is_anonymous(#{anonymous := true}) -> true;
is_anonymous(_AuthResult)          -> false.
