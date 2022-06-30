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

-export([
    authenticate/1,
    authorize/3
]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec authenticate(emqx_types:clientinfo()) ->
    {ok, map()}
    | {ok, map(), binary()}
    | {continue, map()}
    | {continue, binary(), map()}
    | {error, term()}.
authenticate(Credential) ->
    case run_hooks('client.authenticate', [Credential], {ok, #{is_superuser => false}}) of
        ok ->
            {ok, #{is_superuser => false}};
        Other ->
            Other
    end.

%% @doc Check Authorization
-spec authorize(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic()) ->
    allow | deny.
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
    case run_hooks('client.authorize', [ClientInfo, PubSub, Topic], NoMatch) of
        allow -> allow;
        _Other -> deny
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
