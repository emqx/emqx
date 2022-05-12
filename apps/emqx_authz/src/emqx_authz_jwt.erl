%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_jwt).

-include_lib("emqx/include/logger.hrl").

-behaviour(emqx_authz).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% APIs
-export([
    description/0,
    create/1,
    update/1,
    destroy/1,
    authorize/4
]).

-define(JWT_RULE_NAMES, [
    {<<"pub">>, publish},
    {<<"sub">>, subscribe},
    {<<"all">>, all}
]).

%%--------------------------------------------------------------------
%% emqx_authz callbacks
%%--------------------------------------------------------------------

description() ->
    "AuthZ with JWT".

create(#{acl_claim_name := _AclClaimName} = Source) ->
    Source.

update(#{acl_claim_name := _AclClaimName} = Source) ->
    Source.

destroy(_Source) -> ok.

authorize(#{jwt := JWT} = Client, PubSub, Topic, #{acl_claim_name := AclClaimName}) ->
    case verify(JWT) of
        {ok, #{AclClaimName := Rules}} when is_map(Rules) ->
            do_authorize(Client, PubSub, Topic, Rules);
        _ ->
            {matched, deny}
    end;
authorize(_Client, _PubSub, _Topic, _Source) ->
    nomatch.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

verify(JWT) ->
    Now = erlang:system_time(second),
    VerifyClaims =
        [
            {<<"exp">>, fun(ExpireTime) ->
                is_integer(ExpireTime) andalso Now < ExpireTime
            end},
            {<<"iat">>, fun(IssueAt) ->
                is_integer(IssueAt) andalso IssueAt =< Now
            end},
            {<<"nbf">>, fun(NotBefore) ->
                is_integer(NotBefore) andalso NotBefore =< Now
            end}
        ],
    IsValid = lists:all(
        fun({ClaimName, Validator}) ->
            (not maps:is_key(ClaimName, JWT)) orelse
                Validator(maps:get(ClaimName, JWT))
        end,
        VerifyClaims
    ),
    case IsValid of
        true -> {ok, JWT};
        false -> error
    end.

do_authorize(Client, PubSub, Topic, AclRules) ->
    do_authorize(Client, PubSub, Topic, AclRules, ?JWT_RULE_NAMES).

do_authorize(_Client, _PubSub, _Topic, _AclRules, []) ->
    {matched, deny};
do_authorize(Client, PubSub, Topic, AclRules, [{Key, Action} | JWTRuleNames]) ->
    TopicFilters = maps:get(Key, AclRules, []),
    case
        emqx_authz_rule:match(
            Client,
            PubSub,
            Topic,
            emqx_authz_rule:compile({allow, all, Action, TopicFilters})
        )
    of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, AclRules, JWTRuleNames)
    end.
