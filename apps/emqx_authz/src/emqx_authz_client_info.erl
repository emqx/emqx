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

-module(emqx_authz_client_info).

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

-define(RULE_NAMES, [
    {[pub, <<"pub">>], publish},
    {[sub, <<"sub">>], subscribe},
    {[all, <<"all">>], all}
]).

%%--------------------------------------------------------------------
%% emqx_authz callbacks
%%--------------------------------------------------------------------

description() ->
    "AuthZ with ClientInfo".

create(Source) ->
    Source.

update(Source) ->
    Source.

destroy(_Source) -> ok.

authorize(#{acl := Acl} = Client, PubSub, Topic, _Source) ->
    case check(Acl) of
        {ok, Rules} when is_map(Rules) ->
            do_authorize(Client, PubSub, Topic, Rules);
        {error, MatchResult} ->
            MatchResult
    end;
authorize(_Client, _PubSub, _Topic, _Source) ->
    nomatch.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

check(#{expire := Expire, rules := Rules}) when is_map(Rules) ->
    Now = erlang:system_time(second),
    case Expire of
        N when is_integer(N) andalso N > Now -> {ok, Rules};
        undefined -> {ok, Rules};
        _ -> {error, {matched, deny}}
    end;
%% no expire
check(#{rules := Rules}) ->
    {ok, Rules};
%% no rules — no match
check(#{}) ->
    {error, nomatch}.

do_authorize(Client, PubSub, Topic, AclRules) ->
    do_authorize(Client, PubSub, Topic, AclRules, ?RULE_NAMES).

do_authorize(_Client, _PubSub, _Topic, _AclRules, []) ->
    {matched, deny};
do_authorize(Client, PubSub, Topic, AclRules, [{Keys, Action} | RuleNames]) ->
    TopicFilters = get_topic_filters(Keys, AclRules, []),
    case
        emqx_authz_rule:match(
            Client,
            PubSub,
            Topic,
            emqx_authz_rule:compile({allow, all, Action, TopicFilters})
        )
    of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, AclRules, RuleNames)
    end.

get_topic_filters([], _Rules, Default) ->
    Default;
get_topic_filters([Key | Keys], Rules, Default) ->
    case Rules of
        #{Key := Value} -> Value;
        #{} -> get_topic_filters(Keys, Rules, Default)
    end.
