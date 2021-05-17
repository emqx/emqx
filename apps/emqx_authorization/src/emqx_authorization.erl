%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authorization).

-include("emqx_authorization.hrl").

-export([ init/0
        , compile/1
        , lookup/0
        , update/1
        ]).


init() ->
    Rules = [
             #{<<"access">> => <<"allow">>,
               <<"action">> => <<"pub">>,
               <<"topics">> => [<<"#">>,<<"Topic/A">>]
              }
            ],
    ok = application:set_env(?APP, rules, Rules),
    NRules = [compile(Rule) || Rule <- Rules],
    ok = emqx_hooks:add('client.check_acl', {?MODULE, check_acl, [NRules]},  999999).

lookup() ->
    application:get_env(?APP, rules, []).

update(Rules) ->
    ok = application:set_env(?APP, rules, Rules),
    NRules = [compile(Rule) || Rule <- Rules],
    Action = find_action_in_hooks(),
    ok = emqx_hooks:del('client.check_acl', Action),
    ok = emqx_hooks:add('client.check_acl', {?MODULE, check_acl, [Rules]},  999999).

find_action_in_hooks() ->
    Callbacks = emqx_hooks:lookup('client.check_acl'),
    [Action] = [Action || {callback,{?MODULE, check_acl, _} = Action, _, _} <- Callbacks ],
    Action.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec(compile(rule()) -> rule()).
compile(#{<<"topics">> := TopicFilters,
          <<"action">> := Action,
          <<"access">> := Access
         } = Rule ) when ?ALLOW_DENY(Access), ?PUBSUB(Action) ->
    Principal = compile(principal, maps:get(<<"principal">>, Rule, <<"all">>)),
    NTopicFilters = [compile(topic, Topic) || Topic <- TopicFilters],
    Rule#{<<"principal">> => Principal,
          <<"topics">> => NTopicFilters
         }.

compile(principal, <<"all">>) -> <<"all">>;
compile(principal, #{<<"username">> := Username}) ->
    {ok, MP} = re:compile(bin(Username)),
    #{username => MP};
compile(principal, #{<<"clientid">> := Clientid}) ->
    {ok, MP} = re:compile(bin(Clientid)),
    #{clientid => MP};
compile(principal, #{<<"ipaddress">> := IpAddress}) ->
    #{ipaddress => esockd_cidr:parse(IpAddress, true)};

compile(topic, {eq, Topic}) ->
    {eq, emqx_topic:words(bin(Topic))};
compile(topic, Topic) ->
    Words = emqx_topic:words(bin(Topic)),
    case pattern(Words) of
        true  -> {pattern, Words};
        false -> Words
    end.

pattern(Words) ->
    lists:member(<<"%u">>, Words) orelse lists:member(<<"%c">>, Words).

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.

%%--------------------------------------------------------------------
%% ACL callbacks
%%--------------------------------------------------------------------

% %% @doc Check ACL
% -spec(check_acl(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_topic:topic(),
%                 emqx_access_rule:acl_result(), acl_rules())
%       -> {ok, allow} | {ok, deny} | ok).
% check_acl(Client, PubSub, Topic, _AclResult, Rules) ->
%     case match(Client, Topic, lookup(PubSub, Rules)) of
%         {matched, allow} -> {ok, allow};
%         {matched, deny}  -> {ok, deny};
%         nomatch          -> ok
%     end.
%
