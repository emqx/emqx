%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_acl_internal).

-behaviour(emqttd_acl_mod).

-include("emqttd.hrl").

-export([all_rules/0]).

%% ACL callbacks
-export([init/1, check_acl/2, reload_acl/1, description/0]).

-define(ACL_RULE_TAB, mqtt_acl_rule).

-record(state, {acl_file, nomatch = allow}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Read all rules
-spec(all_rules() -> list(emqttd_access_rule:rule())).
all_rules() ->
    case ets:lookup(?ACL_RULE_TAB, all_rules) of
        [] -> [];
        [{_, Rules}] -> Rules
    end.

%%--------------------------------------------------------------------
%% ACL callbacks
%%--------------------------------------------------------------------

%% @doc Init internal ACL
-spec(init(AclOpts :: list()) -> {ok, State :: any()}).
init(AclOpts) ->
    ets:new(?ACL_RULE_TAB, [set, public, named_table, {read_concurrency, true}]),
    AclFile = proplists:get_value(file, AclOpts),
    Default = proplists:get_value(nomatch, AclOpts, allow),
    State = #state{acl_file = AclFile, nomatch = Default},
    true = load_rules_from_file(State),
    {ok, State}.

load_rules_from_file(#state{acl_file = AclFile}) ->
    {ok, Terms} = file:consult(AclFile),
    Rules = [emqttd_access_rule:compile(Term) || Term <- Terms],
    lists:foreach(fun(PubSub) ->
        ets:insert(?ACL_RULE_TAB, {PubSub,
            lists:filter(fun(Rule) -> filter(PubSub, Rule) end, Rules)})
        end, [publish, subscribe]),
    ets:insert(?ACL_RULE_TAB, {all_rules, Terms}).

filter(_PubSub, {allow, all}) ->
    true;
filter(_PubSub, {deny, all}) ->
    true;
filter(publish, {_AllowDeny, _Who, publish, _Topics}) ->
    true;
filter(_PubSub, {_AllowDeny, _Who, pubsub, _Topics}) ->
    true;
filter(subscribe, {_AllowDeny, _Who, subscribe, _Topics}) ->
    true;
filter(_PubSub, {_AllowDeny, _Who, _, _Topics}) ->
    false.

%% @doc Check ACL
-spec(check_acl({Client, PubSub, Topic}, State) -> allow | deny | ignore when
      Client :: mqtt_client(),
      PubSub :: pubsub(),
      Topic  :: binary(),
      State  :: #state{}).
check_acl({Client, PubSub, Topic}, #state{nomatch = Default}) ->
    case match(Client, Topic, lookup(PubSub)) of
        {matched, allow} -> allow;
        {matched, deny}  -> deny;
        nomatch          -> Default
    end.

lookup(PubSub) ->
    case ets:lookup(?ACL_RULE_TAB, PubSub) of
        [] -> [];
        [{PubSub, Rules}] -> Rules
    end.

match(_Client, _Topic, []) ->
    nomatch;

match(Client, Topic, [Rule|Rules]) ->
    case emqttd_access_rule:match(Client, Topic, Rule) of
        nomatch -> match(Client, Topic, Rules);
        {matched, AllowDeny} -> {matched, AllowDeny}
    end.

%% @doc Reload ACL
-spec(reload_acl(State :: #state{}) -> ok | {error, Reason :: any()}).
reload_acl(State) ->
    case catch load_rules_from_file(State) of
        {'EXIT', Error} -> {error, Error};
        _ -> ok
    end.

%% @doc ACL Module Description
-spec(description() -> string()).
description() -> "Internal ACL with etc/acl.config".

