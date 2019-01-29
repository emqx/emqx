%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_acl_internal).

-behaviour(emqx_acl_mod).

-include("emqx.hrl").

-export([all_rules/0]).

%% ACL mod callbacks
-export([init/1, check_acl/2, reload_acl/1, description/0]).

-define(ACL_RULE_TAB, emqx_acl_rule).

-type(state() :: #{acl_file := string()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Read all rules
-spec(all_rules() -> list(emqx_access_rule:rule())).
all_rules() ->
    case ets:lookup(?ACL_RULE_TAB, all_rules) of
        [] -> [];
        [{_, Rules}] -> Rules
    end.

%%------------------------------------------------------------------------------
%% ACL callbacks
%%------------------------------------------------------------------------------

-spec(init([File :: string()]) -> {ok, #{}}).
init([File]) ->
    _ = emqx_tables:new(?ACL_RULE_TAB, [set, public, {read_concurrency, true}]),
    ok = load_rules_from_file(File),
    {ok, #{acl_file => File}}.

load_rules_from_file(AclFile) ->
    case file:consult(AclFile) of
        {ok, Terms} ->
            Rules = [emqx_access_rule:compile(Term) || Term <- Terms],
            lists:foreach(fun(PubSub) ->
                ets:insert(?ACL_RULE_TAB, {PubSub,
                    lists:filter(fun(Rule) -> filter(PubSub, Rule) end, Rules)})
                end, [publish, subscribe]),
            ets:insert(?ACL_RULE_TAB, {all_rules, Terms}),
            ok;
        {error, Reason} ->
            emqx_logger:error("[ACL_INTERNAL] Failed to read ~s: ~p", [AclFile, Reason]),
            {error, Reason}
    end.
  
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
-spec(check_acl({emqx_types:credentials(), emqx_types:pubsub(), emqx_topic:topic()}, #{})
      -> allow | deny | ignore).
check_acl({Credentials, PubSub, Topic}, _State) ->
    case match(Credentials, Topic, lookup(PubSub)) of
        {matched, allow} -> allow;
        {matched, deny}  -> deny;
        nomatch          -> ignore
    end.

lookup(PubSub) ->
    case ets:lookup(?ACL_RULE_TAB, PubSub) of
        [] -> [];
        [{PubSub, Rules}] -> Rules
    end.

match(_Credentials, _Topic, []) ->
    nomatch;
match(Credentials, Topic, [Rule|Rules]) ->
    case emqx_access_rule:match(Credentials, Topic, Rule) of
        nomatch ->
            match(Credentials, Topic, Rules);
        {matched, AllowDeny} ->
            {matched, AllowDeny}
    end.

-spec(reload_acl(state()) -> ok | {error, term()}).
reload_acl(#{acl_file := AclFile}) ->
    case catch load_rules_from_file(AclFile) of
        ok ->
            emqx_logger:info("Reload acl_file ~s successfully", [AclFile]),
            ok;
        {error, Error} ->
            {error, Error};
        {'EXIT', Error} ->
            {error, Error}
    end.

-spec(description() -> string()).
description() ->
    "Internal ACL with etc/acl.conf".

