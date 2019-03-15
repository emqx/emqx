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

-module(emqx_mod_acl_internal).

-behaviour(emqx_gen_mod).

-include("emqx.hrl").
-include("logger.hrl").

-export([load/1, unload/1]).

-export([all_rules/0]).

-export([check_acl/4, reload_acl/1, description/0]).

-define(ACL_RULE_TAB, emqx_acl_rule).

-type(state() :: #{acl_file := string()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

load(Env) ->
    _ = emqx_tables:new(?ACL_RULE_TAB, [set, public, {read_concurrency, true}]),
    File = proplists:get_value(acl_file, Env),
    ok = load_rules_from_file(File),
    emqx_hooks:add('client.check_acl', fun ?MODULE:check_acl/4, -1).

unload(_Env) ->
    emqx_tables:delete(?ACL_RULE_TAB),
    emqx_hooks:del('client.check_acl', fun ?MODULE:check_acl/4).

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
-spec(check_acl(emqx_types:credentials(), emqx_types:pubsub(), emqx_topic:topic(), AclResult::allow | deny) -> {ok, allow} | {ok, deny} | ok).
check_acl(Credentials, PubSub, Topic, _AclResult) ->
    case match(Credentials, Topic, lookup(PubSub)) of
        {matched, allow} -> {ok, allow};
        {matched, deny}  -> {ok, deny};
        nomatch          -> ok
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
    try load_rules_from_file(AclFile) of
        ok ->
            emqx_logger:info("Reload acl_file ~s successfully", [AclFile]),
            ok;
        {error, Error} ->
            {error, Error}
    catch
        error:Reason:StackTrace ->
            ?LOG(error, "Reload acl failed. StackTrace: ~p", [StackTrace]),
            {error, Reason}
    end.

-spec(description() -> string()).
description() ->
    "Internal ACL with etc/acl.conf".
