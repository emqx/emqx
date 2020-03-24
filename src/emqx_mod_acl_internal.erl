%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_acl_internal).

-behaviour(emqx_gen_mod).

-include("emqx.hrl").
-include("logger.hrl").

-logger_header("[ACL_INTERNAL]").

%% APIs
-export([ all_rules/0
        , check_acl/5
        , reload_acl/0
        ]).

%% emqx_gen_mod callbacks
-export([ load/1
        , unload/1
        ]).

-define(MFA(M, F, A), {M, F, A}).

-type(acl_rules() :: #{publish   => [emqx_access_rule:rule()],
                       subscribe => [emqx_access_rule:rule()]}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

load(_Env) ->
    Rules = rules_from_file(acl_file()),
    emqx_hooks:add('client.check_acl', ?MFA(?MODULE, check_acl, [Rules]),  -1).

unload(_Env) ->
    Rules = rules_from_file(acl_file()),
    emqx_hooks:del('client.check_acl', ?MFA(?MODULE, check_acl, [Rules])).

%% @doc Read all rules
-spec(all_rules() -> list(emqx_access_rule:rule())).
all_rules() ->
    rules_from_file(acl_file()).

%%--------------------------------------------------------------------
%% ACL callbacks
%%--------------------------------------------------------------------

%% @doc Check ACL
-spec(check_acl(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_topic:topic(),
                emqx_access_rule:acl_result(), acl_rules())
      -> {ok, allow} | {ok, deny} | ok).
check_acl(Client, PubSub, Topic, _AclResult, Rules) ->
    case match(Client, Topic, lookup(PubSub, Rules)) of
        {matched, allow} -> {ok, allow};
        {matched, deny}  -> {ok, deny};
        nomatch          -> ok
    end.

-spec(reload_acl() -> ok | {error, term()}).
reload_acl() ->
    unload([]), load([]).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

acl_file() -> emqx:get_env(acl_file).

lookup(PubSub, Rules) ->
    maps:get(PubSub, Rules, []).

match(_Client, _Topic, []) ->
    nomatch;
match(Client, Topic, [Rule|Rules]) ->
    case emqx_access_rule:match(Client, Topic, Rule) of
        nomatch ->
            match(Client, Topic, Rules);
        {matched, AllowDeny} ->
            {matched, AllowDeny}
    end.

-spec(rules_from_file(file:filename()) -> map()).
rules_from_file(AclFile) ->
    case file:consult(AclFile) of
        {ok, Terms} ->
            Rules = [emqx_access_rule:compile(Term) || Term <- Terms],
            #{publish   => [Rule || Rule <- Rules, filter(publish, Rule)],
              subscribe => [Rule || Rule <- Rules, filter(subscribe, Rule)]};
        {error, eacces} ->
            ?LOG(alert, "Insufficient permissions to read the ~s file", [AclFile]),
            #{};
        {error, enoent} ->
            ?LOG(alert, "The ~s file does not exist", [AclFile]),
            #{};
        {error, Reason} ->
            ?LOG(alert, "Failed to read ~s: ~p", [AclFile, Reason]),
            #{}
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

