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

-module(emqx_access_rule).

-include("emqx.hrl").

%% APIs
-export([ match/3
        , compile/1
        ]).

-type(acl_result() :: allow | deny).

-type(who() :: all | binary() |
               {client, binary()} |
               {user, binary()} |
               {ipaddr, esockd_cidr:cidr_string()}).

-type(access() :: subscribe | publish | pubsub).

-type(rule() :: {acl_result(), all} |
                {acl_result(), who(), access(), list(emqx_topic:topic())}).

-export_type([rule/0]).

-define(ALLOW_DENY(A), ((A =:= allow) orelse (A =:= deny))).
-define(PUBSUB(A), ((A =:= subscribe) orelse (A =:= publish) orelse (A =:= pubsub))).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

%% @doc Compile Access Rule.
compile({A, all}) when ?ALLOW_DENY(A) ->
    {A, all};

compile({A, Who, Access, Topic}) when ?ALLOW_DENY(A), ?PUBSUB(Access), is_binary(Topic) ->
    {A, compile(who, Who), Access, [compile(topic, Topic)]};

compile({A, Who, Access, TopicFilters}) when ?ALLOW_DENY(A), ?PUBSUB(Access) ->
    {A, compile(who, Who), Access, [compile(topic, Topic) || Topic <- TopicFilters]}.

compile(who, all) ->
    all;
compile(who, {ipaddr, CIDR}) ->
    {ipaddr, esockd_cidr:parse(CIDR, true)};
compile(who, {client, all}) ->
    {client, all};
compile(who, {client, ClientId}) ->
    {client, bin(ClientId)};
compile(who, {user, all}) ->
    {user, all};
compile(who, {user, Username}) ->
    {user, bin(Username)};
compile(who, {'and', Conds}) when is_list(Conds) ->
    {'and', [compile(who, Cond) || Cond <- Conds]};
compile(who, {'or', Conds}) when is_list(Conds) ->
    {'or', [compile(who, Cond) || Cond <- Conds]};

compile(topic, {eq, Topic}) ->
    {eq, emqx_topic:words(bin(Topic))};
compile(topic, Topic) ->
    Words = emqx_topic:words(bin(Topic)),
    case 'pattern?'(Words) of
        true  -> {pattern, Words};
        false -> Words
    end.

'pattern?'(Words) ->
    lists:member(<<"%u">>, Words)
        orelse lists:member(<<"%c">>, Words).

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.

%% @doc Match access rule
-spec(match(emqx_types:credentials(), emqx_types:topic(), rule())
      -> {matched, allow} | {matched, deny} | nomatch).
match(_Credentials, _Topic, {AllowDeny, all}) when ?ALLOW_DENY(AllowDeny) ->
    {matched, AllowDeny};
match(Credentials, Topic, {AllowDeny, Who, _PubSub, TopicFilters})
    when ?ALLOW_DENY(AllowDeny) ->
    case match_who(Credentials, Who)
         andalso match_topics(Credentials, Topic, TopicFilters) of
        true  -> {matched, AllowDeny};
        false -> nomatch
    end.

match_who(_Credentials, all) ->
    true;
match_who(_Credentials, {user, all}) ->
    true;
match_who(_Credentials, {client, all}) ->
    true;
match_who(#{client_id := ClientId}, {client, ClientId}) ->
    true;
match_who(#{username := Username}, {user, Username}) ->
    true;
match_who(#{peername := undefined}, {ipaddr, _Tup}) ->
    false;
match_who(#{peername := {IP, _}}, {ipaddr, CIDR}) ->
    esockd_cidr:match(IP, CIDR);
match_who(Credentials, {'and', Conds}) when is_list(Conds) ->
    lists:foldl(fun(Who, Allow) ->
                  match_who(Credentials, Who) andalso Allow
                end, true, Conds);
match_who(Credentials, {'or', Conds}) when is_list(Conds) ->
    lists:foldl(fun(Who, Allow) ->
                  match_who(Credentials, Who) orelse Allow
                end, false, Conds);
match_who(_Credentials, _Who) ->
    false.

match_topics(_Credentials, _Topic, []) ->
    false;
match_topics(Credentials, Topic, [{pattern, PatternFilter}|Filters]) ->
    TopicFilter = feed_var(Credentials, PatternFilter),
    match_topic(emqx_topic:words(Topic), TopicFilter)
        orelse match_topics(Credentials, Topic, Filters);
match_topics(Credentials, Topic, [TopicFilter|Filters]) ->
   match_topic(emqx_topic:words(Topic), TopicFilter)
       orelse match_topics(Credentials, Topic, Filters).

match_topic(Topic, {eq, TopicFilter}) ->
    Topic == TopicFilter;
match_topic(Topic, TopicFilter) ->
    emqx_topic:match(Topic, TopicFilter).

feed_var(Credentials, Pattern) ->
    feed_var(Credentials, Pattern, []).
feed_var(_Credentials, [], Acc) ->
    lists:reverse(Acc);
feed_var(Credentials = #{client_id := undefined}, [<<"%c">>|Words], Acc) ->
    feed_var(Credentials, Words, [<<"%c">>|Acc]);
feed_var(Credentials = #{client_id := ClientId}, [<<"%c">>|Words], Acc) ->
    feed_var(Credentials, Words, [ClientId |Acc]);
feed_var(Credentials = #{username := undefined}, [<<"%u">>|Words], Acc) ->
    feed_var(Credentials, Words, [<<"%u">>|Acc]);
feed_var(Credentials = #{username := Username}, [<<"%u">>|Words], Acc) ->
    feed_var(Credentials, Words, [Username|Acc]);
feed_var(Credentials, [W|Words], Acc) ->
    feed_var(Credentials, Words, [W|Acc]).

