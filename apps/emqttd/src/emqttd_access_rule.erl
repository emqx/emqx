%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd ACL rule.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_access_rule).

-include("emqttd.hrl").

-type who() :: all | binary() |
               {ipaddr, esockd_access:cidr()} |
               {client, binary()} |
               {user, binary()}.

-type access() :: subscribe | publish | pubsub.

-type topic() :: binary().

-type rule() :: {allow, all} |
                {allow, who(), access(), list(topic())} |
                {deny, all} |
                {deny, who(), access(), list(topic())}.

-export([compile/1, match/3]).

%%%-----------------------------------------------------------------------------
%% @doc
%% Compile rule.
%%
%% @end
%%%-----------------------------------------------------------------------------
compile({A, all}) when (A =:= allow) orelse (A =:= deny) ->
    {A, all};

compile({A, Who, Access, TopicFilters}) when (A =:= allow) orelse (A =:= deny) ->
    {A, compile(who, Who), Access, [compile(topic, bin(Topic)) || Topic <- TopicFilters]}.

compile(who, all) -> 
    all;
compile(who, {ipaddr, CIDR}) ->
    {Start, End} = esockd_access:range(CIDR),
    {ipaddr, {CIDR, Start, End}};
compile(who, {client, all}) ->
    {client, all};
compile(who, {client, ClientId}) ->
    {client, bin(ClientId)};
compile(who, {user, all}) ->
    {user, all};
compile(who, {user, Username}) ->
    {user, bin(Username)};

compile(topic, Topic) ->
    Words = emqttd_topic:words(Topic),
    case 'pattern?'(Words) of
        true -> {pattern, Words};
        false -> Words
    end.

'pattern?'(Words) ->
    lists:member(<<"$u">>, Words)
        orelse lists:member(<<"$c">>, Words).

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.

%%%-----------------------------------------------------------------------------
%% @doc
%% Match rule.
%%
%% @end
%%%-----------------------------------------------------------------------------
-spec match(mqtt_user(), topic(), rule()) -> {matched, allow} | {matched, deny} | nomatch.
match(_User, _Topic, {AllowDeny, all}) when (AllowDeny =:= allow) orelse (AllowDeny =:= deny) ->
    {matched, AllowDeny};
match(User, Topic, {AllowDeny, Who, _PubSub, TopicFilters})
        when (AllowDeny =:= allow) orelse (AllowDeny =:= deny) ->
    case match_who(User, Who) andalso match_topics(User, Topic, TopicFilters) of
        true  -> {matched, AllowDeny};
        false -> nomatch
    end.

match_who(_User, all) ->
    true;
match_who(_User, {user, all}) ->
    true;
match_who(_User, {client, all}) ->
    true;
match_who(#mqtt_user{clientid = ClientId}, {client, ClientId}) ->
    true;
match_who(#mqtt_user{username = Username}, {user, Username}) ->
    true;
match_who(#mqtt_user{ipaddr = IP}, {ipaddr, {_CDIR, Start, End}}) ->
    I = esockd_access:atoi(IP),
    I >= Start andalso I =< End;
match_who(_User, _Who) ->
    false.

match_topics(_User, _Topic, []) ->
    false;
match_topics(User, Topic, [{pattern, PatternFilter}|Filters]) ->
    TopicFilter = feed_var(User, PatternFilter),
    case match_topic(emqttd_topic:words(Topic), TopicFilter) of
        true -> true;
        false -> match_topics(User, Topic, Filters)
    end;
match_topics(User, Topic, [TopicFilter|Filters]) ->
   case match_topic(emqttd_topic:words(Topic), TopicFilter) of
    true -> true;
    false -> match_topics(User, Topic, Filters)
    end.

match_topic(Topic, TopicFilter) ->
    emqttd_topic:match(Topic, TopicFilter).

feed_var(User, Pattern) ->
    feed_var(User, Pattern, []).
feed_var(_User, [], Acc) ->
    lists:reverse(Acc);
feed_var(User = #mqtt_user{clientid = undefined}, [<<"$c">>|Words], Acc) ->
    feed_var(User, Words, [<<"$c">>|Acc]);
feed_var(User = #mqtt_user{clientid = ClientId}, [<<"$c">>|Words], Acc) ->
    feed_var(User, Words, [ClientId |Acc]);
feed_var(User = #mqtt_user{username = undefined}, [<<"$u">>|Words], Acc) ->
    feed_var(User, Words, [<<"$u">>|Acc]);
feed_var(User = #mqtt_user{username = Username}, [<<"$u">>|Words], Acc) ->
    feed_var(User, Words, [Username|Acc]);
feed_var(User, [W|Words], Acc) ->
    feed_var(User, Words, [W|Acc]).

