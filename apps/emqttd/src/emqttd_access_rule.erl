%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
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

-author("Feng Lee <feng@emqtt.io>").

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

-export_type([rule/0]).

-export([compile/1, match/3]).

%%------------------------------------------------------------------------------
%% @doc Compile access rule
%% @end
%%------------------------------------------------------------------------------
compile({A, all}) when (A =:= allow) orelse (A =:= deny) ->
    {A, all};

compile({A, Who, Access, TopicFilters}) when (A =:= allow) orelse (A =:= deny) ->
    {A, compile(who, Who), Access, [compile(topic, Topic) || Topic <- TopicFilters]}.

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

compile(topic, {eq, Topic}) ->
    {eq, emqtt_topic:words(bin(Topic))};
compile(topic, Topic) ->
    Words = emqtt_topic:words(bin(Topic)),
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

%%------------------------------------------------------------------------------
%% @doc Match rule
%% @end
%%------------------------------------------------------------------------------
-spec match(mqtt_client(), topic(), rule()) -> {matched, allow} | {matched, deny} | nomatch.
match(_Client, _Topic, {AllowDeny, all}) when (AllowDeny =:= allow) orelse (AllowDeny =:= deny) ->
    {matched, AllowDeny};
match(Client, Topic, {AllowDeny, Who, _PubSub, TopicFilters})
        when (AllowDeny =:= allow) orelse (AllowDeny =:= deny) ->
    case match_who(Client, Who) andalso match_topics(Client, Topic, TopicFilters) of
        true  -> {matched, AllowDeny};
        false -> nomatch
    end.

match_who(_Client, all) ->
    true;
match_who(_Client, {user, all}) ->
    true;
match_who(_Client, {client, all}) ->
    true;
match_who(#mqtt_client{clientid = ClientId}, {client, ClientId}) ->
    true;
match_who(#mqtt_client{username = Username}, {user, Username}) ->
    true;
match_who(#mqtt_client{ipaddr = undefined}, {ipaddr, _Tup}) ->
    false;
match_who(#mqtt_client{ipaddr = IP}, {ipaddr, {_CDIR, Start, End}}) ->
    I = esockd_access:atoi(IP),
    I >= Start andalso I =< End;
match_who(_Client, _Who) ->
    false.

match_topics(_Client, _Topic, []) ->
    false;
match_topics(Client, Topic, [{pattern, PatternFilter}|Filters]) ->
    TopicFilter = feed_var(Client, PatternFilter),
    case match_topic(emqtt_topic:words(Topic), TopicFilter) of
        true -> true;
        false -> match_topics(Client, Topic, Filters)
    end;
match_topics(Client, Topic, [TopicFilter|Filters]) ->
   case match_topic(emqtt_topic:words(Topic), TopicFilter) of
    true -> true;
    false -> match_topics(Client, Topic, Filters)
    end.

match_topic(Topic, {eq, TopicFilter}) ->
    Topic =:= TopicFilter;
match_topic(Topic, TopicFilter) ->
    emqtt_topic:match(Topic, TopicFilter).

feed_var(Client, Pattern) ->
    feed_var(Client, Pattern, []).
feed_var(_Client, [], Acc) ->
    lists:reverse(Acc);
feed_var(Client = #mqtt_client{clientid = undefined}, [<<"$c">>|Words], Acc) ->
    feed_var(Client, Words, [<<"$c">>|Acc]);
feed_var(Client = #mqtt_client{clientid = ClientId}, [<<"$c">>|Words], Acc) ->
    feed_var(Client, Words, [ClientId |Acc]);
feed_var(Client = #mqtt_client{username = undefined}, [<<"$u">>|Words], Acc) ->
    feed_var(Client, Words, [<<"$u">>|Acc]);
feed_var(Client = #mqtt_client{username = Username}, [<<"$u">>|Words], Acc) ->
    feed_var(Client, Words, [Username|Acc]);
feed_var(Client, [W|Words], Acc) ->
    feed_var(Client, Words, [W|Acc]).

