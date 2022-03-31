%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_rule).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% APIs
-export([
    match/4,
    matches/4,
    compile/1
]).

-type ipaddress() ::
    {ipaddr, esockd_cidr:cidr_string()}
    | {ipaddrs, list(esockd_cidr:cidr_string())}.

-type username() :: {username, binary()}.

-type clientid() :: {clientid, binary()}.

-type who() ::
    ipaddress()
    | username()
    | clientid()
    | {'and', [ipaddress() | username() | clientid()]}
    | {'or', [ipaddress() | username() | clientid()]}
    | all.

-type action() :: subscribe | publish | all.
-type permission() :: allow | deny.

-type rule() :: {permission(), who(), action(), list(emqx_types:topic())}.

-export_type([
    action/0,
    permission/0
]).

compile({Permission, all}) when
    ?ALLOW_DENY(Permission)
->
    {Permission, all, all, [compile_topic(<<"#">>)]};
compile({Permission, Who, Action, TopicFilters}) when
    ?ALLOW_DENY(Permission), ?PUBSUB(Action), is_list(TopicFilters)
->
    {atom(Permission), compile_who(Who), atom(Action), [
        compile_topic(Topic)
     || Topic <- TopicFilters
    ]}.

compile_who(all) ->
    all;
compile_who({user, Username}) ->
    compile_who({username, Username});
compile_who({username, {re, Username}}) ->
    {ok, MP} = re:compile(bin(Username)),
    {username, MP};
compile_who({username, Username}) ->
    {username, {eq, bin(Username)}};
compile_who({client, Clientid}) ->
    compile_who({clientid, Clientid});
compile_who({clientid, {re, Clientid}}) ->
    {ok, MP} = re:compile(bin(Clientid)),
    {clientid, MP};
compile_who({clientid, Clientid}) ->
    {clientid, {eq, bin(Clientid)}};
compile_who({ipaddr, CIDR}) ->
    {ipaddr, esockd_cidr:parse(CIDR, true)};
compile_who({ipaddrs, CIDRs}) ->
    {ipaddrs, lists:map(fun(CIDR) -> esockd_cidr:parse(CIDR, true) end, CIDRs)};
compile_who({'and', L}) when is_list(L) ->
    {'and', [compile_who(Who) || Who <- L]};
compile_who({'or', L}) when is_list(L) ->
    {'or', [compile_who(Who) || Who <- L]}.

compile_topic(<<"eq ", Topic/binary>>) ->
    {eq, emqx_topic:words(Topic)};
compile_topic({eq, Topic}) ->
    {eq, emqx_topic:words(bin(Topic))};
compile_topic(Topic) ->
    Words = emqx_topic:words(bin(Topic)),
    case pattern(Words) of
        true -> {pattern, Words};
        false -> Words
    end.

pattern(Words) ->
    lists:member(?PH_USERNAME, Words) orelse lists:member(?PH_CLIENTID, Words).

atom(B) when is_binary(B) ->
    try
        binary_to_existing_atom(B, utf8)
    catch
        _E:_S -> binary_to_atom(B)
    end;
atom(A) when is_atom(A) -> A.

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.

-spec matches(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic(), [rule()]) ->
    {matched, allow} | {matched, deny} | nomatch.
matches(_Client, _PubSub, _Topic, []) ->
    nomatch;
matches(Client, PubSub, Topic, [{Permission, Who, Action, TopicFilters} | Tail]) ->
    case match(Client, PubSub, Topic, {Permission, Who, Action, TopicFilters}) of
        nomatch -> matches(Client, PubSub, Topic, Tail);
        Matched -> Matched
    end.

-spec match(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic(), rule()) ->
    {matched, allow} | {matched, deny} | nomatch.
match(Client, PubSub, Topic, {Permission, Who, Action, TopicFilters}) ->
    case
        match_action(PubSub, Action) andalso
            match_who(Client, Who) andalso
            match_topics(Client, Topic, TopicFilters)
    of
        true -> {matched, Permission};
        _ -> nomatch
    end.

match_action(publish, publish) -> true;
match_action(subscribe, subscribe) -> true;
match_action(_, all) -> true;
match_action(_, _) -> false.

match_who(_, all) ->
    true;
match_who(#{username := undefined}, {username, _}) ->
    false;
match_who(#{username := Username}, {username, {eq, Username}}) ->
    true;
match_who(#{username := Username}, {username, {re_pattern, _, _, _, _} = MP}) ->
    case re:run(Username, MP) of
        {match, _} -> true;
        _ -> false
    end;
match_who(#{clientid := Clientid}, {clientid, {eq, Clientid}}) ->
    true;
match_who(#{clientid := Clientid}, {clientid, {re_pattern, _, _, _, _} = MP}) ->
    case re:run(Clientid, MP) of
        {match, _} -> true;
        _ -> false
    end;
match_who(#{peerhost := undefined}, {ipaddr, _CIDR}) ->
    false;
match_who(#{peerhost := IpAddress}, {ipaddr, CIDR}) ->
    esockd_cidr:match(IpAddress, CIDR);
match_who(#{peerhost := undefined}, {ipaddrs, _CIDR}) ->
    false;
match_who(#{peerhost := IpAddress}, {ipaddrs, CIDRs}) ->
    lists:any(
        fun(CIDR) ->
            esockd_cidr:match(IpAddress, CIDR)
        end,
        CIDRs
    );
match_who(ClientInfo, {'and', Principals}) when is_list(Principals) ->
    lists:foldl(
        fun(Principal, Permission) ->
            match_who(ClientInfo, Principal) andalso Permission
        end,
        true,
        Principals
    );
match_who(ClientInfo, {'or', Principals}) when is_list(Principals) ->
    lists:foldl(
        fun(Principal, Permission) ->
            match_who(ClientInfo, Principal) orelse Permission
        end,
        false,
        Principals
    );
match_who(_, _) ->
    false.

match_topics(_ClientInfo, _Topic, []) ->
    false;
match_topics(ClientInfo, Topic, [{pattern, PatternFilter} | Filters]) ->
    TopicFilter = feed_var(ClientInfo, PatternFilter),
    match_topic(emqx_topic:words(Topic), TopicFilter) orelse
        match_topics(ClientInfo, Topic, Filters);
match_topics(ClientInfo, Topic, [TopicFilter | Filters]) ->
    match_topic(emqx_topic:words(Topic), TopicFilter) orelse
        match_topics(ClientInfo, Topic, Filters).

match_topic(Topic, {'eq', TopicFilter}) ->
    Topic =:= TopicFilter;
match_topic(Topic, TopicFilter) ->
    emqx_topic:match(Topic, TopicFilter).

feed_var(ClientInfo, Pattern) ->
    feed_var(ClientInfo, Pattern, []).
feed_var(_ClientInfo, [], Acc) ->
    lists:reverse(Acc);
feed_var(ClientInfo = #{clientid := undefined}, [?PH_CLIENTID | Words], Acc) ->
    feed_var(ClientInfo, Words, [?PH_CLIENTID | Acc]);
feed_var(ClientInfo = #{clientid := ClientId}, [?PH_CLIENTID | Words], Acc) ->
    feed_var(ClientInfo, Words, [ClientId | Acc]);
feed_var(ClientInfo = #{username := undefined}, [?PH_USERNAME | Words], Acc) ->
    feed_var(ClientInfo, Words, [?PH_USERNAME | Acc]);
feed_var(ClientInfo = #{username := Username}, [?PH_USERNAME | Words], Acc) ->
    feed_var(ClientInfo, Words, [Username | Acc]);
feed_var(ClientInfo, [W | Words], Acc) ->
    feed_var(ClientInfo, Words, [W | Acc]).
