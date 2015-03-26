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
%%% emqttd access rule
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_access_rule).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-export([match/3, encode/1, decode/1]).

-type who()     :: all | binary() |
                   {ipaddr, esockd_access:cidr()} |
                   {clientid, binary()} |
                   {clientid, {regexp, binary()}} |
                   {username, binary()} |
                   {username, {regexp, binary()}}.

-type rule()    :: {allow, all} |
                   {allow, who(), binary()} |
                   {deny,  all} |
                   {deny,  who(), binary()}.

-opaque enc_who() :: all | binary() | 
                     {ipaddr, esockd_access:range()} |
                     {clientid, binary()} | 
                     {clientid, {regexp, binary(), re:mp()}} | 
                     {username, binary()} | 
                     {username, {regexp, binary(), re:mp()}}.

-opaque enc_rule() :: {allow, all} |
                      {allow, enc_who(), binary()} | 
                      {deny,  all} | 
                      {deny,  enc_who(), binary()}.

-export_type([who/0, rule/0, enc_who/0, enc_rule/0]).

-spec match(mqtt_user(), binary(), enc_rule()) -> {matched, allow} | {matched, deny} | nomatch.
match(_User, _Topic, []) ->
    nomatch;
match(_User, _Topic, [{AllowDeny, all}|_]) ->
    AllowDeny;
match(User, Topic, [{AllowDeny, all, TopicFilter}|Rules]) ->
    case emqttd_topic:match(Topic, TopicFilter) of
        true -> AllowDeny;
        false -> match(User, Topic, Rules)
    end;
match(User = #mqtt_user{clientid = ClientId}, Topic, [{AllowDeny, ClientId, TopicFilter}|Rules]) when is_binary(ClientId) ->
    case emqttd_topic:match(Topic, TopicFilter) of
        true -> AllowDeny;
        false -> match(User, Topic, Rules)
    end;
match(User = #mqtt_user{peername = IpAddr}, Topic, [{AllowDeny, {peername, CIDR}, TopicFilter}|Rules]) ->
    case {match_cidr(IpAddr, CIDR), emqttd_topic:match(Topic, TopicFilter)} of
        {true, true} -> AllowDeny;
        _ -> match(User, Topic, Rules)
    end;
match(User = #mqtt_user{username = Username}, Topic, [{AllowDeny, {username, Username}, TopicFilter}|Rules]) ->
    case emqttd_topic:match(Topic, TopicFilter) of
        true -> AllowDeny;
        false -> match(User, Topic, Rules)
    end.


encode({allow, all}) ->
    {allow, all};
encode({allow, Who, Topic}) ->
    {allow, encode(who, Who), Topic};
encode({deny, all}) ->
    {deny, all};
encode({deny, Who, Topic}) ->
    {deny, encode(who, Who), Topic}.

encode(who, all) -> 
    all;
encode(who, ClientId) when is_binary(ClientId) ->
    ClientId;
encode(who, {ip, CIDR}) ->
    {Start, End} = esockd_access:range(CIDR),
    {ip, {CIDR, Start, End}};
encode(who, {clientid, ClientId}) ->
    {clientid, compile(ClientId)};
encode(who, {username, Username}) ->
    {username, compile(Username)}.

compile(Bin) when is_binary(Bin) ->
    Bin;
compile({regexp, Regexp}) ->
    {ok, MP} = re:compile(Regexp),
    {regexp, Regexp, MP}.

decode({allow, all}) ->
    {allow, all};
decode({allow, EncodedWho, Topic}) ->
    {allow, decode(who, EncodedWho), Topic};
decode({deny, all}) ->
    {deny, all};
decode({deny, EncodedWho, Topic}) ->
    {allow, decode(who, EncodedWho), Topic}.

decode(who, all) ->
    all;
decode(who, ClientId) when is_binary(ClientId) ->
    ClientId;
decode(who, {ip, {CIDR, _Start, _End}}) ->
    {ip, CIDR};
decode(who, {clientid, ClientId}) when is_binary(ClientId) ->
    {clientid, uncompile(ClientId)};
decode(who, {username, Username}) when is_binary(Username) ->
    {username, uncompile(Username)}.

uncompile(Bin) when is_binary(Bin) ->
    Bin;
uncompile({regexp, Regexp, MP}) ->
    {ok, MP} = re:compile(Regexp),
    {regexp, Regexp}.


