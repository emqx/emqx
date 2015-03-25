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
%%% emqttd access rule match
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_access).

-include("emqttd.hrl").

-export([match/3]).

-type who() :: all | 
               {clientid, binary()} | 
               {peername, string() | inet:ip_address()} |
               {username, binary()}.

-type rule() :: {allow, all} |
                {allow, who(), binary()} |
                {deny,  all} |
                {deny,  who(), binary()}.

-spec match(mqtt_user(), binary(), list(rule())) -> allow | deny | nomatch.
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

match_cidr(IpAddr, CIDR) -> true.
    
