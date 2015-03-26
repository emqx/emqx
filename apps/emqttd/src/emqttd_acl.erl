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
%%% emqttd ACL.
%%%
%%% Two types of authorization:
%%% 
%%% subscribe topic
%%% publish to topic
%%%
%%% TODO: Support regexp...
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_acl).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(ACL_TAB, mqtt_acl).

%% API Function Exports
-export([start_link/0, check/3, allow/3, deny/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type pubsub() :: publish | subscribe.

-type who() :: all | binary() |
               {clientid, binary()} | 
               {peername, string() | inet:ip_address()} |
               {username, binary()}.

-type rule() :: {allow, all} |
                {allow, who(), binary()} |
                {deny,  all} |
                {deny,  who(), binary()}.

-record(mqtt_acl, {pubsub   :: pubsub(),
                   rules    :: list(rule())}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start ACL Server.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec check(PubSub, User, Topic) -> allowed | refused when
      PubSub :: pubsub(),
      User   :: mqtt_user(),
      Topic  :: binary().
check(PubSub, User, Topic) ->
    case match(User, Topic, lookup(PubSub)) of
        nomatch -> allowed;
        allow -> allow;
        deny -> deny 
    end.

lookup(PubSub) ->
    case ets:lookup(?ACL_TAB, PubSub) of
        [] -> [];
        [#mqtt_acl{pubsub = PubSub, rules = Rules}] -> Rules
    end.

match(_User, _Topic, []) ->
    nomatch;
match(User, Topic, Rules) ->
    %TODO:...
    nomatch.

-spec allow(PubSub, Who, Topic) -> ok | {error, any()} when 
      PubSub :: pubsub(),
      Who    :: who(),
      Topic  :: binary().
allow(PubSub, Who, Topic) ->
    add_rule(PubSub, {allow, Who, Topic}).

-spec deny(PubSub, Who, Topic) -> ok | {error, any()} when
      PubSub :: pubsub(),
      Who    :: who(),
      Topic  :: binary().
deny(PubSub, Who, Topic) ->
    add_rule(PubSub, {deny, Who, Topic}).

add_rule(PubSub, RawRule) ->
    case rule(RawRule) of
        {error, Error} ->
            {error, Error};
        Rule -> 
            F = fun() -> 
                case mnesia:wread(?ACL_TAB, PubSub) of
                    [] ->
                        mnesia:write(?ACL_TAB, #mqtt_acl{pubsub = PubSub, rules = [Rule]});
                    [Rules] ->
                        mnesia:write(?ACL_TAB, #mqtt_acl{pubsub = PubSub, rules = [Rule|Rules]})
                end
            end,
            case mnesia:transaction(F) of
                {atomic, _} -> ok;
                {aborted, Reason} -> {error, {aborted, Reason}}
            end
    end.

%% TODO: 
-spec rule(rule()) -> rule().
rule({allow, all}) ->
    {allow, all};
rule({allow, Who, Topic}) ->
    {allow, Who, Topic};
rule({deny, Who, Topic}) ->
    {deny, Who, Topic};
rule({deny, all}) ->
    {deny, all}.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init(Args) ->
    mnesia:create_table(?ACL_TAB, [
		{type, set},
		{record_name, mqtt_acl},
		{ram_copies, [node()]},
		{attributes, record_info(fields, mqtt_acl)}]),
    mnesia:add_table_copy(?ACL_TAB, node(), ram_copies),
    {ok, Args}.

handle_call(_Request, _From, State) ->
    {reply, error, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------



