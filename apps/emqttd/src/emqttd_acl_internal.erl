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
%%% Internal ACL that load rules from etc/acl.config
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_acl_internal).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-export([start_link/1]).

-behaviour(emqttd_acl).

%% ACL callbacks
-export([check_acl/3, reload_acl/0, description/0]).

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(ACL_RULE_TABLE, mqtt_acl_rule).

-record(state, {acl_file, raw_rules = []}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start Internal ACL Server.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link(AclOpts) -> {ok, pid()} | ignore | {error, any()} when
        AclOpts :: [{file, list()}].
start_link(AclOpts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [AclOpts], []).

%%%=============================================================================
%%% ACL callbacks 
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Check ACL.
%%
%% @end
%%------------------------------------------------------------------------------
-spec check_acl(User, PubSub, Topic) -> {ok, allow} | {ok, deny} | ignore | {error, any()} when
      User   :: mqtt_user(),
      PubSub :: publish | subscribe,
      Topic  :: binary().
check_acl(User, PubSub, Topic) ->
    case match(User, Topic, lookup(PubSub)) of
        {matched, allow} -> {ok, allow};
        {matched, deny}  -> {ok, deny};
        nomatch          -> {error, nomatch}
    end.

lookup(PubSub) ->
    case ets:lookup(?ACL_RULE_TABLE, PubSub) of
        [] -> [];
        [{PubSub, Rules}] -> Rules
    end.

match(_User, _Topic, []) ->
    nomatch;

match(User, Topic, [Rule|Rules]) ->
    case emqttd_access_rule:match(User, Topic, Rule) of
        nomatch -> match(User, Topic, Rules);
        {matched, AllowDeny} -> {matched, AllowDeny}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Reload ACL.
%%
%% @end
%%------------------------------------------------------------------------------
-spec reload_acl() -> ok.
reload_acl() ->
    gen_server:call(?SERVER, reload).

%%------------------------------------------------------------------------------
%% @doc
%% ACL Description.
%%
%% @end
%%------------------------------------------------------------------------------
-spec description() -> string().
description() ->
    "Internal ACL with etc/acl.config".

%%%=============================================================================
%%% gen_server callbacks 
%%%=============================================================================

init([AclOpts]) ->
    ets:new(?ACL_RULE_TABLE, [set, proteted, named_table]),
    AclFile = proplists:get_value(file, AclOpts),
    load_rules(#state{acl_file = AclFile}).

handle_call(reload, _From, State) ->
    case catch load_rules(State) of
        {ok, NewState} -> 
            {reply, ok, NewState};
        {'EXIT', Error} -> 
            {reply, {error, Error}, State}
    end;

handle_call(Req, _From, State) ->
    lager:error("BadReq: ~p", [Req]),
    {reply, {error, badreq}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

load_rules(State = #state{acl_file = AclFile}) ->
    {ok, Terms} = file:consult(AclFile),
    Rules = [emqttd_access_rule:compile(Term) || Term <- Terms],
    lists:foreach(fun(PubSub) ->
        ets:insert(?ACL_RULE_TABLE, {PubSub, 
            lists:filter(fun(Rule) -> filter(PubSub, Rule) end, Rules)})
        end, [publish, subscribe]),
    {ok, State#state{raw_rules = Terms}}.

filter(_PubSub, {allow, all}) ->
    true;
filter(publish, {_AllowDeny, _Who, publish, _Topics}) ->
    true;
filter(_PubSub, {_AllowDeny, _Who, pubsub, _Topics}) ->
    true;
filter(subscribe, {_AllowDeny, _Who, subscribe, _Topics}) ->
    true;
filter(_PubSub, {_AllowDeny, _Who, _, _Topics}) ->
    false.

