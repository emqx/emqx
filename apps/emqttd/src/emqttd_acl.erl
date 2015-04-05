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
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_acl).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-type pubsub() :: subscribe | publish | pubsub.

-type who() :: all | binary() |
               {ipaddr, esockd_access:cidr()} |
               {client, binary()} |
               {user, binary()}.

-type rule() :: {allow, all} |
                {allow, who(), pubsub(), list(binary())} |
                {deny, all} |
                {deny, who(), pubsub(), list(binary())}.

-export_type([pubsub/0]).

-callback check_acl(PubSub, User, Topic) -> {ok, allow | deny} | ignore | {error, any()} when
    PubSub   :: pubsub(),
    User     :: mqtt_user(),
    Topic    :: binary().

-callback reload_acl() -> ok | {error, any()}.

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/1, check/3, reload/0, register_mod/1, unregister_mod/1]).

%% ACL Callback
-export([check_acl/3, reload_acl/0]).

-ifdef(TEST).

-export([compile/1, match/3]).

-endif.

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(ACL_TAB, mqtt_acl).

-record(state, {acl_file, raw_rules = []}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start ACL Server.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link(AclOpts) -> {ok, pid()} | ignore | {error, any()} when
    AclOpts     :: [{file, list()}].
start_link(AclOpts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [AclOpts], []).

-spec check(PubSub, User, Topic) -> allow | deny when
      PubSub :: pubsub(),
      User   :: mqtt_user(),
      Topic  :: binary().
check(PubSub, User, Topic) ->
    case ets:lookup(?ACL_TAB, acl_mods) of
        [] -> {error, "No ACL mod!"};
        [{_, Mods}] -> check(PubSub, User, Topic, Mods)
    end.
check(_PubSub, _User, _Topic, []) ->
    {error, "All ACL mods ignored!"};
check(PubSub, User, Topic, [Mod|Mods]) ->
    case Mod:check_acl(PubSub, User, Topic) of
        {ok, AllowDeny} -> AllowDeny;
        ignore -> check(PubSub, User, Topic, Mods)
    end.

%%TODO: 
reload() ->
    case ets:lookup(?ACL_TAB, acl_mods) of
        [] -> {error, "No ACL mod!"};
        [{_, Mods}] -> [M:reload() || M <- Mods]
    end.

register_mod(Mod) ->
    gen_server:call(?MODULE, {register_mod, Mod}).

unregister_mod(Mod) ->
    gen_server:call(?MODULE, {unregister_mod, Mod}).

-spec check_acl(PubSub, User, Topic) -> {ok, allow} | {ok, deny} | ignore | {error, any()} when
      PubSub :: pubsub(),
      User   :: mqtt_user(),
      Topic  :: binary().
check_acl(PubSub, User, Topic) ->
    case match(User, Topic, lookup(PubSub)) of
        {matched, allow} -> {ok, allow};
        {matched, deny}  -> {ok, deny};
        nomatch          -> {error, nomatch}
    end.

-spec reload_acl() -> ok.
reload_acl() ->
    gen_server:call(?SERVER, reload).

lookup(PubSub) ->
    case ets:lookup(?ACL_TAB, PubSub) of
        [] -> [];
        [{PubSub, Rules}] -> Rules
    end.

match(_User, _Topic, []) ->
    nomatch;

match(User, Topic, [Rule|Rules]) ->
    case match_rule(User, Topic, Rule) of
        nomatch -> match(User, Topic, Rules);
        {matched, AllowDeny} -> {matched, AllowDeny}
    end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([AclOpts]) ->
    ets:new(?ACL_TAB, [set, protected, named_table]),
    ets:insert(?ACL_TAB, {acl_mods, [?MODULE]}),
    AclFile = proplists:get_value(file, AclOpts),
    load_rules(#state{acl_file = AclFile}).

load_rules(State = #state{acl_file = AclFile}) ->
    {ok, Terms} = file:consult(AclFile),
    Rules = [compile(Term) || Term <- Terms],
    lists:foreach(fun(PubSub) ->
        ets:insert(?ACL_TAB, {PubSub, 
            lists:filter(fun(Rule) -> filter(PubSub, Rule) end, Rules)})
        end, [publish, subscribe]),
    {ok, State#state{raw_rules = Terms}}.

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

handle_call(reload, _From, State) ->
    case catch load_rules(State) of
        {ok, NewState} -> 
            {reply, ok, NewState};
        {'EXIT', Error} -> 
            {reply, {error, Error}, State}
    end;

handle_call({register_mod, Mod}, _From, State) ->
    [{_, Mods}] = ets:lookup(?ACL_TAB, acl_mods), 
    case lists:member(Mod, Mods) of
        true -> 
            {reply, {error, registered}, State};
        false -> 
            ets:insert(?ACL_TAB, {acl_mods, [Mod|Mods]}),
            {reply, ok, State}
    end;

handle_call({unregister_mod, Mod}, _From, State) ->
    [{_, Mods}] = ets:lookup(?ACL_TAB, acl_mods), 
    case lists:member(Mod, Mods) of
        true ->
            ets:insert(?ACL_TAB, lists:delete(Mod, Mods)),
            {reply, ok, State};
        false -> 
            {reply, {error, not_found}, State}
    end;

handle_call(Req, _From, State) ->
    lager:error("Bad Request: ~p", [Req]),
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

%%%-----------------------------------------------------------------------------
%% @doc
%% Compile rule.
%%
%% @end
%%%-----------------------------------------------------------------------------
compile({A, all}) when (A =:= allow) orelse (A =:= deny) ->
    {A, all};

compile({A, Who, PubSub, TopicFilters}) when (A =:= allow) orelse (A =:= deny) ->
    {A, compile(who, Who), PubSub, [compile(topic, bin(Topic)) || Topic <- TopicFilters]}.

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
    case pattern(Words) of
        true -> {pattern, Words};
        false -> Words
    end.

pattern(Words) ->
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
-spec match_rule(mqtt_user(), binary(), rule()) -> {matched, allow} | {matched, deny} | nomatch.
match_rule(_User, _Topic, {AllowDeny, all}) when (AllowDeny =:= allow) orelse (AllowDeny =:= deny) ->
    {matched, AllowDeny};
match_rule(User, Topic, {AllowDeny, Who, _PubSub, TopicFilters})
        when (AllowDeny =:= allow) orelse (AllowDeny =:= deny)  ->
    case match_who(User, Who) andalso match_topics(User, Topic, TopicFilters) of
        true -> {matched, AllowDeny};
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


