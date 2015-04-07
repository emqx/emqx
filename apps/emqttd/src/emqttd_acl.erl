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
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_acl).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/1, check/3, reload/0,
         register_mod/1, unregister_mod/1, all_modules/0,
         stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(ACL_TABLE, mqtt_acl).

%%%=============================================================================
%%% ACL behavihour
%%%=============================================================================

-ifdef(use_specs).

-callback init(AclOpts :: list()) -> {ok, State :: any()}.

-callback check_acl(User, PubSub, Topic) -> allow | deny | ignore when
    User     :: mqtt_user(),
    PubSub   :: pubsub(),
    Topic    :: binary().

-callback reload_acl() -> ok | {error, any()}.

-callback description() -> string().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
        [{init, 1}, {check_acl, 3}, {reload_acl, 0}, {description, 0}];
behaviour_info(_Other) ->
        undefined.

-endif.

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

%%------------------------------------------------------------------------------
%% @doc
%% Check ACL.
%%
%% @end
%%--------------------------------------------------------------------------
-spec check(User, PubSub, Topic) -> allow | deny | ignore when
      User   :: mqtt_user(),
      PubSub :: pubsub(),
      Topic  :: binary().
check(User, PubSub, Topic) when PubSub =:= publish orelse PubSub =:= subscribe ->
    case ets:lookup(?ACL_TABLE, acl_modules) of
        [] -> allow;
        [{_, Mods}] -> check(User, PubSub, Topic, Mods)
    end.

check(#mqtt_user{clientid = ClientId}, PubSub, Topic, []) ->
    lager:error("ACL: nomatch when ~s ~s ~s", [ClientId, PubSub, Topic]),
    allow;

check(User, PubSub, Topic, [Mod|Mods]) ->
    case Mod:check_acl(User, PubSub, Topic) of
        allow -> allow;
        deny  -> deny;
        ignore -> check(User, PubSub, Topic, Mods)
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Reload ACL.
%%
%% @end
%%------------------------------------------------------------------------------
reload() ->
    case ets:lookup(?ACL_TABLE, acl_modules) of
        [] -> {error, "No ACL mod!"};
        [{_, Mods}] -> [M:reload_acl() || M <- Mods]
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Register ACL Module.
%%
%% @end
%%------------------------------------------------------------------------------
-spec register_mod(Mod :: atom()) -> ok | {error, any()}.
register_mod(Mod) ->
    gen_server:call(?SERVER, {register_mod, Mod}).

%%------------------------------------------------------------------------------
%% @doc
%% Unregister ACL Module.
%%
%% @end
%%------------------------------------------------------------------------------
-spec unregister_mod(Mod :: atom()) -> ok | {error, any()}.
unregister_mod(Mod) ->
    gen_server:cast(?SERVER, {unregister_mod, Mod}).

%%------------------------------------------------------------------------------
%% @doc
%% All ACL Modules.
%%
%% @end
%%------------------------------------------------------------------------------
all_modules() ->
    case ets:lookup(?ACL_TABLE, acl_modules) of
        [] -> [];
        [{_, Mods}] -> Mods
    end.

stop() ->
    gen_server:call(?SERVER, stop).

%%%=============================================================================
%%% gen_server callbacks.
%%%=============================================================================
init([_AclOpts]) ->
    ets:new(?ACL_TABLE, [set, protected, named_table]),
    {ok, state}.

handle_call({register_mod, Mod}, _From, State) ->
    Mods = all_modules(),
    case lists:member(Mod, Mods) of
        true ->
            {reply, {error, existed}, State};
        false ->
            ets:insert(?ACL_TABLE, {acl_modules, [Mod | Mods]}),
            {reply, ok, State}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    lager:error("Bad Request: ~p", [Req]),
    {reply, {error, badreq}, State}.

handle_cast({unregister_mod, Mod}, State) ->
    Mods = all_modules(),
    case lists:member(Mod, Mods) of
        true ->
            ets:insert(?ACL_TABLE, {acl_modules, lists:delete(Mod, Mods)});
        false -> 
            lager:error("unknown acl module: ~s", [Mod])
    end,
    {noreply, State};

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

