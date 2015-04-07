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
-export([start_link/1, check/1, reload/0,
         register_mod/2, unregister_mod/1, all_modules/0,
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

-callback check_acl({User, PubSub, Topic}, State :: any()) -> allow | deny | ignore when
    User     :: mqtt_user(),
    PubSub   :: pubsub(),
    Topic    :: binary().

-callback reload_acl(State :: any()) -> ok | {error, any()}.

-callback description() -> string().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
        [{init, 1}, {check_acl, 2}, {reload_acl, 1}, {description, 0}];
behaviour_info(_Other) ->
        undefined.

-endif.

%%%=============================================================================
%%% API
%%%=============================================================================

%% @doc Start ACL Server.
-spec start_link(AclMods :: list()) -> {ok, pid()} | ignore | {error, any()}.
start_link(AclMods) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [AclMods], []).

%% @doc Check ACL.
-spec check({User, PubSub, Topic}) -> allow | deny when
      User   :: mqtt_user(),
      PubSub :: pubsub(),
      Topic  :: binary().
check({User, PubSub, Topic}) when PubSub =:= publish orelse PubSub =:= subscribe ->
    case ets:lookup(?ACL_TABLE, acl_modules) of
        [] -> allow;
        [{_, AclMods}] -> check({User, PubSub, Topic}, AclMods)
    end.

check({#mqtt_user{clientid = ClientId}, PubSub, Topic}, []) ->
    lager:error("ACL: nomatch when ~s ~s ~s", [ClientId, PubSub, Topic]),
    allow;

check({User, PubSub, Topic}, [{M, State}|AclMods]) ->
    case M:check_acl({User, PubSub, Topic}, State) of
        allow -> allow;
        deny  -> deny;
        ignore -> check({User, PubSub, Topic}, AclMods)
    end.

%% @doc Reload ACL.
-spec reload() -> list() | {error, any()}.
reload() ->
    case ets:lookup(?ACL_TABLE, acl_modules) of
        [] -> 
            {error, "No ACL modules!"};
        [{_, AclMods}] -> 
            [M:reload_acl(State) || {M, State} <- AclMods]
    end.

%% @doc Register ACL Module.
-spec register_mod(AclMod :: atom(), Opts :: list()) -> ok | {error, any()}.
register_mod(AclMod, Opts) ->
    gen_server:call(?SERVER, {register_mod, AclMod, Opts}).

%% @doc Unregister ACL Module.
-spec unregister_mod(AclMod :: atom()) -> ok | {error, any()}.
unregister_mod(AclMod) ->
    gen_server:call(?SERVER, {unregister_mod, AclMod}).

%% @doc All ACL Modules.
-spec all_modules() -> list().
all_modules() ->
    case ets:lookup(?ACL_TABLE, acl_modules) of
        [] -> [];
        [{_, AclMods}] -> AclMods
    end.

%% @doc Stop ACL server.
-spec stop() -> ok.
stop() ->
    gen_server:call(?SERVER, stop).

%%%=============================================================================
%%% gen_server callbacks.
%%%=============================================================================
init([AclMods]) ->
    ets:new(?ACL_TABLE, [set, protected, named_table]),
    AclMods1 = lists:map(
            fun({M, Opts}) ->
                AclMod = aclmod(M),
                {ok, State} = AclMod:init(Opts),
                {AclMod, State}
            end, AclMods),
    ets:insert(?ACL_TABLE, {acl_modules, AclMods1}),
    {ok, state}.

handle_call({register_mod, Mod, Opts}, _From, State) ->
    AclMods = all_modules(),
    Reply =
    case lists:keyfind(Mod, 1, AclMods) of
        false -> 
            case catch Mod:init(Opts) of
                {ok, ModState} -> 
                    ets:insert(?ACL_TABLE, {acl_modules, [{Mod, ModState}|AclMods]}),
                    ok;
                {'EXIT', Error} ->
                    {error, Error}
            end;
        _ -> 
            {error, existed}
    end,
    {reply, Reply, State};

handle_call({unregister_mod, Mod}, _From, State) ->
    AclMods = all_modules(),
    Reply =
    case lists:keyfind(Mod, 1, AclMods) of
        false -> 
            {error, not_found}; 
        _ -> 
            ets:insert(?ACL_TABLE, {acl_modules, lists:keydelete(Mod, 1, AclMods)}), ok
    end,
    {reply, Reply, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

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

aclmod(Name) when is_atom(Name) ->
	list_to_atom(lists:concat(["emqttd_acl_", Name])).


