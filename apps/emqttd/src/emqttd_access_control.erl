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
%%% emqttd authentication and ACL server.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_access_control).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/1,
         auth/2,       % authentication
         check_acl/3,  % acl check
         reload_acl/0, % reload acl
         register_mod/3,
         unregister_mod/2,
         lookup_mods/1,
         stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(ACCESS_CONTROL_TAB, mqtt_access_control).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start access control server.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link(AcOpts :: list()) -> {ok, pid()} | ignore | {error, any()}.
start_link(AcOpts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [AcOpts], []).

%%------------------------------------------------------------------------------
%% @doc
%% Authenticate client.
%%
%% @end
%%------------------------------------------------------------------------------
-spec auth(mqtt_client(), undefined | binary()) -> ok | {error, string()}.
auth(Client, Password) when is_record(Client, mqtt_client) ->
    auth(Client, Password, lookup_mods(auth)).
auth(_Client, _Password, []) ->
    {error, "No auth module to check!"};
auth(Client, Password, [{Mod, State} | Mods]) ->
    case Mod:check(Client, Password, State) of
        ok -> ok;
        {error, Reason} -> {error, Reason};
        ignore -> auth(Client, Password, Mods)
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Check ACL.
%%
%% @end
%%------------------------------------------------------------------------------
-spec check_acl(Client, PubSub, Topic) -> allow | deny when
      Client :: mqtt_client(),
      PubSub :: pubsub(),
      Topic  :: binary().
check_acl(Client, PubSub, Topic) when PubSub =:= publish orelse PubSub =:= subscribe ->
    case lookup_mods(acl) of
        [] -> allow;
        AclMods -> check_acl(Client, PubSub, Topic, AclMods)
    end.
check_acl(#mqtt_client{clientid = ClientId}, PubSub, Topic, []) ->
    lager:error("ACL: nomatch when ~s ~s ~s", [ClientId, PubSub, Topic]),
    allow;
check_acl(Client, PubSub, Topic, [{M, State}|AclMods]) ->
    case M:check_acl({Client, PubSub, Topic}, State) of
        allow -> allow;
        deny  -> deny;
        ignore -> check_acl(Client, PubSub, Topic, AclMods)
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Reload ACL.
%%
%% @end
%%------------------------------------------------------------------------------
-spec reload_acl() -> list() | {error, any()}.
reload_acl() ->
    [M:reload_acl(State) || {M, State} <- lookup_mods(acl)].

%%------------------------------------------------------------------------------
%% @doc
%% Register auth or ACL module.
%%
%% @end
%%------------------------------------------------------------------------------
-spec register_mod(Type :: auth | acl, Mod :: atom(), Opts :: list()) -> ok | {error, any()}.
register_mod(Type, Mod, Opts) when Type =:= auth; Type =:= acl->
    gen_server:call(?SERVER, {register_mod, Type, Mod, Opts}).

%%------------------------------------------------------------------------------
%% @doc
%% Unregister auth or ACL module.
%%
%% @end
%%------------------------------------------------------------------------------
-spec unregister_mod(Type :: auth | acl, Mod :: atom()) -> ok | {error, any()}.
unregister_mod(Type, Mod) when Type =:= auth; Type =:= acl ->
    gen_server:call(?SERVER, {unregister_mod, Type, Mod}).

%%------------------------------------------------------------------------------
%% @doc
%% Lookup authentication or ACL modules.
%%
%% @end
%%------------------------------------------------------------------------------
-spec lookup_mods(auth | acl) -> list().
lookup_mods(Type) ->
    case ets:lookup(?ACCESS_CONTROL_TAB, tab_key(Type)) of
        [] -> [];
        [{_, Mods}] -> Mods
    end.
tab_key(auth) ->
    auth_modules;
tab_key(acl) ->
    acl_modules.

%%------------------------------------------------------------------------------
%% @doc
%% Stop access control server.
%%
%% @end
%%------------------------------------------------------------------------------
stop() ->
    gen_server:call(?MODULE, stop).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([AcOpts]) ->
	ets:new(?ACCESS_CONTROL_TAB, [set, named_table, protected, {read_concurrency, true}]),
    ets:insert(?ACCESS_CONTROL_TAB, {auth_modules, init_mods(auth, proplists:get_value(auth, AcOpts))}),
    ets:insert(?ACCESS_CONTROL_TAB, {acl_modules, init_mods(acl, proplists:get_value(acl, AcOpts))}),
	{ok, state}.

init_mods(auth, AuthMods) ->
    [init_mod(fun authmod/1, Name, Opts) || {Name, Opts} <- AuthMods];

init_mods(acl, AclMods) ->
    [init_mod(fun aclmod/1, Name, Opts) || {Name, Opts} <- AclMods].

init_mod(Fun, Name, Opts) ->
    Module = Fun(Name),
    {ok, State} = Module:init(Opts),
    {Module, State}.

handle_call({register_mod, Type, Mod, Opts}, _From, State) ->
    Mods = lookup_mods(Type),
    Reply =
    case lists:keyfind(Mod, 1, Mods) of
        false -> 
            case catch Mod:init(Opts) of
                {ok, ModState} -> 
                    ets:insert(?ACCESS_CONTROL_TAB, {tab_key(Type), [{Mod, ModState}|Mods]}),
                    ok;
                {'EXIT', Error} ->
                    {error, Error}
            end;
        _ -> 
            {error, existed}
    end,
    {reply, Reply, State};

handle_call({unregister_mod, Type, Mod}, _From, State) ->
    Mods = lookup_mods(Type),
    Reply =
    case lists:keyfind(Mod, 1, Mods) of
        false -> 
            {error, not_found}; 
        _ -> 
            ets:insert(?ACCESS_CONTROL_TAB, {tab_key(Type), lists:keydelete(Mod, 1, Mods)}), ok
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

authmod(Name) when is_atom(Name) ->
	list_to_atom(lists:concat(["emqttd_auth_", Name])).

aclmod(Name) when is_atom(Name) ->
	list_to_atom(lists:concat(["emqttd_acl_", Name])).

