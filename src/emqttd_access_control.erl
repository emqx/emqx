%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_access_control).

-include("emqttd.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/0, start_link/1,
         auth/2,       % authentication
         check_acl/3,  % acl check
         reload_acl/0, % reload acl
         lookup_mods/1,
         register_mod/3, register_mod/4,
         unregister_mod/2,
         stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(ACCESS_CONTROL_TAB, mqtt_access_control).

-type password() :: undefined | binary().

-record(state, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start access control server.
-spec(start_link() -> {ok, pid()} | ignore | {error, any()}).
start_link() -> start_link(emqttd:env(access)).

-spec(start_link(Opts :: list()) -> {ok, pid()} | ignore | {error, any()}).
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Opts], []).

%% @doc Authenticate MQTT Client.
-spec(auth(Client :: mqtt_client(), Password :: password()) -> ok | {error, any()}).
auth(Client, Password) when is_record(Client, mqtt_client) ->
    auth(Client, Password, lookup_mods(auth)).
auth(_Client, _Password, []) ->
    {error, "No auth module to check!"};
auth(Client, Password, [{Mod, State, _Seq} | Mods]) ->
    case catch Mod:check(Client, Password, State) of
        ok              -> ok;
        ignore          -> auth(Client, Password, Mods);
        {error, Reason} -> {error, Reason};
        {'EXIT', Error} -> {error, Error}
    end.

%% @doc Check ACL
-spec(check_acl(Client, PubSub, Topic) -> allow | deny when
      Client :: mqtt_client(),
      PubSub :: pubsub(),
      Topic  :: binary()).
check_acl(Client, PubSub, Topic) when ?IS_PUBSUB(PubSub) ->
    case lookup_mods(acl) of
        []      -> allow;
        AclMods -> check_acl(Client, PubSub, Topic, AclMods)
    end.
check_acl(#mqtt_client{client_id = ClientId}, PubSub, Topic, []) ->
    lager:error("ACL: nomatch for ~s ~s ~s", [ClientId, PubSub, Topic]),
    allow;
check_acl(Client, PubSub, Topic, [{Mod, State, _Seq}|AclMods]) ->
    case Mod:check_acl({Client, PubSub, Topic}, State) of
        allow  -> allow;
        deny   -> deny;
        ignore -> check_acl(Client, PubSub, Topic, AclMods)
    end.

%% @doc Reload ACL Rules.
-spec(reload_acl() -> list(ok | {error, already_existed})).
reload_acl() ->
    [Mod:reload_acl(State) || {Mod, State, _Seq} <- lookup_mods(acl)].

%% @doc Register Authentication or ACL module.
-spec(register_mod(auth | acl, atom(), list()) -> ok | {error, any()}).
register_mod(Type, Mod, Opts) when Type =:= auth; Type =:= acl->
    register_mod(Type, Mod, Opts, 0).

-spec(register_mod(auth | acl, atom(), list(), non_neg_integer()) -> ok | {error, any()}).
register_mod(Type, Mod, Opts, Seq) when Type =:= auth; Type =:= acl->
    gen_server:call(?SERVER, {register_mod, Type, Mod, Opts, Seq}).

%% @doc Unregister authentication or ACL module
-spec(unregister_mod(Type :: auth | acl, Mod :: atom()) -> ok | {error, any()}).
unregister_mod(Type, Mod) when Type =:= auth; Type =:= acl ->
    gen_server:call(?SERVER, {unregister_mod, Type, Mod}).

%% @doc Lookup authentication or ACL modules.
-spec(lookup_mods(auth | acl) -> list()).
lookup_mods(Type) ->
    case ets:lookup(?ACCESS_CONTROL_TAB, tab_key(Type)) of
        []          -> [];
        [{_, Mods}] -> Mods
    end.

tab_key(auth) -> auth_modules;
tab_key(acl)  -> acl_modules.

%% @doc Stop access control server.
stop() -> gen_server:call(?MODULE, stop).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Opts]) ->
    ets:new(?ACCESS_CONTROL_TAB, [set, named_table, protected, {read_concurrency, true}]),
    ets:insert(?ACCESS_CONTROL_TAB, {auth_modules, init_mods(auth, proplists:get_value(auth, Opts))}),
    ets:insert(?ACCESS_CONTROL_TAB, {acl_modules, init_mods(acl, proplists:get_value(acl, Opts))}),
    {ok, #state{}}.

init_mods(auth, AuthMods) ->
    [init_mod(authmod(Name), Opts) || {Name, Opts} <- AuthMods];

init_mods(acl, AclMods) ->
    [init_mod(aclmod(Name), Opts) || {Name, Opts} <- AclMods].

init_mod(Mod, Opts) ->
    {ok, State} = Mod:init(Opts), {Mod, State, 0}.

handle_call({register_mod, Type, Mod, Opts, Seq}, _From, State) ->
    Mods = lookup_mods(Type),
    Existed = lists:keyfind(Mod, 1, Mods),
    {reply, if_existed(Existed, fun() ->
                case catch Mod:init(Opts) of
                    {ok, ModState} ->
                        NewMods = lists:sort(fun({_, _, Seq1}, {_, _, Seq2}) ->
                                            Seq1 >= Seq2
                                    end, [{Mod, ModState, Seq} | Mods]),
                        ets:insert(?ACCESS_CONTROL_TAB, {tab_key(Type), NewMods}),
                        ok;
                    {error, Error} ->
                        {error, Error};
                    {'EXIT', Reason} ->
                        {error, Reason}
                end
            end), State};

handle_call({unregister_mod, Type, Mod}, _From, State) ->
    Mods = lookup_mods(Type),
    case lists:keyfind(Mod, 1, Mods) of
        false ->
            {reply, {error, not_found}, State};
        _ ->
            ets:insert(?ACCESS_CONTROL_TAB, {tab_key(Type), lists:keydelete(Mod, 1, Mods)}),
            {reply, ok, State}
    end;

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

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

authmod(Name) when is_atom(Name) ->
    mod(emqttd_auth_, Name).

aclmod(Name) when is_atom(Name) ->
    mod(emqttd_acl_, Name).

mod(Prefix, Name) ->
    list_to_atom(lists:concat([Prefix, Name])).

if_existed(false, Fun) -> Fun();
if_existed(_Mod, _Fun) -> {error, already_existed}.

