%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-behaviour(gen_server).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

%% API Function Exports
-export([start_link/0, auth/2, check_acl/3, reload_acl/0, lookup_mods/1,
         register_mod/3, register_mod/4, unregister_mod/2, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(ACCESS_CONTROL_TAB, mqtt_access_control).

-type(password() :: undefined | binary()).

-record(state, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start access control server.
-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Authenticate MQTT Client.
-spec(auth(Client :: mqtt_client(), Password :: password()) -> ok | {ok, boolean()} | {error, term()}).
auth(Client, Password) when is_record(Client, mqtt_client) ->
    auth(Client, Password, lookup_mods(auth)).
auth(_Client, _Password, []) ->
    case emqttd:env(allow_anonymous, false) of
        true  -> ok;
        false -> {error, "No auth module to check!"}
    end;
auth(Client, Password, [{Mod, State, _Seq} | Mods]) ->
    case catch Mod:check(Client, Password, State) of
        ok              -> ok;
        {ok, IsSuper}   -> {ok, IsSuper};
        ignore          -> auth(Client, Password, Mods);
        {error, Reason} -> {error, Reason};
        {'EXIT', Error} -> {error, Error}
    end.

%% @doc Check ACL
-spec(check_acl(Client, PubSub, Topic) -> allow | deny when
      Client :: mqtt_client(),
      PubSub :: pubsub(),
      Topic  :: binary()).
check_acl(Client, PubSub, Topic) when ?PS(PubSub) ->
    check_acl(Client, PubSub, Topic, lookup_mods(acl)).

check_acl(_Client, _PubSub, _Topic, []) ->
    emqttd:env(acl_nomatch, allow);
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
-spec(register_mod(auth | acl, atom(), list()) -> ok | {error, term()}).
register_mod(Type, Mod, Opts) when Type =:= auth; Type =:= acl->
    register_mod(Type, Mod, Opts, 0).

-spec(register_mod(auth | acl, atom(), list(), non_neg_integer()) -> ok | {error, term()}).
register_mod(Type, Mod, Opts, Seq) when Type =:= auth; Type =:= acl->
    gen_server:call(?SERVER, {register_mod, Type, Mod, Opts, Seq}).

%% @doc Unregister authentication or ACL module
-spec(unregister_mod(Type :: auth | acl, Mod :: atom()) -> ok | {error, not_found | term()}).
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
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([]) ->
    ets:new(?ACCESS_CONTROL_TAB, [set, named_table, protected, {read_concurrency, true}]),
    {ok, #state{}}.

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

if_existed(false, Fun) -> Fun();

if_existed(_Mod, _Fun) -> {error, already_existed}.

