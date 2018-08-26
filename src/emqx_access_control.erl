%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_access_control).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/0]).
-export([authenticate/2]).
-export([check_acl/3, reload_acl/0, lookup_mods/1]).
-export([register_mod/3, register_mod/4, unregister_mod/2]).
-export([stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

-type(password() :: undefined | binary()).

-record(state, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Start access control server.
-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    case gen_server:start_link({local, ?SERVER}, ?MODULE, [], []) of
        {ok, Pid} ->
            ok = register_default_mod(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

register_default_mod() ->
    case emqx_config:get_env(acl_file) of
        undefined -> ok;
        File ->
            emqx_access_control:register_mod(acl, emqx_acl_internal, [File])
    end.

%% @doc Authenticate Client.
-spec(authenticate(Client :: client(), Password :: password())
      -> ok | {ok, boolean()} | {error, term()}).
authenticate(Client, Password) when is_record(Client, client) ->
    authenticate(Client, Password, lookup_mods(auth)).

authenticate(#client{zone = Zone}, _Password, []) ->
    case emqx_zone:get_env(Zone, allow_anonymous, false) of
        true  -> ok;
        false -> {error, "No auth module to check!"}
    end;

authenticate(Client, Password, [{Mod, State, _Seq} | Mods]) ->
    case catch Mod:check(Client, Password, State) of
        ok              -> ok;
        {ok, IsSuper}   -> {ok, IsSuper};
        ignore          -> authenticate(Client, Password, Mods);
        {error, Reason} -> {error, Reason};
        {'EXIT', Error} -> {error, Error}
    end.

%% @doc Check ACL
-spec(check_acl(client(), pubsub(), topic()) -> allow | deny).
check_acl(Client, PubSub, Topic) when ?PS(PubSub) ->
    CacheEnabled = emqx_acl_cache:is_enabled(),
    check_acl(Client, PubSub, Topic, lookup_mods(acl), CacheEnabled).

check_acl(Client, PubSub, Topic, AclMods, false) ->
    do_check_acl(Client, PubSub, Topic, AclMods);
check_acl(Client, PubSub, Topic, AclMods, true) ->
    case emqx_acl_cache:get_acl_cache(PubSub, Topic) of
        not_found ->
            AclResult = do_check_acl(Client, PubSub, Topic, AclMods),
            emqx_acl_cache:put_acl_cache(PubSub, Topic, AclResult),
            AclResult;
        AclResult ->
            AclResult
    end.

%% @doc Reload ACL Rules.
-spec(reload_acl() -> list(ok | {error, already_existed})).
reload_acl() ->
    [Mod:reload_acl(State) || {Mod, State, _Seq} <- lookup_mods(acl)].

%% @doc Register Authentication or ACL module.
-spec(register_mod(auth | acl, atom(), list()) -> ok | {error, term()}).
register_mod(Type, Mod, Opts) when Type =:= auth; Type =:= acl ->
    register_mod(Type, Mod, Opts, 0).

-spec(register_mod(auth | acl, atom(), list(), non_neg_integer())
      -> ok | {error, term()}).
register_mod(Type, Mod, Opts, Seq) when Type =:= auth; Type =:= acl->
    gen_server:call(?SERVER, {register_mod, Type, Mod, Opts, Seq}).

%% @doc Unregister authentication or ACL module
-spec(unregister_mod(Type :: auth | acl, Mod :: atom())
      -> ok | {error, not_found | term()}).
unregister_mod(Type, Mod) when Type =:= auth; Type =:= acl ->
    gen_server:call(?SERVER, {unregister_mod, Type, Mod}).

%% @doc Lookup authentication or ACL modules.
-spec(lookup_mods(auth | acl) -> list()).
lookup_mods(Type) ->
    case ets:lookup(?TAB, tab_key(Type)) of
        [] -> [];
        [{_, Mods}] -> Mods
    end.

tab_key(auth) -> auth_modules;
tab_key(acl)  -> acl_modules.

%% @doc Stop access control server.
stop() ->
    gen_server:stop(?MODULE, normal, infinity).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = emqx_tables:new(?TAB, [set, protected, {read_concurrency, true}]),
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
                        ets:insert(?TAB, {tab_key(Type), NewMods}),
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
            _ = ets:insert(?TAB, {tab_key(Type), lists:keydelete(Mod, 1, Mods)}),
            {reply, ok, State}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    emqx_logger:error("[AccessControl] unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[AccessControl] unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    emqx_logger:error("[AccessControl] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_check_acl(#client{zone = Zone}, _PubSub, _Topic, []) ->
    emqx_zone:get_env(Zone, acl_nomatch, deny);
do_check_acl(Client, PubSub, Topic, [{Mod, State, _Seq}|AclMods]) ->
    case Mod:check_acl({Client, PubSub, Topic}, State) of
        allow  -> allow;
        deny   -> deny;
        ignore -> do_check_acl(Client, PubSub, Topic, AclMods)
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

if_existed(false, Fun) ->
    Fun();
if_existed(_Mod, _Fun) ->
    {error, already_existed}.
