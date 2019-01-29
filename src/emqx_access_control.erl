%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([check_acl/3, reload_acl/0]).
-export([register_mod/3, register_mod/4, unregister_mod/2]).
-export([lookup_mods/1]).
-export([stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Start access control server.
-spec(start_link() -> {ok, pid()} | {error, term()}).
start_link() ->
    start_with(fun register_default_acl/0).

start_with(Fun) ->
    case gen_server:start_link({local, ?SERVER}, ?MODULE, [], []) of
        {ok, Pid} ->
            Fun(), {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

register_default_acl() ->
    case emqx_config:get_env(acl_file) of
        undefined -> ok;
        File -> register_mod(acl, emqx_acl_internal, [File])
    end.

-spec(authenticate(emqx_types:credentials(), emqx_types:password())
      -> ok | {ok, map()} | {continue, map()} | {error, term()}).
authenticate(Credentials, Password) ->
    authenticate(Credentials, Password, lookup_mods(auth)).

authenticate(Credentials, _Password, []) ->
    Zone = maps:get(zone, Credentials, undefined),
    case emqx_zone:get_env(Zone, allow_anonymous, false) of
        true  -> ok;
        false -> {error, auth_modules_not_found}
    end;

authenticate(Credentials, Password, [{Mod, State, _Seq} | Mods]) ->
    case catch Mod:check(Credentials, Password, State) of
        ok -> ok;
        {ok, IsSuper} when is_boolean(IsSuper) ->
            {ok, #{is_superuser => IsSuper}};
        {ok, Result} when is_map(Result) ->
            {ok, Result};
        {continue, Result} when is_map(Result) ->
            {continue, Result};
        ignore ->
            authenticate(Credentials, Password, Mods);
        {error, Reason} ->
            {error, Reason};
        {'EXIT', Error} ->
            {error, Error}
    end.

%% @doc Check ACL
-spec(check_acl(emqx_types:credentials(), emqx_types:pubsub(), emqx_types:topic()) -> allow | deny).
check_acl(Credentials, PubSub, Topic) when PubSub =:= publish; PubSub =:= subscribe ->
    check_acl(Credentials, PubSub, Topic, lookup_mods(acl), emqx_acl_cache:is_enabled()).

check_acl(Credentials, PubSub, Topic, AclMods, false) ->
    do_check_acl(Credentials, PubSub, Topic, AclMods);
check_acl(Credentials, PubSub, Topic, AclMods, true) ->
    case emqx_acl_cache:get_acl_cache(PubSub, Topic) of
        not_found ->
            AclResult = do_check_acl(Credentials, PubSub, Topic, AclMods),
            emqx_acl_cache:put_acl_cache(PubSub, Topic, AclResult),
            AclResult;
        AclResult ->
            AclResult
    end.

do_check_acl(#{zone := Zone}, _PubSub, _Topic, []) ->
    emqx_zone:get_env(Zone, acl_nomatch, deny);
do_check_acl(Credentials, PubSub, Topic, [{Mod, State, _Seq}|AclMods]) ->
    case Mod:check_acl({Credentials, PubSub, Topic}, State) of
        allow  -> allow;
        deny   -> deny;
        ignore -> do_check_acl(Credentials, PubSub, Topic, AclMods)
    end.

-spec(reload_acl() -> list(ok | {error, term()})).
reload_acl() ->
    [Mod:reload_acl(State) || {Mod, State, _Seq} <- lookup_mods(acl)].

%% @doc Register an Auth/ACL module.
-spec(register_mod(auth | acl, module(), list()) -> ok | {error, term()}).
register_mod(Type, Mod, Opts) when Type =:= auth; Type =:= acl ->
    register_mod(Type, Mod, Opts, 0).

-spec(register_mod(auth | acl, module(), list(), non_neg_integer())
      -> ok | {error, term()}).
register_mod(Type, Mod, Opts, Seq) when Type =:= auth; Type =:= acl->
    gen_server:call(?SERVER, {register_mod, Type, Mod, Opts, Seq}).

%% @doc Unregister an Auth/ACL module.
-spec(unregister_mod(auth | acl, module()) -> ok | {error, not_found | term()}).
unregister_mod(Type, Mod) when Type =:= auth; Type =:= acl ->
    gen_server:call(?SERVER, {unregister_mod, Type, Mod}).

%% @doc Lookup all Auth/ACL modules.
-spec(lookup_mods(auth | acl) -> list()).
lookup_mods(Type) ->
    case ets:lookup(?TAB, tab_key(Type)) of
        [] -> [];
        [{_, Mods}] -> Mods
    end.

tab_key(auth) -> auth_modules;
tab_key(acl)  -> acl_modules.

stop() ->
    gen_server:stop(?SERVER, normal, infinity).

%%-----------------------------------------------------------------------------
%% gen_server callbacks
%%-----------------------------------------------------------------------------

init([]) ->
    ok = emqx_tables:new(?TAB, [set, protected, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({register_mod, Type, Mod, Opts, Seq}, _From, State) ->
    Mods = lookup_mods(Type),
    reply(case lists:keymember(Mod, 1, Mods) of
              true  -> {error, already_exists};
              false ->
                    try Mod:init(Opts) of
                        {ok, ModState} ->
                            NewMods = lists:sort(fun({_, _, Seq1}, {_, _, Seq2}) ->
                                                        Seq1 >= Seq2
                                                end, [{Mod, ModState, Seq} | Mods]),
                            ets:insert(?TAB, {tab_key(Type), NewMods}),
                            ok
                    catch
                        _:Error ->
                            emqx_logger:error("[AccessControl] Failed to init ~s: ~p", [Mod, Error]),
                            {error, Error}
                    end
          end, State);

handle_call({unregister_mod, Type, Mod}, _From, State) ->
    Mods = lookup_mods(Type),
    reply(case lists:keyfind(Mod, 1, Mods) of
              false ->
                  {error, not_found};
              {Mod, _ModState, _Seq} ->
                  ets:insert(?TAB, {tab_key(Type), lists:keydelete(Mod, 1, Mods)}), ok
          end, State);

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Req, _From, State) ->
    emqx_logger:error("[AccessControl] unexpected request: ~p", [Req]),
    {reply, ignored, State}.

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

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

reply(Reply, State) ->
    {reply, Reply, State}.

