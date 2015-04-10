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
%%% emqttd authentication.
%%%
%%% TODO: 
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_auth).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-export([start_link/1, login/2, add_module/2, remove_module/1, all_modules/0, stop/0]).

-behavior(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(AUTH_TABLE, mqtt_auth).

%%%=============================================================================
%%% Auth behavihour
%%%=============================================================================

-ifdef(use_specs).

-callback check(User, Password, State) -> ok | ignore | {error, string()} when
    User     :: mqtt_user(),
    Password :: binary(),
    State    :: any().

-callback description() -> string().

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
        [{check, 3}, {description, 0}];
behaviour_info(_Other) ->
        undefined.

-endif.

-spec start_link(list()) -> {ok, pid()} | ignore | {error, any()}.
start_link(AuthMods) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [AuthMods], []).

-spec login(mqtt_user(), undefined | binary()) -> ok | {error, string()}.
login(User, Password) when is_record(User, mqtt_user) ->
    [{_, AuthMods}] = ets:lookup(?AUTH_TABLE, auth_modules),
    check(User, Password, AuthMods).

check(_User, _Password, []) ->
    {error, "No auth module to check!"};
check(User, Password, [{Mod, State} | Mods]) ->
    case Mod:check(User, Password, State) of
        ok -> ok;
        {error, Reason} -> {error, Reason};
        ignore -> check(User, Password, Mods)
    end.

add_module(Mod, Opts) ->
    gen_server:call(?MODULE, {add_module, Mod, Opts}).

remove_module(Mod) ->
    gen_server:call(?MODULE, {remove_module, Mod}).

all_modules() ->
    case ets:lookup(?AUTH_TABLE, auth_modules) of
        [] -> [];
        [{_, AuthMods}] -> AuthMods
    end.

stop() ->
    gen_server:call(?MODULE, stop).

init([AuthMods]) ->
	ets:new(?AUTH_TABLE, [set, named_table, protected, {read_concurrency, true}]),
    Modules = lists:map(
                fun({Mod, Opts}) ->
                        AuthMod = authmod(Mod),
                        {ok, State} = AuthMod:init(Opts),
                        {AuthMod, State}
                end, AuthMods),
    ets:insert(?AUTH_TABLE, {auth_modules, Modules}),
	{ok, state}.

handle_call({add_module, Mod, Opts}, _From, State) ->
    AuthMods = all_modules(),
    Reply =
    case lists:keyfind(Mod, 1, AuthMods) of
        false -> 
            case catch Mod:init(Opts) of
                {ok, ModState} -> 
                    ets:insert(?AUTH_TABLE, {auth_modules, [{Mod, ModState}|AuthMods]}),
                    ok;
                {error, Reason} ->
                    {error, Reason};
                {'EXIT', Error} ->
                    {error, Error}
            end;
        _ -> 
            {error, existed}
    end,
    {reply, Reply, State};

handle_call({remove_module, Mod}, _From, State) ->
    AuthMods = all_modules(),
    Reply =
    case lists:keyfind(Mod, 1, AuthMods) of
        false -> 
            {error, not_found}; 
        _ -> 
            ets:insert(?AUTH_TABLE, {auth_modules, lists:keydelete(Mod, 1, AuthMods)}), ok
    end,
    {reply, Reply, State};

handle_call(stop, _From, State) ->
	{stop, normal, ok, State}.

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

