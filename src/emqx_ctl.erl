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

-module(emqx_ctl).

-behaviour(gen_server).

-include("logger.hrl").

-export([start_link/0]).

-export([ register_command/2
        , register_command/3
        , unregister_command/1
        ]).

-export([ run_command/1
        , run_command/2
        , lookup_command/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {seq = 0}).

-type(cmd() :: atom()).

-define(SERVER, ?MODULE).
-define(TAB, emqx_command).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(register_command(cmd(), {module(), atom()}) -> ok).
register_command(Cmd, MF) when is_atom(Cmd) ->
    register_command(Cmd, MF, []).

-spec(register_command(cmd(), {module(), atom()}, list()) -> ok).
register_command(Cmd, MF, Opts) when is_atom(Cmd) ->
    cast({register_command, Cmd, MF, Opts}).

-spec(unregister_command(cmd()) -> ok).
unregister_command(Cmd) when is_atom(Cmd) ->
    cast({unregister_command, Cmd}).

cast(Msg) ->
    gen_server:cast(?SERVER, Msg).

run_command([]) ->
    run_command(help, []);
run_command([Cmd | Args]) ->
    run_command(list_to_atom(Cmd), Args).

-spec(run_command(cmd(), [string()]) -> ok | {error, term()}).
run_command(help, []) ->
    usage();
run_command(Cmd, Args) when is_atom(Cmd) ->
    case lookup_command(Cmd) of
        [{Mod, Fun}] ->
            try Mod:Fun(Args) of
                _ -> ok
            catch
                _:Reason:Stacktrace ->
                    ?ERROR("[Ctl] CMD Error:~p, Stacktrace:~p", [Reason, Stacktrace]),
                    {error, Reason}
            end;
        [] ->
            usage(), {error, cmd_not_found}
    end.

-spec(lookup_command(cmd()) -> [{module(), atom()}]).
lookup_command(Cmd) when is_atom(Cmd) ->
    case ets:match(?TAB, {{'_', Cmd}, '$1', '_'}) of
        [El] -> El;
        []   -> []
    end.

usage() ->
    io:format("Usage: ~s~n", [?MODULE]),
    [begin io:format("~80..-s~n", [""]), Mod:Cmd(usage) end
     || {_, {Mod, Cmd}, _} <- ets:tab2list(?TAB)].

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    ok = emqx_tables:new(?TAB, [protected, ordered_set]),
    {ok, #state{seq = 0}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "[Ctl] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({register_command, Cmd, MF, Opts}, State = #state{seq = Seq}) ->
    case ets:match(?TAB, {{'$1', Cmd}, '_', '_'}) of
        [] -> ets:insert(?TAB, {{Seq, Cmd}, MF, Opts});
        [[OriginSeq] | _] ->
            ?LOG(warning, "[Ctl] CMD ~s is overidden by ~p", [Cmd, MF]),
            ets:insert(?TAB, {{OriginSeq, Cmd}, MF, Opts})
    end,
    noreply(next_seq(State));

handle_cast({unregister_command, Cmd}, State) ->
    ets:match_delete(?TAB, {{'_', Cmd}, '_', '_'}),
    noreply(State);

handle_cast(Msg, State) ->
    ?LOG(error, "[Ctl] Unexpected cast: ~p", [Msg]),
    noreply(State).

handle_info(Info, State) ->
    ?LOG(error, "[Ctl] Unexpected info: ~p", [Info]),
    noreply(State).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal Function
%%------------------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

next_seq(State = #state{seq = Seq}) ->
    State#state{seq = Seq + 1}.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

register_command_test_() ->
    {setup,
        fun() ->
            {ok, InitState} = emqx_ctl:init([]),
            InitState
        end,
        fun(State) ->
            ok = emqx_ctl:terminate(shutdown, State)
        end,
        fun(State = #state{seq = Seq}) ->
            emqx_ctl:handle_cast({register_command, test0, {?MODULE, test0}, []}, State),
            [?_assertMatch([{{0,test0},{?MODULE, test0}, []}], ets:lookup(?TAB, {Seq,test0}))]
        end
    }.

-endif.

