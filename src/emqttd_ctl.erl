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

-module(emqttd_ctl).

-behaviour(gen_server).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_cli.hrl").

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/0, register_cmd/2, register_cmd/3, unregister_cmd/1,
         lookup/1, run/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {seq = 0}).

-define(CMD_TAB, mqttd_ctl_cmd).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register a command
-spec(register_cmd(atom(), {module(), atom()}) -> ok).
register_cmd(Cmd, MF) ->
    register_cmd(Cmd, MF, []).

%% @doc Register a command with opts
-spec(register_cmd(atom(), {module(), atom()}, list()) -> ok).
register_cmd(Cmd, MF, Opts) ->
    cast({register_cmd, Cmd, MF, Opts}).

%% @doc Unregister a command
-spec(unregister_cmd(atom()) -> ok).
unregister_cmd(Cmd) ->
    cast({unregister_cmd, Cmd}).

cast(Msg) -> gen_server:cast(?SERVER, Msg).

%% @doc Run a command
-spec(run([string()]) -> any()).
run([]) -> usage(), ok;

run(["help"]) -> usage(), ok;

run(["set"] = CmdS) when length(CmdS) =:= 1 ->
    emqttd_cli_config:set_usage(), ok;

run(["set" | _] = CmdS) ->
    emqttd_cli_config:run(["config" | CmdS]), ok;

run(["show" | _] = CmdS) ->
    emqttd_cli_config:run(["config" | CmdS]), ok;

run([CmdS|Args]) ->
    case lookup(list_to_atom(CmdS)) of
        [{Mod, Fun}] ->
            try Mod:Fun(Args) of
               _ -> ok
            catch
                _:Reason ->
                    io:format("Reason:~p, get_stacktrace:~p~n",
                              [Reason, erlang:get_stacktrace()]),
                    {error, Reason}
            end;
        [] ->
            usage(),
            {error, cmd_not_found}
    end.

%% @doc Lookup a command
-spec(lookup(atom()) -> [{module(), atom()}]).
lookup(Cmd) ->
    case ets:match(?CMD_TAB, {{'_', Cmd}, '$1', '_'}) of
        [El] -> El;
        []   -> []
    end.

%% @doc Usage
usage() ->
    ?PRINT("Usage: ~s~n", [?MODULE]),
    [begin ?PRINT("~80..-s~n", [""]), Mod:Cmd(usage) end
        || {_, {Mod, Cmd}, _} <- ets:tab2list(?CMD_TAB)].

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ets:new(?CMD_TAB, [ordered_set, named_table, protected]),
    {ok, #state{seq = 0}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({register_cmd, Cmd, MF, Opts}, State = #state{seq = Seq}) ->
    case ets:match(?CMD_TAB, {{'$1', Cmd}, '_', '_'}) of
        [] ->
            ets:insert(?CMD_TAB, {{Seq, Cmd}, MF, Opts});
        [[OriginSeq] | _] ->
            lager:warning("CLI: ~s is overidden by ~p", [Cmd, MF]),
            ets:insert(?CMD_TAB, {{OriginSeq, Cmd}, MF, Opts})
    end,
    noreply(next_seq(State));

handle_cast({unregister_cmd, Cmd}, State) ->
    ets:match_delete(?CMD_TAB, {{'_', Cmd}, '_', '_'}),
    noreply(State);

handle_cast(_Msg, State) ->
    noreply(State).

handle_info(_Info, State) ->
    noreply(State).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Function Definitions
%%--------------------------------------------------------------------

noreply(State) ->
    {noreply, State, hibernate}.

next_seq(State = #state{seq = Seq}) ->
    State#state{seq = Seq + 1}.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

register_cmd_test_() ->
    {setup, 
        fun() ->
            {ok, InitState} = emqttd_ctl:init([]),
            InitState
        end,
        fun(State) ->
            ok = emqttd_ctl:terminate(shutdown, State)
        end,
        fun(State = #state{seq = Seq}) -> 
                emqttd_ctl:handle_cast({register_cmd, test0, {?MODULE, test0}, []}, State),
                [?_assertMatch([{{0,test0},{?MODULE, test0}, []}], ets:lookup(?CMD_TAB, {Seq,test0}))]
        end
    }.

-endif.
