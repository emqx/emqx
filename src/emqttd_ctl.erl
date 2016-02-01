%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>. All Rights Reserved.
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
%%% @doc emqttd control
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_ctl).

-behaviour(gen_server).

-include("emqttd.hrl").

-include("emqttd_cli.hrl").

-define(SERVER, ?MODULE).

%% API Function Exports
-export([start_link/0,
         register_cmd/3,
         unregister_cmd/1,
         run/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {seq = 0}).

-define(CMD_TAB, mqttd_ctl_cmd).

%%%=============================================================================
%%% API
%%%=============================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc Register a command
%% @end
%%------------------------------------------------------------------------------
-spec register_cmd(atom(), {module(), atom()}, list()) -> ok.
register_cmd(Cmd, MF, Opts) ->
    cast({register_cmd, Cmd, MF, Opts}).

%%------------------------------------------------------------------------------
%% @doc Unregister a command
%% @end
%%------------------------------------------------------------------------------
-spec unregister_cmd(atom()) -> ok.
unregister_cmd(Cmd) ->
    cast({unregister_cmd, Cmd}).

cast(Msg) ->
    gen_server:cast(?SERVER, Msg).

%%------------------------------------------------------------------------------
%% @doc Run a command
%% @end
%%------------------------------------------------------------------------------
run([]) -> usage();

run(["help"]) -> usage();

run([CmdS|Args]) ->
    Cmd = list_to_atom(CmdS),
    case ets:match(?CMD_TAB, {{'_', Cmd}, '$1', '_'}) of
        [[{Mod, Fun}]] -> Mod:Fun(Args);
        [] -> usage() 
    end.
    
%%------------------------------------------------------------------------------
%% @doc Usage
%% @end
%%------------------------------------------------------------------------------
usage() ->
    ?PRINT("Usage: ~s~n", [?MODULE]),
    [begin ?PRINT("~80..-s~n", [""]), Mod:Cmd(usage) end
        || {_, {Mod, Cmd}, _} <- ets:tab2list(?CMD_TAB)].

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

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

%%%=============================================================================
%%% Internal Function Definitions
%%%=============================================================================

noreply(State) ->
    {noreply, State, hibernate}.

next_seq(State = #state{seq = Seq}) ->
    State#state{seq = Seq + 1}.


