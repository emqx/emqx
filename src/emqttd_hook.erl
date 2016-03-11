%%--------------------------------------------------------------------
%% Copyright (c) 2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_hook).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_server).

%% Start
-export([start_link/0]).

%% Hooks API
-export([add/3, add/4, delete/2, run/3, lookup/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

-record(callback, {function       :: function(),
                   init_args = [] :: list(any()),
                   priority  = 0  :: integer()}).

-record(hook, {name :: atom(), callbacks = [] :: list(#callback{})}).

-define(HOOK_TAB, mqtt_hook).

%%--------------------------------------------------------------------
%% Start API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------

-spec(add(atom(), function(), list(any())) -> ok).
add(HookPoint, Function, InitArgs) ->
    add(HookPoint, Function, InitArgs, 0).

-spec(add(atom(), function(), list(any()), integer()) -> ok).
add(HookPoint, Function, InitArgs, Priority) ->
    gen_server:call(?MODULE, {add, HookPoint, Function, InitArgs, Priority}).

-spec(delete(atom(), function()) -> ok).
delete(HookPoint, Function) ->
    gen_server:call(?MODULE, {delete, HookPoint, Function}).

-spec(run(atom(), list(any()), any()) -> any()).
run(HookPoint, Args, Acc) ->
    run_(lookup(HookPoint), Args, Acc).

%% @private
run_([#callback{function = Fun, init_args = InitArgs} | Callbacks], Args, Acc) ->
    case apply(Fun, lists:append([Args, [Acc], InitArgs])) of
        ok             -> run_(Callbacks, Args, Acc);
        {ok, NewAcc}   -> run_(Callbacks, Args, NewAcc);
        stop           -> {stop, Acc};
        {stop, NewAcc} -> {stop, NewAcc}
    end;

run_([], _Args, Acc) ->
    {ok, Acc}.

-spec(lookup(atom()) -> [#callback{}]).
lookup(HookPoint) ->
    case ets:lookup(?HOOK_TAB, HookPoint) of
        [] -> [];
        [#hook{callbacks = Callbacks}] -> Callbacks
    end.

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([]) ->
    ets:new(?HOOK_TAB, [set, protected, named_table, {keypos, #hook.name}]),
    {ok, #state{}}.

handle_call({add, HookPoint, Function, InitArgs, Priority}, _From, State) ->
    Reply =
    case ets:lookup(?HOOK_TAB, HookPoint) of
        [#hook{callbacks = Callbacks}] ->
            case lists:keyfind(Function, #callback.function, Callbacks) of
                false ->
                    Callback = #callback{function  = Function,
                                         init_args = InitArgs,
                                         priority  = Priority},
                    insert_hook_(HookPoint, add_callback_(Callback, Callbacks));
                _Callback ->
                    {error, already_hooked}
            end;
        [] ->
            Callback = #callback{function  = Function,
                                 init_args = InitArgs,
                                 priority  = Priority},
            insert_hook_(HookPoint, [Callback])
    end,
    {reply, Reply, State};

handle_call({delete, HookPoint, Function}, _From, State) ->
    Reply =
    case ets:lookup(?HOOK_TAB, HookPoint) of
        [#hook{callbacks = Callbacks}] ->
            insert_hook_(HookPoint, del_callback_(Function, Callbacks));
        [] ->
            {error, not_found}
    end,
    {reply, Reply, State};

handle_call(_Req, _From, State) ->
    {reply, ignore, State}.

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

insert_hook_(HookPoint, Callbacks) ->
    ets:insert(?HOOK_TAB, #hook{name = HookPoint, callbacks = Callbacks}), ok.

add_callback_(Callback, Callbacks) ->
    lists:keymerge(#callback.priority, Callbacks, [Callback]).

del_callback_(Function, Callbacks) ->
    lists:keydelete(Function, #callback.function, Callbacks).

