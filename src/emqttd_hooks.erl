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

-module(emqttd_hooks).

-behaviour(gen_server).

-author("Feng Lee <feng@emqtt.io>").

%% Start
-export([start_link/0]).

%% Hooks API
-export([add/3, add/4, delete/2, run/2, run/3, lookup/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

-type(hooktag() :: atom() | string() | binary()).

-export_type([hooktag/0]).

-record(callback, {tag            :: hooktag(),
                   function       :: function(),
                   init_args = [] :: list(any()),
                   priority  = 0  :: integer()}).

-record(hook, {name :: atom(), callbacks = [] :: list(#callback{})}).

-define(HOOK_TAB, mqtt_hook).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------

-spec(add(atom(), function() | {hooktag(), function()}, list(any())) -> ok).
add(HookPoint, Function, InitArgs) when is_function(Function) ->
    add(HookPoint, {undefined, Function}, InitArgs, 0);

add(HookPoint, {Tag, Function}, InitArgs) when is_function(Function) ->
    add(HookPoint, {Tag, Function}, InitArgs, 0).

-spec(add(atom(), function() | {hooktag(), function()}, list(any()), integer()) -> ok).
add(HookPoint, Function, InitArgs, Priority) when is_function(Function) ->
    add(HookPoint, {undefined, Function}, InitArgs, Priority);
add(HookPoint, {Tag, Function}, InitArgs, Priority) when is_function(Function) ->
    gen_server:call(?MODULE, {add, HookPoint, {Tag, Function}, InitArgs, Priority}).

-spec(delete(atom(), function() | {hooktag(), function()}) -> ok).
delete(HookPoint, Function) when is_function(Function) ->
    delete(HookPoint, {undefined, Function});
delete(HookPoint, {Tag, Function}) when is_function(Function) ->
    gen_server:call(?MODULE, {delete, HookPoint, {Tag, Function}}).

%% @doc Run hooks without Acc.
-spec(run(atom(), list(Arg :: any())) -> ok | stop).
run(HookPoint, Args) ->
    run_(lookup(HookPoint), Args).

-spec(run(atom(), list(Arg :: any()), any()) -> any()).
run(HookPoint, Args, Acc) ->
    run_(lookup(HookPoint), Args, Acc).

%% @private
run_([#callback{function = Fun, init_args = InitArgs} | Callbacks], Args) ->
    case apply(Fun, lists:append([Args, InitArgs])) of
        ok   -> run_(Callbacks, Args);
        stop -> stop;
        _Any -> run_(Callbacks, Args)
    end;

run_([], _Args) ->
    ok.

%% @private
run_([#callback{function = Fun, init_args = InitArgs} | Callbacks], Args, Acc) ->
    case apply(Fun, lists:append([Args, [Acc], InitArgs])) of
        ok             -> run_(Callbacks, Args, Acc);
        {ok, NewAcc}   -> run_(Callbacks, Args, NewAcc);
        stop           -> {stop, Acc};
        {stop, NewAcc} -> {stop, NewAcc};
        _Any           -> run_(Callbacks, Args, Acc)
    end;

run_([], _Args, Acc) ->
    {ok, Acc}.

-spec(lookup(atom()) -> [#callback{}]).
lookup(HookPoint) ->
    case ets:lookup(?HOOK_TAB, HookPoint) of
        [#hook{callbacks = Callbacks}] -> Callbacks;
        [] -> []
    end.

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([]) ->
    ets:new(?HOOK_TAB, [set, protected, named_table, {keypos, #hook.name}]),
    {ok, #state{}}.

handle_call({add, HookPoint, {Tag, Function}, InitArgs, Priority}, _From, State) ->
    Callback = #callback{tag = Tag, function = Function,
                         init_args = InitArgs, priority = Priority},
    {reply,
     case ets:lookup(?HOOK_TAB, HookPoint) of
         [#hook{callbacks = Callbacks}] ->
             case contain_(Tag, Function, Callbacks) of
                 false ->
                     insert_hook_(HookPoint, add_callback_(Callback, Callbacks));
                 true  ->
                     {error, already_hooked}
             end;
         [] ->
             insert_hook_(HookPoint, [Callback])
     end, State};

handle_call({delete, HookPoint, {Tag, Function}}, _From, State) ->
    {reply,
     case ets:lookup(?HOOK_TAB, HookPoint) of
         [#hook{callbacks = Callbacks}] ->
             case contain_(Tag, Function, Callbacks) of
                 true  ->
                     insert_hook_(HookPoint, del_callback_(Tag, Function, Callbacks));
                 false ->
                     {error, not_found}
             end;
         [] ->
             {error, not_found}
     end, State};

handle_call(Req, _From, State) ->
    {reply, {error, {unexpected_request, Req}}, State}.

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

del_callback_(Tag, Function, Callbacks) ->
    lists:filter(
      fun(#callback{tag = Tag1, function = Func1}) ->
        not ((Tag =:= Tag1) andalso (Function =:= Func1))
      end, Callbacks).

contain_(_Tag, _Function, []) ->
    false;
contain_(Tag, Function, [#callback{tag = Tag, function = Function}|_Callbacks]) ->
    true;
contain_(Tag, Function, [_Callback | Callbacks]) ->
    contain_(Tag, Function, Callbacks).

