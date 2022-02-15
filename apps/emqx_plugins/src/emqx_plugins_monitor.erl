%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_plugins_monitor).
-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ get_plugins/0
        , start_link/0
        , aggregate_status/1
        ]).

-export([lock/1, unlock/1, lock/0, unlock/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-spec aggregate_status(list(tuple())) ->
    [#{{Name :: binary(), Vsn :: binary()} => [#{node => node(), status => running | stopped}]}].
aggregate_status(List) -> aggregate_status(List, #{}).

aggregate_status([], Acc) -> Acc;
aggregate_status([{Node, Plugins} | List], Acc) ->
    NewAcc =
        lists:foldl(fun(Plugin, SubAcc) ->
            #{<<"name">> := Name, <<"rel_vsn">> := Vsn} = Plugin,
            Key = {Name, Vsn},
            Value = #{node => Node, status => plugin_status(Plugin)},
            SubAcc#{Key => [Value | maps:get(Key, Acc, [])]}
                    end, Acc, Plugins),
    aggregate_status(List, NewAcc).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

lock(Owner) ->
    gen_server:call(?MODULE, {lock, Owner}).

unlock(Owner) ->
    gen_server:call(?MODULE, {unlock, Owner}).

init([]) ->
    {ok, #{ref => schedule_next_check(), failed => 0, lock => undefined}}.

handle_call({lock, Owner}, _From, #{lock := undefined} = State) ->
    {reply, true, State#{lock => Owner}};
handle_call({lock, Owner}, _From, #{lock := Locker} = State) ->
    case is_locker_alive(Locker) of
        true -> {reply, false, State};
        false -> {reply, true, State#{lock => Owner}}
    end;
handle_call({unlock, Owner}, _From, #{lock := Owner} = State) ->
    {reply, true, State#{lock => undefined}};
handle_call({unlock, _Owner}, _From, #{lock := Locker} = State) ->
    case is_locker_alive(Locker) of
        true -> {reply, false, State};
        false -> {reply, true, State#{lock => undefined}}
    end;
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, _Ref, check}, State = #{failed := Failed}) ->
    NewFailed = maybe_alarm(check(), Failed),
    {noreply, State#{ref => schedule_next_check(), failed => NewFailed}};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

schedule_next_check() ->
    Check = emqx_plugins:get_config(check_interval, 5000),
    emqx_misc:start_timer(Check, check).

is_locker_alive({Node, Pid}) ->
    case rpc:call(Node, erlang, is_process_alive, [Pid]) of
        {badrpc, _} -> false;
        Boolean -> Boolean
    end.

check() ->
    Nodes = mria_mnesia:running_nodes(),
    case rpc:multicall(Nodes, ?MODULE, get_plugins, [], 15000) of
        {Plugins, []} -> check_plugins(Plugins);
        {_ , BadNodes} -> {error, lists:flatten(io_lib:format("~p rpc to ~p failed", [node(), BadNodes]))}
    end.

get_plugins() ->
    {node(), emqx_plugins:list()}.

check_plugins(Plugins) ->
    StatusMap = aggregate_status(Plugins),
    case ensure_install(StatusMap) of
        {error, Bad} ->
            ?SLOG(warning, #{msg => "plugin_not_install_on_some_node", broken_plugins => Bad});
        {ok, Good} ->
            check_status(Good)
    end.

ensure_install(Status) ->
    Size = length(mria_mnesia:running_nodes()),
    {Good, Bad} =
        maps:fold(fun({Key, Value}, {GoodAcc, BadAcc}) ->
            case length(Value) =:= Size of
                true -> {GoodAcc#{Key => Value}, BadAcc};
                false -> {GoodAcc, BadAcc#{Key => Value}}
            end
                  end, {#{}, #{}}, Status),
    case map_size(Bad) =:= 0 of
        true -> {ok, Good};
        false -> {error, Bad}
    end.

check_status(_Plugins) ->
    ok.

%% alarm when failed 3 time.
maybe_alarm({error, _Reason}, Failed) when Failed >= 2 ->
    %alarm(Reason),
    0;
maybe_alarm({error, _Reason}, Failed) -> Failed + 1;
maybe_alarm(ok, _Failed) -> 0.

%% running_status: running loaded, stopped
%% config_status: not_configured disable enable
plugin_status(#{running_status := running}) -> running;
plugin_status(_) -> stopped.

-define(RESOURCE, plugins_lock).

lock() ->
    ekka_locker:acquire(?MODULE, ?RESOURCE, all, undefined).

unlock() ->
    ekka_locker:release(?MODULE, ?RESOURCE, all).
