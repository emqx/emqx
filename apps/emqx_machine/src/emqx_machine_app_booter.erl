%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_machine_app_booter).

%% @doc This process serves as a serialization point for starting and stopping
%% applications as part of the boot process or when joining a new cluster.  One motivation
%% for this is that a join request might start while the node is still starting its list
%% of applications (e.g. when booting the first time).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/0,

    start_apps/0,
    stop_apps/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Internal exports (for `emqx_machine_terminator' only)
-export([do_stop_apps/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% calls/casts/infos
-record(start_apps, {}).
-record(stop_apps, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_apps() ->
    gen_server:call(?MODULE, #start_apps{}, infinity).

stop_apps() ->
    gen_server:call(?MODULE, #stop_apps{}, infinity).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    %% Ensure that the stop callback is set, so that join requests concurrent to the
    %% startup are serialized here.
    %% It would still be problematic if a join request arrives before this process is
    %% started, though.
    ekka:callback(stop, fun emqx_machine_boot:stop_apps/0),
    State = #{},
    {ok, State}.

handle_call(#start_apps{}, _From, State) ->
    handle_start_apps(),
    {reply, ok, State};
handle_call(#stop_apps{}, _From, State) ->
    do_stop_apps(),
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%% Callers who wish to stop applications should not call this directly, and instead use
%% `stop_apps/0`.  The only exception is `emqx_machine_terminator': it should call this
%% block directly in its try-catch block, without the possibility of crashing this
%% process.
do_stop_apps() ->
    ?SLOG(notice, #{msg => "stopping_emqx_apps"}),
    _ = emqx_alarm_handler:unload(),
    ok = emqx_conf_app:unset_config_loaded(),
    ok = emqx_plugins:ensure_stopped(),
    lists:foreach(fun stop_one_app/1, lists:reverse(emqx_machine_boot:sorted_reboot_apps())).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

handle_start_apps() ->
    ?SLOG(notice, #{msg => "(re)starting_emqx_apps"}),
    lists:foreach(fun start_one_app/1, emqx_machine_boot:sorted_reboot_apps()),
    ?tp(emqx_machine_boot_apps_started, #{}).

start_one_app(App) ->
    ?SLOG(debug, #{msg => "starting_app", app => App}),
    case application:ensure_all_started(App, emqx_machine_boot:restart_type(App)) of
        {ok, Apps} ->
            ?SLOG(debug, #{msg => "started_apps", apps => Apps});
        {error, Reason} ->
            ?SLOG(critical, #{msg => "failed_to_start_app", app => App, reason => Reason}),
            error({failed_to_start_app, App, Reason})
    end.

stop_one_app(App) ->
    ?SLOG(debug, #{msg => "stopping_app", app => App}),
    try
        _ = application:stop(App)
    catch
        C:E ->
            ?SLOG(error, #{
                msg => "failed_to_stop_app",
                app => App,
                exception => C,
                reason => E
            })
    end.
