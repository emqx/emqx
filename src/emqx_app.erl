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

-module(emqx_app).

-behaviour(application).

-export([start/2, stop/1]).

-define(APP, emqx).

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    %% We'd like to configure the primary logger level here, rather than set the
    %%   kernel config `logger_level` before starting the erlang vm.
    %% This is because the latter approach an annoying debug msg will be printed out:
    %%   "[debug] got_unexpected_message {'EXIT',<0.1198.0>,normal}"
    logger:set_primary_config(level, application:get_env(emqx, primary_log_level, error)),

    print_banner(),
    ekka:start(),
    {ok, Sup} = emqx_sup:start_link(),
    emqx_modules:load(),
    emqx_plugins:init(),
    emqx_plugins:load(),
    emqx_listeners:start(),
    start_autocluster(),
    register(emqx, self()),
    print_vsn(),
    {ok, Sup}.

-spec(stop(State :: term()) -> term()).
stop(_State) ->
    emqx_listeners:stop(),
    emqx_modules:unload().

%%--------------------------------------------------------------------
%% Print Banner
%%--------------------------------------------------------------------

print_banner() ->
    io:format("Starting ~s on node ~s~n", [?APP, node()]).

print_vsn() ->
    {ok, Descr} = application:get_key(description),
    {ok, Vsn} = application:get_key(vsn),
    io:format("~s ~s is running now!~n", [Descr, Vsn]).

%%--------------------------------------------------------------------
%% Autocluster
%%--------------------------------------------------------------------

start_autocluster() ->
    ekka:callback(prepare, fun emqx:shutdown/1),
    ekka:callback(reboot,  fun emqx:reboot/0),
    ekka:autocluster(?APP).

