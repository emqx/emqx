%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_app).

-behaviour(application).

-export([ start/2
        , stop/1
        , get_description/0
        , get_release/0
        ]).

-define(APP, emqx).

-include("emqx_release.hrl").

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    print_banner(),
    ekka:start(),
    {ok, Sup} = emqx_sup:start_link(),
    ok = start_autocluster(),
    ok = emqx_plugins:init(),
    _ = emqx_plugins:load(),
    emqx_boot:is_enabled(listeners)
      andalso (ok = emqx_listeners:start()),
    register(emqx, self()),
    ok = emqx_alarm_handler:load(),
    print_vsn(),
    {ok, Sup}.

-spec(stop(State :: term()) -> term()).
stop(_State) ->
    ok = emqx_alarm_handler:unload(),
    emqx_boot:is_enabled(listeners)
      andalso emqx_listeners:stop().

%%--------------------------------------------------------------------
%% Print Banner
%%--------------------------------------------------------------------

print_banner() ->
    io:format("Starting ~s on node ~s~n", [?APP, node()]).

print_vsn() ->
    io:format("~s ~s is running now!~n", [get_description(), get_release()]).

get_description() ->
    {ok, Descr0} = application:get_key(?APP, description),
    case os:getenv("EMQX_DESCRIPTION") of
        false -> Descr0;
        "" -> Descr0;
        Str -> string:strip(Str, both, $\n)
    end.

get_release() ->
    case lists:keyfind(emqx_vsn, 1, ?MODULE:module_info(compile)) of
        false ->
            release_in_macro();
        {_, Vsn} ->
            VsnStr = release_in_macro(),
            1 = string:str(Vsn, VsnStr), %% assert
            Vsn
    end.

release_in_macro() ->
    element(2, ?EMQX_RELEASE).

%%--------------------------------------------------------------------
%% Autocluster
%%--------------------------------------------------------------------
start_autocluster() ->
    ekka:callback(prepare, fun emqx:shutdown/1),
    ekka:callback(reboot,  fun emqx:reboot/0),
    _ = ekka:autocluster(?APP), %% returns 'ok' or a pid or 'any()' as in spec
    ok.
