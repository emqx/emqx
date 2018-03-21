%%--------------------------------------------------------------------
%% Copyright © 2013-2018 EMQ Inc. All rights reserved.
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

-include("emqx_mqtt.hrl").

%% Application callbacks
-export([start/2, stop/1]).

-define(APP, emqx).

%%--------------------------------------------------------------------
%% Application Callbacks
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    print_banner(),
    ekka:start(),
    {ok, Sup} = emqx_sup:start_link(),
    ok = register_acl_mod(),
    emqx_modules:load(),
    start_autocluster(),
    register(emqx, self()),
    print_vsn(),
    {ok, Sup}.

-spec(stop(State :: term()) -> term()).
stop(_State) ->
    emqx_modules:unload(),
    catch emqx:stop_listeners().

%%--------------------------------------------------------------------
%% Print Banner
%%--------------------------------------------------------------------

print_banner() ->
    io:format("Starting ~s on node ~s~n", [?APP, node()]).

print_vsn() ->
    {ok, Vsn} = application:get_key(vsn),
    io:format("~s ~s is running now!~n", [?APP, Vsn]).

%%--------------------------------------------------------------------
%% Register default ACL File
%%--------------------------------------------------------------------

register_acl_mod() ->
    case emqx:env(acl_file) of
        {ok, File} -> emqx_access_control:register_mod(acl, emqx_acl_internal, [File]);
        undefined  -> ok
    end.

%%--------------------------------------------------------------------
%% Autocluster
%%--------------------------------------------------------------------

start_autocluster() ->
    ekka:callback(prepare, fun emqx:shutdown/1),
    ekka:callback(reboot,  fun emqx:reboot/0),
    ekka:autocluster(?APP, fun after_autocluster/0).

after_autocluster() ->
    emqx_plugins:init(),
    emqx_plugins:load(),
    emqx:start_listeners().

