%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_zone_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(OPTS, [{enable_acl, true},
               {enable_banned, false}
              ]).

all() -> emqx_ct:all(?MODULE).

t_set_get_env(_) ->
    _ = application:load(emqx),
    application:set_env(emqx, zones, [{external, ?OPTS}]),
    {ok, _} = emqx_zone:start_link(),
    ?assert(emqx_zone:get_env(external, enable_acl)),
    ?assertNot(emqx_zone:get_env(external, enable_banned)),
    ?assertEqual(defval, emqx_zone:get_env(extenal, key, defval)),
    ?assertEqual(undefined, emqx_zone:get_env(external, key)),
    ?assertEqual(undefined, emqx_zone:get_env(internal, key)),
    ?assertEqual(def, emqx_zone:get_env(internal, key, def)),
    emqx_zone:stop().

t_force_reload(_) ->
    {ok, _} = emqx_zone:start_link(),
    application:set_env(emqx, zones, [{zone, [{key, val}]}]),
    ?assertEqual(undefined, emqx_zone:get_env(zone, key)),
    ok = emqx_zone:force_reload(),
    ?assertEqual(val, emqx_zone:get_env(zone, key)),
    emqx_zone:stop().

