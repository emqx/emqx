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

-module(emqx_cm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_reg_unreg_channel(_) ->
    error(not_implemented).

t_get_set_chan_attrs(_) ->
    error(not_implemented).

t_get_set_chan_stats(_) ->
    error(not_implemented).

t_open_session(_) ->
    error(not_implemented).

t_discard_session(_) ->
    error(not_implemented).

t_takeover_session(_) ->
    error(not_implemented).

t_lookup_channels(_) ->
    error(not_implemented).

t_lock_clientid(_) ->
    error(not_implemented).

t_unlock_clientid(_) ->
    error(not_implemented).

