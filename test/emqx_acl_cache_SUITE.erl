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

-module(emqx_acl_cache_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_cache_k(_) ->
    error('TODO').

t_cache_v(_) ->
    error('TODO').

t_cleanup_acl_cache(_) ->
    error('TODO').

t_get_oldest_key(_) ->
    error('TODO').

t_get_newest_key(_) ->
    error('TODO').

t_get_cache_max_size(_) ->
    error('TODO').

t_get_cache_size(_) ->
    error('TODO').

t_dump_acl_cache(_) ->
    error('TODO').

t_empty_acl_cache(_) ->
    error('TODO').

t_put_acl_cache(_) ->
    error('TODO').

t_get_acl_cache(_) ->
    error('TODO').

t_is_enabled(_) ->
    error('TODO').

