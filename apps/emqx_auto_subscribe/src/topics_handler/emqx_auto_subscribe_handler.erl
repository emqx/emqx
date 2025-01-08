%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_auto_subscribe_handler).

-export([init/1]).

-spec init(hocon:config()) -> {Module :: atom(), Config :: term()}.
init(Config) ->
    do_init(Config).

do_init(Config = #{topics := _Topics}) ->
    Options = emqx_auto_subscribe_internal:init(Config),
    {emqx_auto_subscribe_internal, Options};
do_init(_Config) ->
    erlang:error(not_supported).
