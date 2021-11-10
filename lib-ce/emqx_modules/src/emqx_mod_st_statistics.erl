%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_st_statistics).

-behaviour(emqx_gen_mod).

-include_lib("emqx/include/logger.hrl").

-logger_header("[st_statistics]").

%% emqx_gen_mod callbacks
-export([ load/1
        , unload/1
        , description/0
        ]).

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

-spec(load(list()) -> ok).
load(_Env) ->
    _ = emqx_st_statistics_app:start(),
    ok.

-spec(unload(list()) -> ok).
unload(_Env) ->
    emqx_st_statistics_app:stop(undefined).

description() ->
    "EMQ X Slow Topic Statistics Module".

%%--------------------------------------------------------------------
