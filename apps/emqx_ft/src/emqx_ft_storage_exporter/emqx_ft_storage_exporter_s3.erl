%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_storage_exporter_s3).

-include_lib("emqx/include/logger.hrl").

%% Exporter API
-export([start_export/3]).
-export([write/2]).
-export([complete/1]).
-export([discard/1]).
-export([list/1]).

-export([
    start/1,
    stop/1,
    update/2
]).

-type options() :: emqx_s3:profile_config().
-type transfer() :: emqx_ft:transfer().
-type filemeta() :: emqx_ft:filemeta().
-type exportinfo() :: #{
    transfer := transfer(),
    name := file:name(),
    uri := uri_string:uri_string(),
    timestamp := emqx_datetime:epoch_second(),
    size := _Bytes :: non_neg_integer(),
    meta => filemeta()
}.

-type export_st() :: #{
    pid := pid(),
    meta := filemeta()
}.

%%--------------------------------------------------------------------
%% Exporter behaviour
%%--------------------------------------------------------------------

-spec start_export(options(), transfer(), filemeta()) ->
    {ok, export_st()} | {error, term()}.
start_export(_Options, _Transfer, Filemeta = #{name := Filename}) ->
    Pid = spawn(fun() -> Filename end),
    #{meta => Filemeta, pid => Pid}.

-spec write(export_st(), iodata()) ->
    {ok, export_st()} | {error, term()}.
write(ExportSt, _IoData) ->
    {ok, ExportSt}.

-spec complete(export_st()) ->
    ok | {error, term()}.
complete(_ExportSt) ->
    ok.

-spec discard(export_st()) ->
    ok.
discard(_ExportSt) ->
    ok.

-spec list(options()) ->
    {ok, [exportinfo()]} | {error, term()}.
list(_Options) ->
    {ok, []}.

%%--------------------------------------------------------------------
%% Exporter behaviour (lifecycle)
%%--------------------------------------------------------------------

-spec start(options()) -> ok | {error, term()}.
start(Options) ->
    emqx_s3:start_profile(s3_profile_id(), Options).

-spec stop(options()) -> ok.
stop(_Options) ->
    ok = emqx_s3:stop_profile(s3_profile_id()).

-spec update(options(), options()) -> ok.
update(_OldOptions, NewOptions) ->
    emqx_s3:update_profile(s3_profile_id(), NewOptions).

%%--------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

s3_profile_id() ->
    atom_to_binary(?MODULE).
