%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ldap_bind_worker).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eldap/include/eldap.hrl").

-export([
    on_start/4,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

%% ecpool connect & reconnect
-export([connect/1]).

-define(POOL_NAME_SUFFIX, "bind_worker").

%% ===================================================================
-spec on_start(binary(), hocon:config(), proplists:proplist(), map()) ->
    {ok, binary(), map()} | {error, _}.
on_start(InstId, #{method := #{bind_password := _}} = Config, Options, State) ->
    PoolName = pool_name(InstId),
    ?SLOG(info, #{
        msg => "starting_ldap_bind_worker",
        pool => PoolName
    }),

    ok = emqx_resource:allocate_resource(InstId, ?MODULE, PoolName),
    case emqx_resource_pool:start(PoolName, ?MODULE, Options) of
        ok ->
            {ok, prepare_template(Config, State#{bind_pool_name => PoolName})};
        {error, Reason} ->
            ?tp(
                ldap_bind_worker_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end;
on_start(_InstId, _Config, _Options, State) ->
    {ok, State}.

on_stop(InstId, _State) ->
    case emqx_resource:get_allocated_resources(InstId) of
        #{?MODULE := PoolName} ->
            ?SLOG(info, #{
                msg => "stopping_ldap_bind_worker",
                pool => PoolName
            }),
            emqx_resource_pool:stop(PoolName);
        _ ->
            ok
    end.

on_query(
    InstId,
    {bind, DN, Data},
    #{
        bind_password := PWTks,
        bind_pool_name := PoolName
    } = State
) ->
    Password = emqx_placeholder:proc_tmpl(PWTks, Data),

    LogMeta = #{connector => InstId, state => State},
    ?TRACE("QUERY", "ldap_connector_about_to_bind", LogMeta),
    case
        ecpool:pick_and_do(
            PoolName,
            {eldap, simple_bind, [DN, Password]},
            handover
        )
    of
        ok ->
            ?tp(
                ldap_connector_query_return,
                #{result => ok}
            ),
            {ok, #{result => ok}};
        {error, 'invalidCredentials'} ->
            {ok, #{result => 'invalidCredentials'}};
        {error, Reason} ->
            ?SLOG(
                error,
                LogMeta#{msg => "ldap_bind_failed", reason => emqx_utils:redact(Reason)}
            ),
            {error, {unrecoverable_error, Reason}}
    end.

on_get_status(_InstId, #{bind_pool_name := PoolName}) ->
    emqx_ldap:get_status_with_poolname(PoolName);
on_get_status(_InstId, _) ->
    ?status_connected.

%% ===================================================================

connect(Conf) ->
    emqx_ldap:connect(Conf).

prepare_template(#{method := #{bind_password := V}}, State) ->
    %% This is sensitive data
    %% to reduce match cases, here we reuse the existing sensitive filter key: bind_password
    State#{bind_password => emqx_placeholder:preproc_tmpl(V)}.

pool_name(InstId) ->
    <<InstId/binary, "-", ?POOL_NAME_SUFFIX>>.
