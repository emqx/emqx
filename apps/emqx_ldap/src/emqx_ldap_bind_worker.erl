%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ldap_bind_worker).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eldap/include/eldap.hrl").

-export([
    on_start/4,
    on_stop/2,
    on_query/3
]).

%% ecpool connect & reconnect
-export([connect/1]).

-define(POOL_NAME_SUFFIX, "bind_worker").

%% ===================================================================
-spec on_start(binary(), hoconsc:config(), proplists:proplist(), map()) ->
    {ok, binary(), map()} | {error, _}.
on_start(InstId, #{bind_password := _} = Config, Options, State) ->
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
                msg => "starting_ldap_bind_worker",
                pool => PoolName
            }),
            emqx_resource_pool:stop(PoolName);
        _ ->
            ok
    end.

on_query(
    InstId,
    {bind, Data},
    #{
        base_tokens := DNTks,
        bind_password_tokens := PWTks,
        bind_pool_name := PoolName
    } = State
) ->
    DN = emqx_placeholder:proc_tmpl(DNTks, Data),
    Password = emqx_placeholder:proc_tmpl(PWTks, Data),

    LogMeta = #{connector => InstId, state => State},
    ?TRACE("QUERY", "ldap_connector_received", LogMeta),
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
            ok;
        {error, Reason} ->
            ?SLOG(
                error,
                LogMeta#{msg => "ldap_bind_failed", reason => Reason}
            ),
            {error, {unrecoverable_error, Reason}}
    end.

%% ===================================================================

connect(Conf) ->
    emqx_ldap:connect(Conf).

prepare_template(Config, State) ->
    do_prepare_template(maps:to_list(maps:with([bind_password], Config)), State).

do_prepare_template([{bind_password, V} | T], State) ->
    do_prepare_template(T, State#{bind_password_tokens => emqx_placeholder:preproc_tmpl(V)});
do_prepare_template([], State) ->
    State.

pool_name(InstId) ->
    <<InstId/binary, "-", ?POOL_NAME_SUFFIX>>.
