%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ldap).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eldap/include/eldap.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

%% ecpool connect & reconnect
-export([connect/1]).

-export([namespace/0, roots/0, fields/1, desc/1]).

-export([do_get_status/1, get_status_with_poolname/1]).
-export([search/2]).

-define(LDAP_HOST_OPTIONS, #{
    default_port => 389
}).
-define(REDACT_VAL, "******").

-type params_tokens() :: #{atom() => list()}.
-type state() ::
    #{
        pool_name := binary(),
        base_tokens := params_tokens(),
        filter_tokens := params_tokens()
    }.

%%=====================================================================
%% Hocon schema

namespace() -> "ldap".

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server, server()},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {username, fun ensure_username/1},
        {password, emqx_connector_schema_lib:password_field()},
        {base_dn,
            ?HOCON(binary(), #{
                desc => ?DESC(base_dn),
                required => true,
                example => <<"uid=${username},ou=testdevice,dc=emqx,dc=io">>,
                validator => fun emqx_schema:non_empty_string/1
            })},
        {filter,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(filter),
                    default => <<"(objectClass=mqttUser)">>,
                    example => <<"(& (objectClass=mqttUser) (uid=${username}))">>,
                    validator => fun emqx_schema:non_empty_string/1
                }
            )},
        {request_timeout,
            ?HOCON(emqx_schema:timeout_duration_ms(), #{
                desc => ?DESC(request_timeout),
                default => <<"10s">>
            })},
        {ssl,
            ?HOCON(?R_REF(?MODULE, ssl), #{
                default => #{<<"enable">> => false},
                desc => ?DESC(emqx_connector_schema_lib, "ssl")
            })}
    ];
fields(ssl) ->
    Schema = emqx_schema:client_ssl_opts_schema(#{}),
    lists:keydelete("user_lookup_fun", 1, Schema);
fields(bind_opts) ->
    [
        {bind_password,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(bind_password),
                    default => <<"${password}">>,
                    example => <<"${password}">>,
                    sensitive => true,
                    validator => fun emqx_schema:non_empty_string/1
                }
            )}
    ].

desc(ssl) ->
    ?DESC(emqx_connector_schema_lib, "ssl");
desc(_) ->
    undefined.

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?LDAP_HOST_OPTIONS).

ensure_username(required) ->
    true;
ensure_username(Field) ->
    emqx_connector_schema_lib:username(Field).

%% ===================================================================
resource_type() -> ldap.

callback_mode() -> always_sync.

-spec on_start(binary(), hocon:config()) -> {ok, state()} | {error, _}.
on_start(
    InstId,
    #{
        server := Server,
        pool_size := PoolSize,
        ssl := SSL
    } = Config
) ->
    HostPort = emqx_schema:parse_server(Server, ?LDAP_HOST_OPTIONS),
    ?SLOG(info, #{
        msg => "starting_ldap_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
    }),

    Config2 = maps:merge(Config, HostPort),
    Config3 =
        case maps:get(enable, SSL) of
            true ->
                Config2#{sslopts => emqx_tls_lib:to_client_opts(SSL)};
            false ->
                Config2
        end,

    Options = [
        {pool_size, PoolSize},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {options, Config3}
    ],

    case emqx_resource_pool:start(InstId, ?MODULE, [{log_tag, "eldap_info"} | Options]) of
        ok ->
            emqx_ldap_bind_worker:on_start(
                InstId,
                Config,
                [{log_tag, "eldap_bind_info"} | Options],
                prepare_template(Config, #{pool_name => InstId})
            );
        {error, Reason} ->
            ?tp(
                ldap_connector_start_failed,
                #{error => emqx_utils:redact(Reason)}
            ),
            {error, Reason}
    end.

on_stop(InstId, State) ->
    ?SLOG(info, #{
        msg => "stopping_ldap_connector",
        connector => InstId
    }),
    ok = emqx_ldap_bind_worker:on_stop(InstId, State),
    emqx_resource_pool:stop(InstId).

on_query(InstId, {query, Data}, State) ->
    on_query(InstId, {query, Data}, [], State);
on_query(InstId, {query, Data, Attrs}, State) ->
    on_query(InstId, {query, Data}, [{attributes, Attrs}], State);
on_query(InstId, {query, Data, Attrs, Timeout}, State) ->
    on_query(InstId, {query, Data}, [{attributes, Attrs}, {timeout, Timeout}], State);
on_query(InstId, {bind, _DN, _Data} = Req, State) ->
    emqx_ldap_bind_worker:on_query(InstId, Req, State).

on_get_status(InstId, #{pool_name := PoolName} = State) ->
    case get_status_with_poolname(PoolName) of
        connected ->
            emqx_ldap_bind_worker:on_get_status(InstId, State);
        disconnected ->
            ?status_disconnected
    end.

get_status_with_poolname(PoolName) ->
    case emqx_resource_pool:health_check_workers(PoolName, fun ?MODULE:do_get_status/1) of
        true ->
            ?status_connected;
        false ->
            %% Note: here can only return `disconnected` not `connecting`
            %% because the LDAP socket/connection can't be reused
            %% searching on a died socket will never return until timeout
            ?status_disconnected
    end.

do_get_status(Conn) ->
    %% search with an invalid base object
    %% if the server is down, the result is {error, ldap_closed}
    %% otherwise is {error, invalidDNSyntax/timeout}
    {error, ldap_closed} =/=
        eldap:search(Conn, [{base, "cn=checkalive"}, {filter, eldap:'approxMatch'("", "")}]).

%% ===================================================================

connect(Options) ->
    #{
        hostname := Host,
        username := Username,
        password := Password,
        request_timeout := RequestTimeout
    } =
        Conf = proplists:get_value(options, Options),
    OpenOpts = maps:to_list(maps:with([port, sslopts], Conf)),
    LogTag = proplists:get_value(log_tag, Options),
    case eldap:open([Host], [{log, mk_log_func(LogTag)}, {timeout, RequestTimeout} | OpenOpts]) of
        {ok, Handle} = Ret ->
            %% TODO: teach `eldap` to accept 0-arity closures as passwords.
            case eldap:simple_bind(Handle, Username, emqx_secret:unwrap(Password)) of
                ok -> Ret;
                Error -> Error
            end;
        Error ->
            Error
    end.

on_query(
    InstId,
    {query, Data},
    SearchOptions,
    #{base_tokens := BaseTks, filter_tokens := FilterTks} = State
) ->
    Base = emqx_placeholder:proc_tmpl(BaseTks, Data),
    FilterBin = emqx_placeholder:proc_tmpl(FilterTks, Data, #{
        return => full_binary, var_trans => fun filter_escape/1
    }),
    case emqx_ldap_filter_parser:scan_and_parse(FilterBin) of
        {ok, Filter} ->
            do_ldap_query(
                InstId,
                [{base, Base}, {filter, Filter} | SearchOptions],
                State
            );
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "filter_parse_failed",
                filter => FilterBin,
                reason => Reason
            }),
            Error
    end.

search(Pid, SearchOptions) ->
    case eldap:search(Pid, SearchOptions) of
        {error, ldap_closed} ->
            %% ldap server closing the socket does not result in
            %% process restart, so we need to kill it to trigger a quick reconnect
            %% instead of waiting for the next health-check
            _ = exit(Pid, kill),
            {error, ldap_closed};
        {error, {gen_tcp_error, _} = Reason} ->
            %% kill the process to trigger reconnect
            _ = exit(Pid, kill),
            {error, Reason};
        Result ->
            Result
    end.

do_ldap_query(
    InstId,
    SearchOptions,
    #{pool_name := PoolName} = State
) ->
    LogMeta = #{connector => InstId, search => SearchOptions, state => emqx_utils:redact(State)},
    ?TRACE("QUERY", "ldap_connector_received_query", LogMeta),
    case
        ecpool:pick_and_do(
            PoolName,
            {?MODULE, search, [SearchOptions]},
            handover
        )
    of
        {ok, Result} ->
            ?tp(
                ldap_connector_query_return,
                #{result => Result}
            ),
            Entries = Result#eldap_search_result.entries,
            Count = length(Entries),
            case Count =< 1 of
                true ->
                    {ok, Entries};
                false ->
                    %% Accept only a single exact match.
                    %% Multiple matches likely indicate:
                    %% 1. A misconfiguration in EMQX, allowing overly broad query conditions.
                    %% 2. Indistinguishable entries in the LDAP database.
                    %% Neither scenario should be allowed to proceed.
                    Msg = "ldap_query_found_more_than_one_match",
                    ?SLOG(
                        error,
                        LogMeta#{
                            msg => "ldap_query_found_more_than_one_match",
                            count => Count
                        }
                    ),
                    {error, {unrecoverable_error, Msg}}
            end;
        {error, 'noSuchObject'} ->
            {ok, []};
        {error, Reason} ->
            ?SLOG(
                error,
                LogMeta#{
                    msg => "ldap_connector_query_failed",
                    reason => emqx_utils:redact(Reason)
                }
            ),
            {error, {unrecoverable_error, Reason}}
    end.

%% Note: the value of the `_Level` here always is 2
mk_log_func(LogTag) ->
    fun(_Level, Format, Args) ->
        ?SLOG(
            debug,
            #{
                msg => LogTag,
                log => io_lib:format(Format, [redact_ldap_log(Arg) || Arg <- Args])
            }
        )
    end.

redact_ldap_log({'BindRequest', Version, Name, {simple, _}}) ->
    {'BindRequest', Version, Name, {simple, ?REDACT_VAL}};
redact_ldap_log(Arg) ->
    Arg.

prepare_template(Config, State) ->
    maps:fold(fun prepare_template/3, State, Config).

prepare_template(base_dn, V, State) ->
    State#{base_tokens => emqx_placeholder:preproc_tmpl(V)};
prepare_template(filter, V, State) ->
    State#{filter_tokens => emqx_placeholder:preproc_tmpl(V)};
prepare_template(_Entry, _, State) ->
    State.

filter_escape(Binary) when is_binary(Binary) ->
    filter_escape(erlang:binary_to_list(Binary));
filter_escape([Char | T]) ->
    case lists:member(Char, filter_special_chars()) of
        true ->
            [$\\, Char | filter_escape(T)];
        _ ->
            [Char | filter_escape(T)]
    end;
filter_escape([]) ->
    [].

filter_special_chars() ->
    [$(, $), $&, $|, $=, $!, $~, $>, $<, $:, $*, $\t, $\n, $\r, $\\].
