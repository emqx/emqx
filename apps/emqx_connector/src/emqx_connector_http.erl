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

-module(emqx_connector_http).

-include("emqx_connector.hrl").

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2,
    reply_delegator/3
]).

-export([
    roots/0,
    fields/1,
    desc/1,
    namespace/0
]).

-export([validate_method/1, join_paths/2]).

-type connect_timeout() :: emqx_schema:duration() | infinity.
-type pool_type() :: random | hash.

-reflect_type([
    connect_timeout/0,
    pool_type/0
]).

-define(DEFAULT_PIPELINE_SIZE, 100).
-define(DEFAULT_REQUEST_TIMEOUT_MS, 30_000).

%%=====================================================================
%% Hocon schema

namespace() -> "connector-http".

roots() ->
    fields(config).

fields(config) ->
    [
        {connect_timeout,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC("connect_timeout")
                }
            )},
        {max_retries,
            sc(
                non_neg_integer(),
                #{deprecated => {since, "5.0.4"}}
            )},
        {retry_interval,
            sc(
                emqx_schema:timeout_duration(),
                #{deprecated => {since, "5.0.4"}}
            )},
        {pool_type,
            sc(
                pool_type(),
                #{
                    default => random,
                    desc => ?DESC("pool_type")
                }
            )},
        {pool_size,
            sc(
                pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC("pool_size")
                }
            )},
        {enable_pipelining,
            sc(
                pos_integer(),
                #{
                    default => ?DEFAULT_PIPELINE_SIZE,
                    desc => ?DESC("enable_pipelining")
                }
            )},
        {request,
            hoconsc:mk(
                ref("request"),
                #{
                    default => undefined,
                    required => false,
                    desc => ?DESC("request")
                }
            )}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields("request") ->
    [
        {method,
            hoconsc:mk(binary(), #{
                required => false,
                desc => ?DESC("method"),
                validator => fun ?MODULE:validate_method/1
            })},
        {path, hoconsc:mk(binary(), #{required => false, desc => ?DESC("path")})},
        {body, hoconsc:mk(binary(), #{required => false, desc => ?DESC("body")})},
        {headers, hoconsc:mk(map(), #{required => false, desc => ?DESC("headers")})},
        {max_retries,
            sc(
                non_neg_integer(),
                #{
                    required => false,
                    desc => ?DESC("max_retries")
                }
            )},
        {request_timeout,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    required => false,
                    desc => ?DESC("request_timeout")
                }
            )}
    ].

desc(config) ->
    "";
desc("request") ->
    "";
desc(_) ->
    undefined.

validate_method(M) when M =:= <<"post">>; M =:= <<"put">>; M =:= <<"get">>; M =:= <<"delete">> ->
    ok;
validate_method(M) ->
    case string:find(M, "${") of
        nomatch ->
            {error,
                <<"Invalid method, should be one of 'post', 'put', 'get', 'delete' or variables in ${field} format.">>};
        _ ->
            ok
    end.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).

%% ===================================================================

callback_mode() -> async_if_possible.

on_start(
    InstId,
    #{
        base_url := #{
            scheme := Scheme,
            host := Host,
            port := Port,
            path := BasePath
        },
        connect_timeout := ConnectTimeout,
        pool_type := PoolType,
        pool_size := PoolSize
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_http_connector",
        connector => InstId,
        config => redact(Config)
    }),
    {Transport, TransportOpts} =
        case Scheme of
            http ->
                {tcp, []};
            https ->
                SSLOpts = emqx_tls_lib:to_client_opts(maps:get(ssl, Config)),
                {tls, SSLOpts}
        end,
    NTransportOpts = emqx_utils:ipv6_probe(TransportOpts),
    PoolOpts = [
        {host, Host},
        {port, Port},
        {connect_timeout, ConnectTimeout},
        {keepalive, 30000},
        {pool_type, PoolType},
        {pool_size, PoolSize},
        {transport, Transport},
        {transport_opts, NTransportOpts},
        {enable_pipelining, maps:get(enable_pipelining, Config, ?DEFAULT_PIPELINE_SIZE)}
    ],
    State = #{
        pool_name => InstId,
        pool_type => PoolType,
        host => Host,
        port => Port,
        connect_timeout => ConnectTimeout,
        base_path => BasePath,
        request => preprocess_request(maps:get(request, Config, undefined))
    },
    case start_pool(InstId, PoolOpts) of
        ok ->
            case do_get_status(InstId, ConnectTimeout) of
                ok ->
                    {ok, State};
                Error ->
                    ok = ehttpc_sup:stop_pool(InstId),
                    Error
            end;
        Error ->
            Error
    end.

start_pool(PoolName, PoolOpts) ->
    case ehttpc_sup:start_pool(PoolName, PoolOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ?SLOG(warning, #{
                msg => "emqx_connector_on_start_already_started",
                pool_name => PoolName
            }),
            ok;
        Error ->
            Error
    end.

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_http_connector",
        connector => InstId
    }),
    ehttpc_sup:stop_pool(InstId).

on_query(InstId, {send_message, Msg}, State) ->
    case maps:get(request, State, undefined) of
        undefined ->
            ?SLOG(error, #{msg => "arg_request_not_found", connector => InstId}),
            {error, arg_request_not_found};
        Request ->
            #{
                method := Method,
                path := Path,
                body := Body,
                headers := Headers,
                request_timeout := Timeout
            } = process_request(Request, Msg),
            %% bridge buffer worker has retry, do not let ehttpc retry
            Retry = 2,
            ClientId = maps:get(clientid, Msg, undefined),
            on_query(
                InstId,
                {ClientId, Method, {Path, Headers, Body}, Timeout, Retry},
                State
            )
    end;
on_query(InstId, {Method, Request}, State) ->
    %% TODO: Get retry from State
    on_query(InstId, {undefined, Method, Request, 5000, _Retry = 2}, State);
on_query(InstId, {Method, Request, Timeout}, State) ->
    %% TODO: Get retry from State
    on_query(InstId, {undefined, Method, Request, Timeout, _Retry = 2}, State);
on_query(
    InstId,
    {KeyOrNum, Method, Request, Timeout, Retry},
    #{base_path := BasePath} = State
) ->
    ?TRACE(
        "QUERY",
        "http_connector_received",
        #{
            request => redact(Request),
            connector => InstId,
            state => redact(State)
        }
    ),
    NRequest = formalize_request(Method, BasePath, Request),
    Worker = resolve_pool_worker(State, KeyOrNum),
    case
        ehttpc:request(
            Worker,
            Method,
            NRequest,
            Timeout,
            Retry
        )
    of
        {error, Reason} when
            Reason =:= econnrefused;
            Reason =:= timeout;
            Reason =:= {shutdown, normal};
            Reason =:= {shutdown, closed}
        ->
            ?SLOG(warning, #{
                msg => "http_connector_do_request_failed",
                reason => Reason,
                connector => InstId
            }),
            {error, {recoverable_error, Reason}};
        {error, {closed, _Message} = Reason} ->
            %% _Message = "The connection was lost."
            ?SLOG(warning, #{
                msg => "http_connector_do_request_failed",
                reason => Reason,
                connector => InstId
            }),
            {error, {recoverable_error, Reason}};
        {error, Reason} = Result ->
            ?SLOG(error, #{
                msg => "http_connector_do_request_failed",
                request => redact(NRequest),
                reason => Reason,
                connector => InstId
            }),
            Result;
        {ok, StatusCode, _} = Result when StatusCode >= 200 andalso StatusCode < 300 ->
            Result;
        {ok, StatusCode, _, _} = Result when StatusCode >= 200 andalso StatusCode < 300 ->
            Result;
        {ok, StatusCode, Headers} ->
            ?SLOG(error, #{
                msg => "http connector do request, received error response",
                note => "the body will be redacted due to security reasons",
                request => redact_request(NRequest),
                connector => InstId,
                status_code => StatusCode
            }),
            {error, #{status_code => StatusCode, headers => Headers}};
        {ok, StatusCode, Headers, Body} ->
            ?SLOG(error, #{
                msg => "http connector do request, received error response.",
                note => "the body will be redacted due to security reasons",
                request => redact_request(NRequest),
                connector => InstId,
                status_code => StatusCode
            }),
            {error, #{status_code => StatusCode, headers => Headers, body => Body}}
    end.

on_query_async(InstId, {send_message, Msg}, ReplyFunAndArgs, State) ->
    case maps:get(request, State, undefined) of
        undefined ->
            ?SLOG(error, #{msg => "arg_request_not_found", connector => InstId}),
            {error, arg_request_not_found};
        Request ->
            #{
                method := Method,
                path := Path,
                body := Body,
                headers := Headers,
                request_timeout := Timeout
            } = process_request(Request, Msg),
            ClientId = maps:get(clientid, Msg, undefined),
            on_query_async(
                InstId,
                {ClientId, Method, {Path, Headers, Body}, Timeout},
                ReplyFunAndArgs,
                State
            )
    end;
on_query_async(
    InstId,
    {KeyOrNum, Method, Request, Timeout},
    ReplyFunAndArgs,
    #{base_path := BasePath} = State
) ->
    Worker = resolve_pool_worker(State, KeyOrNum),
    ?TRACE(
        "QUERY_ASYNC",
        "http_connector_received",
        #{
            request => redact(Request),
            connector => InstId,
            state => redact(State)
        }
    ),
    NRequest = formalize_request(Method, BasePath, Request),
    MaxAttempts = maps:get(max_attempts, State, 3),
    Context = #{
        attempt => 1,
        max_attempts => MaxAttempts,
        state => State,
        key_or_num => KeyOrNum,
        method => Method,
        request => NRequest,
        timeout => Timeout
    },
    ok = ehttpc:request_async(
        Worker,
        Method,
        NRequest,
        Timeout,
        {fun ?MODULE:reply_delegator/3, [Context, ReplyFunAndArgs]}
    ),
    {ok, Worker}.

resolve_pool_worker(State, undefined) ->
    resolve_pool_worker(State, self());
resolve_pool_worker(#{pool_name := PoolName} = State, Key) ->
    case maps:get(pool_type, State, random) of
        random ->
            ehttpc_pool:pick_worker(PoolName);
        hash ->
            ehttpc_pool:pick_worker(PoolName, Key)
    end.

on_get_status(_InstId, #{pool_name := PoolName, connect_timeout := Timeout} = State) ->
    case do_get_status(PoolName, Timeout) of
        ok ->
            connected;
        {error, still_connecting} ->
            connecting;
        {error, Reason} ->
            {disconnected, State, Reason}
    end.

do_get_status(PoolName, Timeout) ->
    Workers = [Worker || {_WorkerName, Worker} <- ehttpc:workers(PoolName)],
    DoPerWorker =
        fun(Worker) ->
            case ehttpc:health_check(Worker, Timeout) of
                ok ->
                    ok;
                {error, Reason} = Error ->
                    ?SLOG(error, #{
                        msg => "http_connector_get_status_failed",
                        reason => redact(Reason),
                        worker => Worker
                    }),
                    Error
            end
        end,
    try emqx_utils:pmap(DoPerWorker, Workers, Timeout) of
        [] ->
            {error, still_connecting};
        [_ | _] = Results ->
            case [E || {error, _} = E <- Results] of
                [] ->
                    ok;
                Errors ->
                    hd(Errors)
            end
    catch
        exit:timeout ->
            ?SLOG(error, #{
                msg => "http_connector_pmap_failed",
                reason => timeout
            }),
            {error, timeout}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
preprocess_request(undefined) ->
    undefined;
preprocess_request(Req) when map_size(Req) == 0 ->
    undefined;
preprocess_request(
    #{
        method := Method,
        path := Path,
        headers := Headers
    } = Req
) ->
    #{
        method => emqx_placeholder:preproc_tmpl(to_bin(Method)),
        path => emqx_placeholder:preproc_tmpl(Path),
        body => maybe_preproc_tmpl(body, Req),
        headers => wrap_auth_header(preproc_headers(Headers)),
        request_timeout => maps:get(request_timeout, Req, ?DEFAULT_REQUEST_TIMEOUT_MS),
        max_retries => maps:get(max_retries, Req, 2)
    }.

preproc_headers(Headers) when is_map(Headers) ->
    maps:fold(
        fun(K, V, Acc) ->
            [
                {
                    emqx_placeholder:preproc_tmpl(to_bin(K)),
                    emqx_placeholder:preproc_tmpl(to_bin(V))
                }
                | Acc
            ]
        end,
        [],
        Headers
    );
preproc_headers(Headers) when is_list(Headers) ->
    lists:map(
        fun({K, V}) ->
            {
                emqx_placeholder:preproc_tmpl(to_bin(K)),
                emqx_placeholder:preproc_tmpl(to_bin(V))
            }
        end,
        Headers
    ).

wrap_auth_header(Headers) ->
    lists:map(fun maybe_wrap_auth_header/1, Headers).

maybe_wrap_auth_header({[{str, Key}] = StrKey, Val}) ->
    {_, MaybeWrapped} = maybe_wrap_auth_header({Key, Val}),
    {StrKey, MaybeWrapped};
maybe_wrap_auth_header({Key, Val} = Header) when
    is_binary(Key), (size(Key) =:= 19 orelse size(Key) =:= 13)
->
    %% We check the size of potential keys in the guard above and consider only
    %% those that match the number of characters of either "Authorization" or
    %% "Proxy-Authorization".
    case try_bin_to_lower(Key) of
        <<"authorization">> ->
            {Key, emqx_secret:wrap(Val)};
        <<"proxy-authorization">> ->
            {Key, emqx_secret:wrap(Val)};
        _Other ->
            Header
    end;
maybe_wrap_auth_header(Header) ->
    Header.

try_bin_to_lower(Bin) ->
    try iolist_to_binary(string:lowercase(Bin)) of
        LowercaseBin -> LowercaseBin
    catch
        _:_ -> Bin
    end.

maybe_preproc_tmpl(Key, Conf) ->
    case maps:get(Key, Conf, undefined) of
        undefined -> undefined;
        Val -> emqx_placeholder:preproc_tmpl(Val)
    end.

process_request(
    #{
        method := MethodTks,
        path := PathTks,
        body := BodyTks,
        headers := HeadersTks,
        request_timeout := ReqTimeout
    } = Conf,
    Msg
) ->
    Conf#{
        method => make_method(emqx_placeholder:proc_tmpl(MethodTks, Msg)),
        path => emqx_placeholder:proc_tmpl(PathTks, Msg),
        body => process_request_body(BodyTks, Msg),
        headers => proc_headers(HeadersTks, Msg),
        request_timeout => ReqTimeout
    }.

process_request_body(undefined, Msg) ->
    emqx_utils_json:encode(Msg);
process_request_body(BodyTks, Msg) ->
    emqx_placeholder:proc_tmpl(BodyTks, Msg).

proc_headers(HeaderTks, Msg) ->
    lists:map(
        fun({K, V}) ->
            {
                emqx_placeholder:proc_tmpl(K, Msg),
                emqx_placeholder:proc_tmpl(emqx_secret:unwrap(V), Msg)
            }
        end,
        HeaderTks
    ).

make_method(M) when M == <<"POST">>; M == <<"post">> -> post;
make_method(M) when M == <<"PUT">>; M == <<"put">> -> put;
make_method(M) when M == <<"GET">>; M == <<"get">> -> get;
make_method(M) when M == <<"DELETE">>; M == <<"delete">> -> delete.

formalize_request(Method, BasePath, {Path, Headers, _Body}) when
    Method =:= get; Method =:= delete
->
    formalize_request(Method, BasePath, {Path, Headers});
formalize_request(_Method, BasePath, {Path, Headers, Body}) ->
    {join_paths(BasePath, Path), Headers, Body};
formalize_request(_Method, BasePath, {Path, Headers}) ->
    {join_paths(BasePath, Path), Headers}.

%% By default, we cannot treat HTTP paths as "file" or "resource" paths,
%% because an HTTP server may handle paths like
%% "/a/b/c/", "/a/b/c" and "/a//b/c" differently.
%%
%% So we try to avoid unneccessary path normalization.
%%
%% See also: `join_paths_test_/0`
join_paths(Path1, Path2) ->
    do_join_paths(lists:reverse(to_list(Path1)), to_list(Path2)).

%% "abc/" + "/cde"
do_join_paths([$/ | Path1], [$/ | Path2]) ->
    lists:reverse(Path1) ++ [$/ | Path2];
%% "abc/" + "cde"
do_join_paths([$/ | Path1], Path2) ->
    lists:reverse(Path1) ++ [$/ | Path2];
%% "abc" + "/cde"
do_join_paths(Path1, [$/ | Path2]) ->
    lists:reverse(Path1) ++ [$/ | Path2];
%% "abc" + "cde"
do_join_paths(Path1, Path2) ->
    lists:reverse(Path1) ++ [$/ | Path2].

to_list(List) when is_list(List) -> List;
to_list(Bin) when is_binary(Bin) -> binary_to_list(Bin).

to_bin(Bin) when is_binary(Bin) ->
    Bin;
to_bin(Str) when is_list(Str) ->
    list_to_binary(Str);
to_bin(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8).

reply_delegator(Context, ReplyFunAndArgs, Result) ->
    spawn(fun() -> maybe_retry(Result, Context, ReplyFunAndArgs) end).

transform_result(Result) ->
    case Result of
        %% The normal reason happens when the HTTP connection times out before
        %% the request has been fully processed
        {error, Reason} when
            Reason =:= econnrefused;
            Reason =:= timeout;
            Reason =:= normal;
            Reason =:= {shutdown, normal};
            Reason =:= {shutdown, closed}
        ->
            {error, {recoverable_error, Reason}};
        {error, {closed, _Message} = Reason} ->
            %% _Message = "The connection was lost."
            {error, {recoverable_error, Reason}};
        _ ->
            Result
    end.

maybe_retry(Result0, _Context = #{attempt := N, max_attempts := Max}, ReplyFunAndArgs) when
    N >= Max
->
    Result = transform_result(Result0),
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
maybe_retry({error, Reason}, Context, ReplyFunAndArgs) ->
    #{
        state := State,
        attempt := Attempt,
        key_or_num := KeyOrNum,
        method := Method,
        request := Request,
        timeout := Timeout
    } = Context,
    %% TODO: reset the expiration time for free retries?
    IsFreeRetry = Reason =:= normal orelse Reason =:= {shutdown, normal},
    NContext =
        case IsFreeRetry of
            true -> Context;
            false -> Context#{attempt := Attempt + 1}
        end,
    Worker = resolve_pool_worker(State, KeyOrNum),
    ok = ehttpc:request_async(
        Worker,
        Method,
        Request,
        Timeout,
        {fun ?MODULE:reply_delegator/3, [NContext, ReplyFunAndArgs]}
    ),
    ok;
maybe_retry(Result, _Context, ReplyFunAndArgs) ->
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result).

%% The HOCON schema system may generate sensitive keys with this format
is_sensitive_key([{str, StringKey}]) ->
    is_sensitive_key(StringKey);
is_sensitive_key(Atom) when is_atom(Atom) ->
    is_sensitive_key(erlang:atom_to_binary(Atom));
is_sensitive_key(Bin) when is_binary(Bin), (size(Bin) =:= 19 orelse size(Bin) =:= 13) ->
    %% We want to convert this to lowercase since the http header fields
    %% are case insensitive, which means that a user of the Webhook bridge
    %% can write this field name in many different ways.
    case try_bin_to_lower(Bin) of
        <<"authorization">> -> true;
        <<"proxy-authorization">> -> true;
        _ -> false
    end;
is_sensitive_key(_) ->
    false.

%% Function that will do a deep traversal of Data and remove sensitive
%% information (i.e., passwords)
redact(Data) ->
    emqx_utils:redact(Data, fun is_sensitive_key/1).

%% because the body may contain some sensitive data
%% and at the same time the redact function will not scan the binary data
%% and we also can't know the body format and where the sensitive data will be
%% so the easy way to keep data security is redacted the whole body
redact_request({Path, Headers}) ->
    {Path, redact(Headers)};
redact_request({Path, Headers, _Body}) ->
    {Path, redact(Headers), <<"******">>}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

redact_test_() ->
    TestData1 = [
        {<<"content-type">>, <<"application/json">>},
        {<<"Authorization">>, <<"Basic YWxhZGRpbjpvcGVuc2VzYW1l">>}
    ],

    TestData2 = #{
        headers =>
            [
                {[{str, <<"content-type">>}], [{str, <<"application/json">>}]},
                {[{str, <<"Authorization">>}], [{str, <<"Basic YWxhZGRpbjpvcGVuc2VzYW1l">>}]}
            ]
    },
    [
        ?_assert(is_sensitive_key(<<"Authorization">>)),
        ?_assert(is_sensitive_key(<<"AuthoriZation">>)),
        ?_assert(is_sensitive_key('AuthoriZation')),
        ?_assert(is_sensitive_key(<<"PrOxy-authoRizaTion">>)),
        ?_assert(is_sensitive_key('PrOxy-authoRizaTion')),
        ?_assertNot(is_sensitive_key(<<"Something">>)),
        ?_assertNot(is_sensitive_key(89)),
        ?_assertNotEqual(TestData1, redact(TestData1)),
        ?_assertNotEqual(TestData2, redact(TestData2))
    ].

join_paths_test_() ->
    [
        ?_assertEqual("abc/cde", join_paths("abc", "cde")),
        ?_assertEqual("abc/cde", join_paths("abc", "/cde")),
        ?_assertEqual("abc/cde", join_paths("abc/", "cde")),
        ?_assertEqual("abc/cde", join_paths("abc/", "/cde")),

        ?_assertEqual("/", join_paths("", "")),
        ?_assertEqual("/cde", join_paths("", "cde")),
        ?_assertEqual("/cde", join_paths("", "/cde")),
        ?_assertEqual("/cde", join_paths("/", "cde")),
        ?_assertEqual("/cde", join_paths("/", "/cde")),

        ?_assertEqual("//cde/", join_paths("/", "//cde/")),
        ?_assertEqual("abc///cde/", join_paths("abc//", "//cde/"))
    ].

-endif.
