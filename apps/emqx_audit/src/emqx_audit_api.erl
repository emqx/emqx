%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_audit_api).

-behaviour(minirest_api).

%% API
-export([api_spec/0, paths/0, schema/1, namespace/0, fields/1]).
-export([audit/2]).
-export([qs2ms/2, format/1]).

-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include("emqx_audit.hrl").

-import(hoconsc, [mk/2, ref/2, array/1]).

-define(TAGS, ["Audit"]).

-define(AUDIT_QS_SCHEMA, [
    {<<"node">>, atom},
    {<<"from">>, atom},
    {<<"source">>, binary},
    {<<"source_ip">>, binary},
    {<<"operation_id">>, binary},
    {<<"operation_type">>, binary},
    {<<"operation_result">>, atom},
    {<<"http_status_code">>, integer},
    {<<"http_method">>, atom},
    {<<"gte_created_at">>, timestamp},
    {<<"lte_created_at">>, timestamp},
    {<<"gte_duration_ms">>, integer},
    {<<"lte_duration_ms">>, integer}
]).
-define(DISABLE_MSG, <<"Audit is disabled">>).

namespace() -> "audit".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/audit"].

schema("/audit") ->
    #{
        'operationId' => audit,
        get => #{
            tags => ?TAGS,
            description => ?DESC(audit_get),
            parameters => [
                {node,
                    ?HOCON(binary(), #{
                        in => query,
                        required => false,
                        example => <<"emqx@127.0.0.1">>,
                        desc => ?DESC(filter_node)
                    })},
                {from,
                    ?HOCON(?ENUM([dashboard, rest_api, cli, erlang_console]), #{
                        in => query,
                        required => false,
                        example => <<"dashboard">>,
                        desc => ?DESC(filter_from)
                    })},
                {source,
                    ?HOCON(binary(), #{
                        in => query,
                        required => false,
                        example => <<"admin">>,
                        desc => ?DESC(filter_source)
                    })},
                {source_ip,
                    ?HOCON(binary(), #{
                        in => query,
                        required => false,
                        example => <<"127.0.0.1">>,
                        desc => ?DESC(filter_source_ip)
                    })},
                {operation_id,
                    ?HOCON(binary(), #{
                        in => query,
                        required => false,
                        example => <<"/rules/{id}">>,
                        desc => ?DESC(filter_operation_id)
                    })},
                {operation_type,
                    ?HOCON(binary(), #{
                        in => query,
                        example => <<"rules">>,
                        required => false,
                        desc => ?DESC(filter_operation_type)
                    })},
                {operation_result,
                    ?HOCON(?ENUM([success, failure]), #{
                        in => query,
                        example => failure,
                        required => false,
                        desc => ?DESC(filter_operation_result)
                    })},
                {http_status_code,
                    ?HOCON(integer(), #{
                        in => query,
                        example => 200,
                        required => false,
                        desc => ?DESC(filter_http_status_code)
                    })},
                {http_method,
                    ?HOCON(?ENUM([post, put, delete]), #{
                        in => query,
                        example => post,
                        required => false,
                        desc => ?DESC(filter_http_method)
                    })},
                {gte_duration_ms,
                    ?HOCON(integer(), #{
                        in => query,
                        example => 0,
                        required => false,
                        desc => ?DESC(filter_gte_duration_ms)
                    })},
                {lte_duration_ms,
                    ?HOCON(integer(), #{
                        in => query,
                        example => 1000,
                        required => false,
                        desc => ?DESC(filter_lte_duration_ms)
                    })},
                {gte_created_at,
                    ?HOCON(emqx_utils_calendar:epoch_microsecond(), #{
                        in => query,
                        required => false,
                        example => <<"2023-10-15T00:00:00.820384+08:00">>,
                        desc => ?DESC(filter_gte_created_at)
                    })},
                {lte_created_at,
                    ?HOCON(emqx_utils_calendar:epoch_microsecond(), #{
                        in => query,
                        example => <<"2023-10-16T00:00:00.820384+08:00">>,
                        required => false,
                        desc => ?DESC(filter_lte_created_at)
                    })},
                ref(emqx_dashboard_swagger, page),
                ref(emqx_dashboard_swagger, limit)
            ],
            summary => <<"List audit logs">>,
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(
                        array(?REF(audit_list)),
                        audit_log_list_example()
                    ),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'],
                    ?DISABLE_MSG
                )
            }
        }
    }.

fields(audit_list) ->
    [
        {data, mk(array(?REF(audit)), #{desc => ?DESC("audit_resp")})},
        {meta, mk(ref(emqx_dashboard_swagger, meta), #{})}
    ];
fields(audit) ->
    [
        {created_at,
            ?HOCON(
                emqx_utils_calendar:epoch_microsecond(),
                #{
                    desc => "The time when the log is created"
                }
            )},
        {node,
            ?HOCON(binary(), #{
                desc => "The node name to which the log is created"
            })},
        {from,
            ?HOCON(?ENUM([dashboard, rest_api, cli, erlang_console]), #{
                desc => "The source type of the log"
            })},
        {source,
            ?HOCON(binary(), #{
                desc => "The source of the log"
            })},
        {source_ip,
            ?HOCON(binary(), #{
                desc => "The source ip of the log"
            })},
        {operation_id,
            ?HOCON(binary(), #{
                desc => "The operation id of the log"
            })},
        {operation_type,
            ?HOCON(binary(), #{
                desc => "The operation type of the log"
            })},
        {operation_result,
            ?HOCON(?ENUM([success, failure]), #{
                desc => "The operation result of the log"
            })},
        {http_status_code,
            ?HOCON(integer(), #{
                desc => "The http status code of the log"
            })},
        {http_method,
            ?HOCON(?ENUM([post, put, delete]), #{
                desc => "The http method of the log"
            })},
        {duration_ms,
            ?HOCON(integer(), #{
                desc => "The duration of the log"
            })},
        {args,
            ?HOCON(?ARRAY(binary()), #{
                desc => "The args of the log"
            })},
        {failure,
            ?HOCON(?ARRAY(binary()), #{
                desc => "The failure of the log"
            })},
        {http_request,
            ?HOCON(?REF(http_request), #{
                desc => "The http request of the log"
            })}
    ];
fields(http_request) ->
    [
        {bindings, ?HOCON(map(), #{})},
        {body, ?HOCON(map(), #{})},
        {headers, ?HOCON(map(), #{})},
        {method, ?HOCON(?ENUM([post, put, delete]), #{})}
    ].

audit(get, #{query_string := QueryString}) ->
    case emqx_config:get([log, audit, enable], false) of
        false ->
            {400, #{code => 'BAD_REQUEST', message => ?DISABLE_MSG}};
        true ->
            case
                emqx_mgmt_api:node_query(
                    node(),
                    ?AUDIT,
                    QueryString,
                    ?AUDIT_QS_SCHEMA,
                    fun ?MODULE:qs2ms/2,
                    fun ?MODULE:format/1
                )
            of
                {error, page_limit_invalid} ->
                    {400, #{code => 'BAD_REQUEST', message => <<"page_limit_invalid">>}};
                {error, Node, Error} ->
                    Message = list_to_binary(
                        io_lib:format("bad rpc call ~p, Reason ~p", [Node, Error])
                    ),
                    {500, #{code => <<"NODE_DOWN">>, message => Message}};
                Result ->
                    {200, Result}
            end
    end.

qs2ms(_Tab, {Qs, _}) ->
    #{
        match_spec => gen_match_spec(Qs, #?AUDIT{_ = '_'}, []),
        fuzzy_fun => undefined
    }.

gen_match_spec([], Audit, Conn) ->
    [{Audit, Conn, ['$_']}];
gen_match_spec([{node, '=:=', T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{node = T}, Conn);
gen_match_spec([{from, '=:=', T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{from = T}, Conn);
gen_match_spec([{source, '=:=', T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{source = T}, Conn);
gen_match_spec([{source_ip, '=:=', T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{source_ip = T}, Conn);
gen_match_spec([{operation_id, '=:=', T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{operation_id = T}, Conn);
gen_match_spec([{operation_type, '=:=', T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{operation_type = T}, Conn);
gen_match_spec([{operation_result, '=:=', T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{operation_result = T}, Conn);
gen_match_spec([{http_status_code, '=:=', T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{http_status_code = T}, Conn);
gen_match_spec([{http_method, '=:=', T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{http_method = T}, Conn);
gen_match_spec([{created_at, Hold, T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{created_at = '$1'}, [{Hold, '$1', T} | Conn]);
gen_match_spec([{created_at, Hold1, T1, Hold2, T2} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{created_at = '$1'}, [
        {Hold1, '$1', T1}, {Hold2, '$1', T2} | Conn
    ]);
gen_match_spec([{duration_ms, Hold, T} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{duration_ms = '$2'}, [{Hold, '$2', T} | Conn]);
gen_match_spec([{duration_ms, Hold1, T1, Hold2, T2} | Qs], Audit, Conn) ->
    gen_match_spec(Qs, Audit#?AUDIT{duration_ms = '$2'}, [
        {Hold1, '$2', T1}, {Hold2, '$2', T2} | Conn
    ]).

format(Audit) ->
    #?AUDIT{
        created_at = CreatedAt,
        node = Node,
        from = From,
        source = Source,
        source_ip = SourceIp,
        operation_id = OperationId,
        operation_type = OperationType,
        operation_result = OperationResult,
        http_status_code = HttpStatusCode,
        http_method = HttpMethod,
        duration_ms = DurationMs,
        args = Args,
        failure = Failure,
        http_request = HttpRequest
    } = Audit,
    #{
        created_at => emqx_utils_calendar:epoch_to_rfc3339(CreatedAt, microsecond),
        node => Node,
        from => From,
        source => Source,
        source_ip => SourceIp,
        operation_id => OperationId,
        operation_type => OperationType,
        operation_result => OperationResult,
        http_status_code => HttpStatusCode,
        http_method => HttpMethod,
        duration_ms => DurationMs,
        args => Args,
        failure => Failure,
        http_request => HttpRequest
    }.

audit_log_list_example() ->
    #{
        data => [api_example(), cli_example()],
        meta => #{
            <<"count">> => 2,
            <<"hasnext">> => false,
            <<"limit">> => 50,
            <<"page">> => 1
        }
    }.

api_example() ->
    #{
        <<"args">> => "",
        <<"created_at">> => "2023-10-17T10:41:20.383993+08:00",
        <<"duration_ms">> => 0,
        <<"failure">> => "",
        <<"from">> => "dashboard",
        <<"http_method">> => "post",
        <<"http_request">> => #{
            <<"bindings">> => #{},
            <<"body">> => #{
                <<"password">> => "******",
                <<"username">> => "admin"
            },
            <<"headers">> => #{
                <<"accept">> => "*/*",
                <<"authorization">> => "******",
                <<"connection">> => "keep-alive",
                <<"content-length">> => "45",
                <<"content-type">> => "application/json"
            },
            <<"method">> => "post"
        },
        <<"http_status_code">> => 200,
        <<"node">> => "emqx@127.0.0.1",
        <<"operation_id">> => "/login",
        <<"operation_result">> => "success",
        <<"operation_type">> => "login",
        <<"source">> => "admin",
        <<"source_ip">> => "127.0.0.1"
    }.

cli_example() ->
    #{
        <<"args">> => [<<"show">>, <<"log">>],
        <<"created_at">> => "2023-10-17T10:45:13.100426+08:00",
        <<"duration_ms">> => 7,
        <<"failure">> => "",
        <<"from">> => "cli",
        <<"http_method">> => "",
        <<"http_request">> => "",
        <<"http_status_code">> => "",
        <<"node">> => "emqx@127.0.0.1",
        <<"operation_id">> => "",
        <<"operation_result">> => "",
        <<"operation_type">> => "conf",
        <<"source">> => "",
        <<"source_ip">> => ""
    }.
