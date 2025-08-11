%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_trace).

-behaviour(minirest_api).

-include_lib("kernel/include/file.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").

-export([
    api_spec/0,
    fields/1,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    trace/2,
    delete_trace/2,
    update_trace/2,
    download_trace_log/2,
    stream_log_file/2,
    log_file_detail/2
]).

-export([validate_name/1]).

%% RPC Targets:
-export([
    get_trace_size/0,
    get_trace_details/1,
    read_trace_file/3
]).

-define(MAX_READ_TRACE_BYTES, 64 * 1024 * 1024).
-define(STREAM_TRACE_RETRY_TIMEOUT, 20).

-define(TO_BIN(_B_), iolist_to_binary(_B_)).
-define(NOT_FOUND_WITH_MSG(N), ?NOT_FOUND(?TO_BIN([N, " NOT FOUND"]))).
-define(TAGS, [<<"Trace">>]).

namespace() -> "trace".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/trace",
        "/trace/:name/stop",
        "/trace/:name/download",
        "/trace/:name/log",
        "/trace/:name/log_detail",
        "/trace/:name"
    ].

schema("/trace") ->
    #{
        'operationId' => trace,
        get => #{
            description => ?DESC(list_all),
            tags => ?TAGS,
            responses => #{
                200 => hoconsc:array(hoconsc:ref(trace))
            }
        },
        post => #{
            description => ?DESC(create_new),
            tags => ?TAGS,
            'requestBody' => delete([status, log_size], fields(trace)),
            responses => #{
                200 => hoconsc:ref(trace),
                400 => emqx_dashboard_swagger:error_codes(
                    [
                        'INVALID_PARAMS',
                        ?EXCEED_LIMIT
                    ],
                    <<"invalid trace params">>
                ),
                409 => emqx_dashboard_swagger:error_codes(
                    [
                        'ALREADY_EXISTS',
                        'DUPLICATE_CONDITION',
                        'BAD_TYPE'
                    ],
                    <<"trace already exists">>
                )
            }
        },
        delete => #{
            description => ?DESC(clear_all),
            tags => ?TAGS,
            responses => #{
                204 => <<"No Content">>
            }
        }
    };
schema("/trace/:name") ->
    #{
        'operationId' => delete_trace,
        delete => #{
            description => ?DESC(delete_trace),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                204 => <<"Delete successfully">>,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Trace Name Not Found">>)
            }
        }
    };
schema("/trace/:name/stop") ->
    #{
        'operationId' => update_trace,
        put => #{
            description => ?DESC(stop_trace),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                200 => hoconsc:ref(trace),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Trace Name Not Found">>)
            }
        }
    };
schema("/trace/:name/download") ->
    #{
        'operationId' => download_trace_log,
        get => #{
            description => ?DESC(download_log_by_name),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name), hoconsc:ref(node)],
            responses => #{
                200 =>
                    #{
                        description => "A trace zip file",
                        content => #{
                            'application/octet-stream' =>
                                #{schema => #{type => "string", format => "binary"}}
                        }
                    },
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND', 'NODE_ERROR'], <<"Trace Name or Node Not Found">>
                )
            }
        }
    };
schema("/trace/:name/log_detail") ->
    #{
        'operationId' => log_file_detail,
        get => #{
            description => ?DESC(get_trace_file_metadata),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                200 => hoconsc:array(hoconsc:ref(log_file_detail)),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"Trace Name Not Found">>)
            }
        }
    };
schema("/trace/:name/log") ->
    #{
        'operationId' => stream_log_file,
        get => #{
            description => ?DESC(view_trace_log),
            tags => ?TAGS,
            parameters => [
                hoconsc:ref(name),
                hoconsc:ref(bytes),
                hoconsc:ref(position),
                hoconsc:ref(node)
            ],
            responses => #{
                200 =>
                    [
                        {items, hoconsc:mk(binary(), #{example => "TEXT-LOG-ITEMS"})},
                        {meta, fields(bytes) ++ fields(position) ++ fields(stream_hint)}
                    ],
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST', 'INVALID_PARAMETER', 'STALE_CURSOR'], <<"Bad input parameter">>
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND', 'NODE_ERROR'], <<"Trace Name or Node Not Found">>
                ),
                503 => emqx_dashboard_swagger:error_codes(
                    ['SERVICE_UNAVAILABLE'], <<"Requested chunk size too big">>
                )
            }
        }
    }.

fields(log_file_detail) ->
    fields(node) ++
        [
            {size, hoconsc:mk(integer(), #{description => ?DESC(file_size)})},
            {mtime, hoconsc:mk(integer(), #{description => ?DESC(file_mtime)})}
        ];
fields(trace) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    description => ?DESC(trace_name),
                    validator => fun ?MODULE:validate_name/1,
                    required => true,
                    example => <<"EMQX-TRACE-1">>
                }
            )},
        {type,
            hoconsc:mk(
                hoconsc:enum([clientid, topic, ip_address, ruleid]),
                #{
                    description => ?DESC(filter_type),
                    required => true,
                    example => <<"clientid">>
                }
            )},
        {topic,
            hoconsc:mk(
                binary(),
                #{
                    description => ?DESC(support_wildcard),
                    required => false,
                    example => <<"/dev/#">>
                }
            )},
        {clientid,
            hoconsc:mk(
                binary(),
                #{
                    description => ?DESC(mqtt_clientid),
                    required => false,
                    example => <<"dev-001">>
                }
            )},
        %% TODO add ip_address type in emqx_schema.erl
        {ip_address,
            hoconsc:mk(
                binary(),
                #{
                    description => ?DESC(client_ip_addess),
                    required => false,
                    example => <<"127.0.0.1">>
                }
            )},
        {ruleid,
            hoconsc:mk(
                binary(),
                #{
                    description => ?DESC(ruleid_field),
                    required => false,
                    example => <<"my_rule">>
                }
            )},
        {status,
            hoconsc:mk(
                hoconsc:enum([running, stopped, waiting]),
                #{
                    description => ?DESC(trace_status),
                    required => false,
                    example => running
                }
            )},
        {payload_encode,
            hoconsc:mk(hoconsc:enum([hex, text, hidden]), #{
                desc =>
                    ""
                    "Determine the format of the payload format in the trace file.<br/>\n"
                    "`text`: Text-based protocol or plain text protocol.\n"
                    " It is recommended when payload is JSON encoded.<br/>\n"
                    "`hex`: Binary hexadecimal encode."
                    "It is recommended when payload is a custom binary protocol.<br/>\n"
                    "`hidden`: payload is obfuscated as `******`"
                    "",
                default => text
            })},
        {payload_limit,
            hoconsc:mk(
                integer(),
                #{
                    desc =>
                        ""
                        "Determine the maximum bytes of the payload will be printed in the trace file.<br/>\n"
                        "The truncated part will be displayed as '...' in the trace file.<br/>\n"
                        "It's only effective when `payload_encode` is `hex` or `text`<br/>\n"
                        "`0`: No limit<br/>\n"
                        "Default is 1024 bytes<br/>\n"
                        "",
                    default => 1024
                }
            )},
        {start_at,
            hoconsc:mk(
                emqx_utils_calendar:epoch_second(),
                #{
                    description => ?DESC(time_format),
                    required => false,
                    example => <<"2021-11-04T18:17:38+08:00">>
                }
            )},
        {end_at,
            hoconsc:mk(
                emqx_utils_calendar:epoch_second(),
                #{
                    description => ?DESC(time_format),
                    required => false,
                    example => <<"2021-11-05T18:17:38+08:00">>
                }
            )},
        {log_size,
            hoconsc:mk(
                hoconsc:array(map()),
                #{
                    description => ?DESC(trace_log_size),
                    example => [#{<<"node">> => <<"emqx@127.0.0.1">>, <<"size">> => 1024}],
                    required => false
                }
            )},
        {formatter,
            hoconsc:mk(
                hoconsc:union([text, json]),
                #{
                    description => ?DESC(trace_log_formatter),
                    example => text,
                    required => false
                }
            )}
    ];
fields(name) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    desc => <<"[a-zA-Z0-9-_]">>,
                    example => <<"EMQX-TRACE-1">>,
                    in => path,
                    validator => fun ?MODULE:validate_name/1
                }
            )}
    ];
fields(node) ->
    [
        {node,
            hoconsc:mk(
                binary(),
                #{
                    description => ?DESC(node_name),
                    in => query,
                    required => false,
                    example => "emqx@127.0.0.1"
                }
            )}
    ];
fields(bytes) ->
    [
        {bytes,
            hoconsc:mk(
                range(0, ?MAX_READ_TRACE_BYTES),
                #{
                    description => ?DESC(max_response_bytes),
                    in => query,
                    required => false,
                    default => 1000
                }
            )}
    ];
fields(position) ->
    [
        {position,
            hoconsc:mk(
                hoconsc:union([integer(), binary()]),
                #{
                    description => ?DESC(current_trace_cursor),
                    in => query,
                    required => false,
                    default => 0
                }
            )}
    ];
fields(stream_hint) ->
    [
        {hint,
            hoconsc:mk(
                hoconsc:enum([eof, retry]),
                #{
                    description => ?DESC(current_trace_stream_hint),
                    in => query,
                    required => false
                }
            )}
    ].

-define(NAME_RE, "^[A-Za-z]+[A-Za-z0-9-_]*$").

validate_name(Name) ->
    NameLen = byte_size(Name),
    case NameLen > 0 andalso NameLen =< 256 of
        true ->
            case re:run(Name, ?NAME_RE) of
                nomatch -> {error, "Name should be " ?NAME_RE};
                _ -> ok
            end;
        false ->
            {error, "Name Length must =< 256"}
    end.

delete(Keys, Fields) ->
    lists:foldl(fun(Key, Acc) -> lists:keydelete(Key, 1, Acc) end, Fields, Keys).

trace(get, _Params) ->
    case emqx_trace:list() of
        [] ->
            {200, []};
        List0 ->
            List = lists:sort(
                fun(#{start_at := A}, #{start_at := B}) -> A > B end,
                List0
            ),
            Now = emqx_trace:now_second(),
            Nodes = emqx:running_nodes(),
            AllTraceSizes = cluster_get_trace_size(),
            Traces =
                lists:map(
                    fun(TraceIn = #{name := Name}) ->
                        Trace = format_trace(TraceIn, Now, Nodes),
                        LogSize = collect_file_size(Nodes, Name, AllTraceSizes),
                        Trace#{log_size => LogSize}
                    end,
                    List
                ),
            {200, Traces}
    end;
trace(post, #{body := Params} = Req) ->
    Namespace = emqx_dashboard:get_namespace(Req),
    case emqx_trace:create(mk_trace(Params, Namespace)) of
        {ok, Created} ->
            {200, format_trace(Created)};
        {error, {already_existed, Name}} ->
            {409, #{
                code => 'ALREADY_EXISTS',
                message => ?TO_BIN([Name, " Already Exists"])
            }};
        {error, {duplicate_condition, Name}} ->
            {409, #{
                code => 'DUPLICATE_CONDITION',
                message => ?TO_BIN([Name, " Duplication Condition"])
            }};
        {error, {bad_type, _}} ->
            {409, #{
                code => 'BAD_TYPE',
                message => <<"Rolling upgrade in progress, create failed">>
            }};
        {error, {max_limit_reached, Limit}} ->
            {400, #{
                code => ?EXCEED_LIMIT,
                message =>
                    case Limit of
                        0 ->
                            <<"Creating traces is disallowed">>;
                        _ ->
                            <<
                                "Number of existing traces has reached the allowed "
                                "maximum, please delete outdated traces first"
                            >>
                    end
            }};
        {error, Reason} ->
            {400, #{
                code => 'INVALID_PARAMS',
                message => ?TO_BIN(Reason)
            }}
    end;
trace(delete, _Param) ->
    ok = emqx_trace:clear(),
    {204}.

mk_trace(Params, Namespace) ->
    Trace0 = #{type := Type} = emqx_utils_maps:safe_atom_key_map(Params),
    Trace1 = maps:without([type, Type], Trace0),
    Trace1#{
        filter => {Type, maps:get(Type, Trace0)},
        namespace => Namespace
    }.

format_trace(Trace) ->
    format_trace(Trace, emqx_trace:now_second(), emqx:running_nodes()).

format_trace(
    Trace = #{
        name := Name,
        start_at := Start,
        end_at := End,
        filter := {Type, Filter}
    },
    Now,
    Nodes
) ->
    LogSize = lists:foldl(
        fun(Node, Acc) -> Acc#{Node => 0} end,
        #{},
        Nodes
    ),
    TraceOut = maps:with([name, payload_encode, payload_limit, formatter], Trace),
    TraceOut#{
        name => Name,
        type => Type,
        Type => iolist_to_binary(Filter),
        start_at => emqx_utils_calendar:epoch_to_rfc3339(Start, second),
        end_at => emqx_utils_calendar:epoch_to_rfc3339(End, second),
        status => emqx_trace:status(Trace, Now),
        log_size => LogSize
    }.

delete_trace(delete, #{bindings := #{name := Name}}) ->
    case emqx_trace:delete(Name) of
        ok -> {204};
        {error, not_found} -> ?NOT_FOUND_WITH_MSG(Name)
    end.

update_trace(put, #{bindings := #{name := Name}}) ->
    case emqx_trace:update(Name, false) of
        ok -> {200, #{enable => false, name => Name}};
        {error, not_found} -> ?NOT_FOUND_WITH_MSG(Name)
    end.

%% if HTTP request headers include accept-encoding: gzip and file size > 300 bytes.
%% cowboy_compress_h will auto encode gzip format.
download_trace_log(get, #{bindings := #{name := Name}, query_string := Query}) ->
    case emqx_trace:get(Name) of
        {ok, Trace} ->
            case parse_node(Query, undefined) of
                {ok, undefined} ->
                    gather_trace_logs(Trace, supported_running_nodes());
                {ok, Node} ->
                    gather_trace_logs(Trace, [Node]);
                {error, not_found} ->
                    ?NOT_FOUND_WITH_MSG(<<"Node">>)
            end;
        {error, not_found} ->
            ?NOT_FOUND_WITH_MSG(Name)
    end.

gather_trace_logs(Trace = #{name := Name}, Nodes) ->
    %% We generate a session ID so that we name files
    %% with unique names. Then we won't cause
    %% overwrites for concurrent requests.
    SessionId = emqx_utils:gen_id(),
    ZipDir = filename:join([emqx_trace:zip_dir(), SessionId]),
    NodeResults = [{N, stream_trace_log_into(N, Trace, ZipDir)} || N <- Nodes],
    Result = maps:groups_from_list(
        fun
            ({_Node, {ok, 0, _}}) ->
                empty;
            ({_Node, {ok, _, _}}) ->
                nonempty;
            ({_Node, {error, {file_error, enoent}}}) ->
                empty;
            ({_Node, {error, not_found}}) ->
                empty;
            ({Node, {error, Reason}}) ->
                ?SLOG(error, #{
                    msg => "stream_trace_log_error",
                    node => Node,
                    trace => Name,
                    reason => Reason
                }),
                error
        end,
        NodeResults
    ),
    case Result of
        #{nonempty := NonEmpty = [_ | _]} ->
            TraceFiles = [Filename || {_Node, {ok, _, Filename}} <- NonEmpty],
            serve_trace_log_archive(Trace, ZipDir, TraceFiles);
        #{error := Reasons} ->
            ok = file:del_dir_r(ZipDir),
            ?INTERNAL_ERROR(Reasons);
        #{empty := _} ->
            ?NOT_FOUND(<<"Trace is empty">>)
    end.

stream_trace_log_into(Node, Trace = #{name := Name}, ZipDir) ->
    Filename = atom_to_list(Node) ++ "-" ++ emqx_trace:log_filename(Trace),
    Filepath = filename:join(ZipDir, Filename),
    case stream_trace_log(Node, Name, Filepath) of
        NWritten when is_integer(NWritten) ->
            {ok, NWritten, Filename};
        Error ->
            Error
    end.

stream_trace_log(Node, Name, Filepath) ->
    case node_stream_trace_log(Node, Name, start, undefined) of
        {ok, <<>>, {eof, _}} ->
            0;
        {ok, Chunk, Cont} ->
            ok = filelib:ensure_dir(Filepath),
            {ok, FD} = file:open(Filepath, [raw, write, binary]),
            try
                ok = file:write(FD, Chunk),
                NRetry = 2,
                stream_trace_log(Node, Name, Cont, byte_size(Chunk), NRetry, FD)
            after
                ok = file:close(FD)
            end;
        Error ->
            Error
    end.

stream_trace_log(Node, Name, {cont, _} = Cont, NWritten, NRetry, FD) ->
    case node_stream_trace_log(Node, Name, Cont, undefined) of
        {ok, Chunk, NCont} ->
            ok = file:write(FD, Chunk),
            stream_trace_log(Node, Name, NCont, NWritten + byte_size(Chunk), NRetry, FD);
        Error ->
            Error
    end;
stream_trace_log(_Node, _Name, {retry, _}, _NWritten, 0, _FD) ->
    {error, inconsistent};
stream_trace_log(Node, Name, {retry, Cursor}, NWritten, NRetry, FD) ->
    ok = timer:sleep(?STREAM_TRACE_RETRY_TIMEOUT),
    stream_trace_log(Node, Name, {cont, Cursor}, NWritten, NRetry - 1, FD);
stream_trace_log(_Node, _Name, {eof, _}, NWritten, _NRetry, _FD) ->
    NWritten.

serve_trace_log_archive(_Trace = #{name := Name}, ZipDir, Files) ->
    %% Write files to ZipDir and create an in-memory zip file
    ZipName = binary_to_list(Name) ++ ".zip",
    Binary =
        try
            {ok, {ZipName, Bin}} = zip:zip(ZipName, Files, [memory, {cwd, ZipDir}]),
            Bin
        after
            %% emqx_trace:delete_files_after_send(ZipFileName, Zips),
            %% TODO use file replace file_binary.(delete file after send is not ready now).
            ok = file:del_dir_r(ZipDir)
        end,
    ?tp(trace_api_download_trace_log, #{
        files => Files,
        name => Name,
        zip_dir => ZipDir,
        zip_name => ZipName
    }),
    Headers = #{
        <<"content-type">> => <<"application/x-zip">>,
        <<"content-disposition">> => iolist_to_binary(
            "attachment; filename=" ++ ZipName
        )
    },
    {200, Headers, {file_binary, ZipName, Binary}}.

log_file_detail(get, #{bindings := #{name := Name}}) ->
    case emqx_trace:get(Name) of
        {ok, _Trace} ->
            Details = cluster_trace_details(Name),
            {200, filter_trace_details(Details)};
        {error, not_found} ->
            ?NOT_FOUND_WITH_MSG(Name)
    end.

filter_trace_details(TraceLogDetail) ->
    GroupFun =
        fun
            ({ok, Info}, Acc) ->
                [Info | Acc];
            ({error, Error}, Acc) ->
                ?SLOG(error, Error#{msg => "get_trace_details_failed"}),
                Acc
        end,
    lists:foldl(GroupFun, [], TraceLogDetail).

stream_log_file(get, #{bindings := #{name := Name}, query_string := Query}) ->
    Position = maps:get(<<"position">>, Query, 0),
    Bytes = maps:get(<<"bytes">>, Query, 1000),
    maybe
        {ok, Node} ?= parse_node(Query, node()),
        {ok, Cont} ?= parse_position(Position),
        case node_stream_trace_log(Node, Name, Cont, Bytes) of
            {ok, Bin, NCont} ->
                Meta = encode_meta(NCont, Bytes),
                {200, #{meta => Meta, items => Bin}};
            %% the waiting trace should return "" not error.
            {error, {file_error, enoent}} ->
                Meta = #{
                    <<"position">> => Position,
                    <<"hint">> => <<"eof">>,
                    <<"bytes">> => Bytes
                },
                {200, #{meta => Meta, items => <<>>}};
            {error, {file_error, enomem}} ->
                ?SLOG(warning, #{
                    code => not_enough_mem,
                    msg => "Requested chunk size too big",
                    bytes => Bytes,
                    name => Name
                }),
                ?SERVICE_UNAVAILABLE(<<"Requested chunk size too big">>);
            {error, not_found} ->
                ?NOT_FOUND_WITH_MSG(Name);
            {error, bad_cursor} ->
                ?BAD_REQUEST(<<"INVALID_PARAMETER">>, <<"Invalid cursor">>);
            {error, stale_cursor} ->
                ?BAD_REQUEST(<<"STALE_CURSOR">>, <<"Stale cursor">>);
            {badrpc, nodedown} ->
                ?SERVICE_UNAVAILABLE(<<"Node is unavailable">>)
        end
    else
        {error, not_found} ->
            ?NOT_FOUND_WITH_MSG(<<"Node">>);
        {error, bad_cursor} ->
            ?BAD_REQUEST(<<"Invalid cursor">>)
    end.

parse_node(Query, Default) ->
    try
        case maps:find(<<"node">>, Query) of
            error ->
                {ok, Default};
            {ok, NodeBin} ->
                Node = binary_to_existing_atom(NodeBin),
                true = lists:member(Node, emqx:running_nodes()),
                {ok, Node}
        end
    catch
        _:_ ->
            {error, not_found}
    end.

parse_position(Bin) when is_binary(Bin) ->
    try
        {ok, {cont, binary_to_term(emqx_base62:decode(Bin), [safe])}}
    catch
        error:_ ->
            {error, bad_cursor}
    end;
parse_position(0) ->
    %% NOTE: Backward compatibility.
    {ok, start};
parse_position(_) ->
    {error, bad_cursor}.

encode_meta(Cont = {What, _}, Bytes) ->
    Meta = #{<<"position">> => encode_position(Cont), <<"bytes">> => Bytes},
    case What of
        cont -> Meta;
        eof -> Meta#{<<"hint">> => <<"eof">>};
        retry -> Meta#{<<"hint">> => <<"retry">>}
    end.

encode_position({_, Cursor}) ->
    emqx_base62:encode(term_to_binary(Cursor)).

collect_file_size(Nodes, Name, AllTraceSizes) ->
    lists:foldl(
        fun(Node, Acc) ->
            Size = maps:get({Node, Name}, AllTraceSizes, 0),
            Acc#{Node => Size}
        end,
        #{},
        Nodes
    ).

%% RPC

cluster_get_trace_size() ->
    Nodes = supported_running_nodes(),
    Responses = filter_bad_replies(emqx_mgmt_trace_proto_v3:list_trace_sizes(Nodes)),
    lists:foldl(fun(F, Acc) -> maps:merge(Acc, F) end, #{}, Responses).

cluster_trace_details(Name) ->
    Nodes = supported_running_nodes(),
    filter_bad_replies(emqx_mgmt_trace_proto_v3:get_trace_details(Nodes, Name)).

node_stream_trace_log(Node, Name, Cont, Limit) ->
    emqx_mgmt_trace_proto_v3:stream_trace_log(Node, Name, Cont, Limit).

filter_bad_replies({GoodRes, BadNodes}) ->
    BadNodes =/= [] andalso
        ?SLOG(error, #{msg => "rpc_call_failed", bad_nodes => BadNodes}),
    GoodRes.

supported_running_nodes() ->
    emqx_bpapi:nodes_supporting_bpapi_version(emqx_mgmt_trace, 3).

%% RPC Targets

%% See `emqx_mgmt_trace_proto_v3:list_trace_sizes/1`.
%% Name is kept for backward compatibility.
-spec get_trace_size() -> #{{node(), binary()} => non_neg_integer()}.
get_trace_size() ->
    lists:foldl(
        fun(#{name := Name}, Acc) ->
            case emqx_trace:log_details(Name) of
                {ok, #{size := Size}} ->
                    Acc#{{node(), Name} => Size};
                {error, _} ->
                    Acc
            end
        end,
        #{},
        emqx_trace:list()
    ).

%% See `emqx_mgmt_trace_proto_v3:get_trace_details/2`.
-spec get_trace_details(emqx_trace:name()) ->
    {ok, #{
        size => non_neg_integer(),
        mtime => file:date_time() | non_neg_integer(),
        node => atom()
    }}
    | {error, #{
        reason => term(),
        node => atom()
    }}.
get_trace_details(Name) ->
    maybe
        {ok, Details} ?= emqx_trace:log_details(Name),
        {ok, Details#{node => node()}}
    else
        {error, Reason} ->
            {error, #{reason => Reason, node => node()}}
    end.

%% See `emqx_mgmt_trace_proto_v3:stream_trace_log/4`.
%% Name is kept for backward compatibility.
read_trace_file(_Name, _Position = 0, _Limit) ->
    %% NOTE: Backward compatibility measure.
    {ok, <<>>};
read_trace_file(Name, Cont, Limit) ->
    emqx_trace:stream_log(Name, Cont, Limit).
