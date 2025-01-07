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
-module(emqx_mgmt_api_trace).

-behaviour(minirest_api).

-include_lib("kernel/include/file.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").

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

%% for rpc
-export([
    read_trace_file/3,
    get_trace_size/0
]).

-define(MAX_SINT32, 2147483647).

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
                        'INVALID_PARAMS'
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
                        {meta, fields(bytes) ++ fields(position)}
                    ],
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], <<"Bad input parameter">>
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
                %% This seems to be the minimum max value we may encounter
                %% across different OS
                range(0, ?MAX_SINT32),
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
                integer(),
                #{
                    description => ?DESC(current_trace_offset),
                    in => query,
                    required => false,
                    default => 0
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
                emqx_trace:format(List0)
            ),
            Nodes = emqx:running_nodes(),
            TraceSize = wrap_rpc(emqx_mgmt_trace_proto_v2:get_trace_size(Nodes)),
            AllFileSize = lists:foldl(fun(F, Acc) -> maps:merge(Acc, F) end, #{}, TraceSize),
            Now = erlang:system_time(second),
            Traces =
                lists:map(
                    fun(
                        Trace = #{
                            name := Name,
                            start_at := Start,
                            end_at := End,
                            enable := Enable,
                            type := Type,
                            filter := Filter
                        }
                    ) ->
                        FileName = emqx_trace:filename(Name, Start),
                        LogSize = collect_file_size(Nodes, FileName, AllFileSize),
                        Trace0 = maps:without([enable, filter], Trace),
                        Trace0#{
                            log_size => LogSize,
                            Type => iolist_to_binary(Filter),
                            start_at => emqx_utils_calendar:epoch_to_rfc3339(Start, second),
                            end_at => emqx_utils_calendar:epoch_to_rfc3339(End, second),
                            status => status(Enable, Start, End, Now)
                        }
                    end,
                    List
                ),
            {200, Traces}
    end;
trace(post, #{body := Param}) ->
    case emqx_trace:create(Param) of
        {ok, Trace0} ->
            {200, format_trace(Trace0)};
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
        {error, Reason} ->
            {400, #{
                code => 'INVALID_PARAMS',
                message => ?TO_BIN(Reason)
            }}
    end;
trace(delete, _Param) ->
    ok = emqx_trace:clear(),
    {204}.

format_trace(Trace0) ->
    [
        #{
            start_at := Start,
            end_at := End,
            enable := Enable,
            type := Type,
            filter := Filter
        } = Trace1
    ] = emqx_trace:format([Trace0]),
    Now = erlang:system_time(second),
    LogSize = lists:foldl(
        fun(Node, Acc) -> Acc#{Node => 0} end,
        #{},
        emqx:running_nodes()
    ),
    Trace2 = maps:without([enable, filter], Trace1),
    Trace2#{
        log_size => LogSize,
        Type => iolist_to_binary(Filter),
        start_at => emqx_utils_calendar:epoch_to_rfc3339(Start, second),
        end_at => emqx_utils_calendar:epoch_to_rfc3339(Start, second),
        status => status(Enable, Start, End, Now)
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
    case emqx_trace:get_trace_filename(Name) of
        {ok, TraceLog} ->
            case parse_node(Query, undefined) of
                {ok, Node} ->
                    TraceFiles = collect_trace_file(Node, TraceLog),
                    maybe_download_trace_log(Name, TraceLog, TraceFiles);
                {error, not_found} ->
                    ?NOT_FOUND_WITH_MSG(<<"Node">>)
            end;
        {error, not_found} ->
            ?NOT_FOUND_WITH_MSG(Name)
    end.

maybe_download_trace_log(Name, TraceLog, TraceFiles) ->
    case group_trace_files(TraceLog, TraceFiles) of
        #{nonempty := Files} ->
            do_download_trace_log(Name, TraceLog, Files);
        #{error := Reasons} ->
            ?INTERNAL_ERROR(Reasons);
        #{empty := _} ->
            ?NOT_FOUND(<<"Trace is empty">>)
    end.

do_download_trace_log(Name, TraceLog, TraceFiles) ->
    %% We generate a session ID so that we name files
    %% with unique names. Then we won't cause
    %% overwrites for concurrent requests.
    SessionId = emqx_utils:gen_id(),
    ZipDir = filename:join([emqx_trace:zip_dir(), SessionId]),
    ok = file:make_dir(ZipDir),
    %% Write files to ZipDir and create an in-memory zip file
    Zips = group_trace_file(ZipDir, TraceLog, TraceFiles),
    ZipName = binary_to_list(Name) ++ ".zip",
    Binary =
        try
            {ok, {ZipName, Bin}} = zip:zip(ZipName, Zips, [memory, {cwd, ZipDir}]),
            Bin
        after
            %% emqx_trace:delete_files_after_send(ZipFileName, Zips),
            %% TODO use file replace file_binary.(delete file after send is not ready now).
            ok = file:del_dir_r(ZipDir)
        end,
    ?tp(trace_api_download_trace_log, #{
        files => Zips,
        name => Name,
        session_id => SessionId,
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

group_trace_files(TraceLog, TraceFiles) ->
    maps:groups_from_list(
        fun
            ({ok, _Node, <<>>}) ->
                empty;
            ({ok, _Node, _Bin}) ->
                nonempty;
            ({error, _Node, enoent}) ->
                empty;
            ({error, Node, Reason}) ->
                ?SLOG(error, #{
                    msg => "download_trace_log_error",
                    node => Node,
                    log => TraceLog,
                    reason => Reason
                }),
                error
        end,
        TraceFiles
    ).

group_trace_file(ZipDir, TraceLog, TraceFiles) ->
    lists:foldl(
        fun({ok, Node, Bin}, Acc) ->
            FileName = Node ++ "-" ++ TraceLog,
            ZipName = filename:join([ZipDir, FileName]),
            case file:write_file(ZipName, Bin) of
                ok -> [FileName | Acc];
                _ -> Acc
            end
        end,
        [],
        TraceFiles
    ).

collect_trace_file(undefined, TraceLog) ->
    Nodes = emqx:running_nodes(),
    wrap_rpc(emqx_mgmt_trace_proto_v2:trace_file(Nodes, TraceLog));
collect_trace_file(Node, TraceLog) ->
    wrap_rpc(emqx_mgmt_trace_proto_v2:trace_file([Node], TraceLog)).

collect_trace_file_detail(TraceLog) ->
    Nodes = emqx:running_nodes(),
    wrap_rpc(emqx_mgmt_trace_proto_v2:trace_file_detail(Nodes, TraceLog)).

wrap_rpc({GoodRes, BadNodes}) ->
    BadNodes =/= [] andalso
        ?SLOG(error, #{msg => "rpc_call_failed", bad_nodes => BadNodes}),
    GoodRes.

log_file_detail(get, #{bindings := #{name := Name}}) ->
    case emqx_trace:get_trace_filename(Name) of
        {ok, TraceLog} ->
            TraceFiles = collect_trace_file_detail(TraceLog),
            {200, group_trace_file_detail(TraceFiles)};
        {error, not_found} ->
            ?NOT_FOUND_WITH_MSG(Name)
    end.

group_trace_file_detail(TraceLogDetail) ->
    GroupFun =
        fun
            ({ok, Info}, Acc) ->
                [Info | Acc];
            ({error, Error}, Acc) ->
                ?SLOG(error, Error#{msg => "read_trace_file_failed"}),
                Acc
        end,
    lists:foldl(GroupFun, [], TraceLogDetail).

stream_log_file(get, #{bindings := #{name := Name}, query_string := Query}) ->
    Position = maps:get(<<"position">>, Query, 0),
    Bytes = maps:get(<<"bytes">>, Query, 1000),
    case parse_node(Query, node()) of
        {ok, Node} ->
            case emqx_mgmt_trace_proto_v2:read_trace_file(Node, Name, Position, Bytes) of
                {ok, Bin} ->
                    Meta = #{<<"position">> => Position + byte_size(Bin), <<"bytes">> => Bytes},
                    {200, #{meta => Meta, items => Bin}};
                {eof, Size} ->
                    Meta = #{<<"position">> => Size, <<"bytes">> => Bytes},
                    {200, #{meta => Meta, items => <<"">>}};
                %% the waiting trace should return "" not error.
                {error, enoent} ->
                    Meta = #{<<"position">> => Position, <<"bytes">> => Bytes},
                    {200, #{meta => Meta, items => <<"">>}};
                {error, not_found} ->
                    ?NOT_FOUND_WITH_MSG(Name);
                {error, enomem} ->
                    ?SLOG(warning, #{
                        code => not_enough_mem,
                        msg => "Requested chunk size too big",
                        bytes => Bytes,
                        name => Name
                    }),
                    ?SERVICE_UNAVAILABLE(<<"Requested chunk size too big">>);
                {badrpc, nodedown} ->
                    ?NOT_FOUND_WITH_MSG(<<"Node">>)
            end;
        {error, not_found} ->
            ?NOT_FOUND_WITH_MSG(<<"Node">>)
    end.

-spec get_trace_size() -> #{{node(), file:name_all()} => non_neg_integer()}.
get_trace_size() ->
    TraceDir = emqx_trace:trace_dir(),
    Node = node(),
    case file:list_dir(TraceDir) of
        {ok, AllFiles} ->
            lists:foldl(
                fun(File, Acc) ->
                    FullFileName = filename:join(TraceDir, File),
                    Acc#{{Node, File} => filelib:file_size(FullFileName)}
                end,
                #{},
                lists:delete("zip", AllFiles)
            );
        _ ->
            #{}
    end.

%% this is an rpc call for stream_log_file/2
-spec read_trace_file(
    binary(),
    non_neg_integer(),
    non_neg_integer()
) ->
    {ok, binary()}
    | {error, _}
    | {eof, non_neg_integer()}.
read_trace_file(Name, Position, Limit) ->
    case emqx_trace:get_trace_filename(Name) of
        {error, _} = Error ->
            Error;
        {ok, TraceFile} ->
            TraceDir = emqx_trace:trace_dir(),
            TracePath = filename:join([TraceDir, TraceFile]),
            read_file(TracePath, Position, Limit)
    end.

read_file(Path, Offset, Bytes) ->
    case file:open(Path, [read, raw, binary]) of
        {ok, IoDevice} ->
            try
                _ =
                    case Offset of
                        0 -> ok;
                        _ -> file:position(IoDevice, {bof, Offset})
                    end,
                case file:read(IoDevice, Bytes) of
                    {ok, Bin} ->
                        {ok, Bin};
                    {error, Reason} ->
                        {error, Reason};
                    eof ->
                        {ok, #file_info{size = Size}} = file:read_file_info(IoDevice),
                        {eof, Size}
                end
            after
                file:close(IoDevice)
            end;
        {error, Reason} ->
            {error, Reason}
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

collect_file_size(Nodes, FileName, AllFiles) ->
    lists:foldl(
        fun(Node, Acc) ->
            Size = maps:get({Node, FileName}, AllFiles, 0),
            Acc#{Node => Size}
        end,
        #{},
        Nodes
    ).

status(false, _Start, _End, _Now) -> <<"stopped">>;
status(true, Start, _End, Now) when Now < Start -> <<"waiting">>;
status(true, _Start, End, Now) when Now >= End -> <<"stopped">>;
status(true, _Start, _End, _Now) -> <<"running">>.
