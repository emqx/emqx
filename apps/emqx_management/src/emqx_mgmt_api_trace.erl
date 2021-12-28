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
-module(emqx_mgmt_api_trace).

-behaviour(minirest_api).

-include_lib("kernel/include/file.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ api_spec/0
        , fields/1
        , paths/0
        , schema/1
        , namespace/0
        ]).

-export([ trace/2
        , delete_trace/2
        , update_trace/2
        , download_trace_log/2
        , stream_log_file/2
        ]).

-export([validate_name/1]).

%% for rpc
-export([read_trace_file/3
        , get_trace_size/0
        ]).

-define(TO_BIN(_B_), iolist_to_binary(_B_)).
-define(NOT_FOUND(N), {404, #{code => 'NOT_FOUND', message => ?TO_BIN([N, " NOT FOUND"])}}).

namespace() -> "trace".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/trace", "/trace/:name/stop", "/trace/:name/download", "/trace/:name/log", "/trace/:name"].


schema("/trace") ->
    #{
        'operationId' => trace,
        get => #{
            description => "List all trace",
            responses => #{
                200 => hoconsc:ref(trace)
            }
        },
        post => #{
            description => "Create new trace",
            'requestBody' => delete([status, log_size], fields(trace)),
            responses => #{
                200 => hoconsc:ref(trace)
            }
        },
        delete => #{
            description => "Clear all traces",
            responses => #{
                204 => <<"No Content">>
            }
        }
    };
schema("/trace/:name") ->
    #{
        'operationId' => delete_trace,
        delete => #{
            description => "Delete trace by name",
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
            description => "Stop trace by name",
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
            description => "Download trace log by name",
            parameters => [hoconsc:ref(name)],
            %% todo zip file octet-stream
            responses => #{
                200 => <<"TODO octet-stream">>
            }
        }
    };
schema("/trace/:name/log") ->
    #{
        'operationId' => stream_log_file,
        get => #{
            description => "view trace log",
            parameters => [
                hoconsc:ref(name),
                hoconsc:ref(bytes),
                hoconsc:ref(position),
                hoconsc:ref(node)
            ],
            %% todo response data
            responses => #{
                200 => <<"TODO">>
            }
        }
    }.

fields(trace) ->
    [
        {name, hoconsc:mk(binary(),
            #{desc => "Unique and format by [a-zA-Z0-9-_]",
                validator => fun ?MODULE:validate_name/1,
                nullable => false,
                example => <<"EMQX-TRACE-1">>})},
        {type, hoconsc:mk(hoconsc:enum([clientid, topic, ip_address]),
            #{desc => """Filter type""",
                nullable => false,
                example => <<"clientid">>})},
        {topic, hoconsc:mk(binary(),
            #{desc => """support mqtt wildcard topic.""",
                nullable => true,
                example => <<"/dev/#">>})},
        {clientid, hoconsc:mk(binary(),
            #{desc => """mqtt clientid.""",
                nullable => true,
                example => <<"dev-001">>})},
        %% TODO add ip_address type in emqx_schema.erl
        {ip_address, hoconsc:mk(binary(),
            #{desc => "client ip address",
                nullable => true,
                example => <<"127.0.0.1">>
            })},
        {status, hoconsc:mk(hoconsc:enum([running, stopped, waiting]),
            #{desc => "trace status",
                nullable => true,
                example => running
            })},
        {start_at, hoconsc:mk(binary(),
            #{desc => "rfc3339 timestamp",
                nullable => true,
                example => <<"2021-11-04T18:17:38+08:00">>
            })},
        {end_at, hoconsc:mk(binary(),
            #{desc => "rfc3339 timestamp",
                nullable => true,
                example => <<"2021-11-05T18:17:38+08:00">>
            })},
        {log_size, hoconsc:mk(hoconsc:array(map()),
            #{desc => "trace log size",
                example => [#{<<"node">> => <<"emqx@127.0.0.1">>, <<"size">> => 1024}],
                nullable => true})}
    ];
fields(name) ->
    [{name, hoconsc:mk(binary(),
        #{
            desc => <<"[a-zA-Z0-9-_]">>,
            example => <<"EMQX-TRACE-1">>,
            in => path,
            validator => fun ?MODULE:validate_name/1
        })}
    ];
fields(node) ->
    [{node, hoconsc:mk(binary(),
        #{
            desc => "Node name",
            in => query,
            nullable => true
        })}];
fields(bytes) ->
    [{bytes, hoconsc:mk(integer(),
        #{
            desc => "Maximum number of bytes to store in request",
            in => query,
            nullable => true,
            default => 1000
        })}];
fields(position) ->
    [{position, hoconsc:mk(integer(),
        #{
            desc => "Offset from the current trace position.",
            in => query,
            nullable => true,
            default => 0
        })}].

-define(NAME_RE, "^[A-Za-z]+[A-Za-z0-9-_]*$").

validate_name(Name) ->
    NameLen = byte_size(Name),
    case NameLen > 0 andalso NameLen =< 256 of
        true ->
            case re:run(Name, ?NAME_RE) of
                nomatch -> {error, "Name should be " ?NAME_RE};
                _ -> ok
            end;
        false -> {error, "Name Length must =< 256"}
    end.

delete(Keys, Fields) ->
    lists:foldl(fun(Key, Acc) -> lists:keydelete(Key, 1, Acc) end, Fields, Keys).

trace(get, _Params) ->
    case emqx_trace:list() of
        [] -> {200, []};
        List0 ->
            List = lists:sort(fun(#{start_at := A}, #{start_at := B}) -> A > B end,
                emqx_trace:format(List0)),
            Nodes = mria_mnesia:running_nodes(),
            TraceSize = cluster_call(?MODULE, get_trace_size, [], 30000),
            AllFileSize = lists:foldl(fun(F, Acc) -> maps:merge(Acc, F) end, #{}, TraceSize),
            Now = erlang:system_time(second),
            Traces =
                lists:map(fun(Trace = #{name := Name, start_at := Start,
                    end_at := End, enable := Enable, type := Type, filter := Filter}) ->
                    FileName = emqx_trace:filename(Name, Start),
                    LogSize = collect_file_size(Nodes, FileName, AllFileSize),
                    Trace0 = maps:without([enable, filter], Trace),
                    Trace0#{log_size => LogSize
                        , Type => iolist_to_binary(Filter)
                        , start_at => list_to_binary(calendar:system_time_to_rfc3339(Start))
                        , end_at => list_to_binary(calendar:system_time_to_rfc3339(End))
                        , status => status(Enable, Start, End, Now)
                    }
                          end, List),
            {200, Traces}
    end;
trace(post, #{body := Param}) ->
    case emqx_trace:create(Param) of
        ok -> {200};
        {error, {already_existed, Name}} ->
            {400, #{
                code => 'ALREADY_EXISTED',
                message => ?TO_BIN([Name, " Already Exists"])
            }};
        {error, {duplicate_condition, Name}} ->
            {400, #{
                code => 'DUPLICATE_CONDITION',
                message => ?TO_BIN([Name, " Duplication Condition"])
            }};
        {error, Reason} ->
            {400, #{
                code => 'INCORRECT_PARAMS',
                message => ?TO_BIN(Reason)
            }}
    end;
trace(delete, _Param) ->
    ok = emqx_trace:clear(),
    {200}.

delete_trace(delete, #{bindings := #{name := Name}}) ->
    case emqx_trace:delete(Name) of
        ok -> {200};
        {error, not_found} -> ?NOT_FOUND(Name)
    end.

update_trace(put, #{bindings := #{name := Name}}) ->
    case emqx_trace:update(Name, false) of
        ok -> {200, #{enable => false, name => Name}};
        {error, not_found} -> ?NOT_FOUND(Name)
    end.

%% if HTTP request headers include accept-encoding: gzip and file size > 300 bytes.
%% cowboy_compress_h will auto encode gzip format.
download_trace_log(get, #{bindings := #{name := Name}}) ->
    case emqx_trace:get_trace_filename(Name) of
        {ok, TraceLog} ->
            TraceFiles = collect_trace_file(TraceLog),
            ZipDir = emqx_trace:zip_dir(),
            Zips = group_trace_file(ZipDir, TraceLog, TraceFiles),
            ZipFileName = ZipDir ++ binary_to_list(Name) ++ ".zip",
            {ok, ZipFile} = zip:zip(ZipFileName, Zips, [{cwd, ZipDir}]),
            emqx_trace:delete_files_after_send(ZipFileName, Zips),
            {200, ZipFile};
        {error, not_found} -> ?NOT_FOUND(Name)
    end.

group_trace_file(ZipDir, TraceLog, TraceFiles) ->
    lists:foldl(fun(Res, Acc) ->
        case Res of
            {ok, Node, Bin} ->
                ZipName = ZipDir ++ Node ++ "-" ++ TraceLog,
                case file:write_file(ZipName, Bin) of
                    ok -> [Node ++ "-" ++ TraceLog | Acc];
                    _ -> Acc
                end;
            {error, Node, Reason} ->
                ?LOG(error, "download trace log error:~p", [{Node, TraceLog, Reason}]),
                Acc
        end
                end, [], TraceFiles).

collect_trace_file(TraceLog) ->
    cluster_call(emqx_trace, trace_file, [TraceLog], 60000).

cluster_call(Mod, Fun, Args, Timeout) ->
    Nodes = mria_mnesia:running_nodes(),
    {GoodRes, BadNodes} = rpc:multicall(Nodes, Mod, Fun, Args, Timeout),
    BadNodes =/= [] andalso ?LOG(error, "rpc call failed on ~p ~p", [BadNodes, {Mod, Fun, Args}]),
    GoodRes.

stream_log_file(get, #{bindings := #{name := Name}, query_string := Query} = T) ->
    Node0 = maps:get(<<"node">>, Query, atom_to_binary(node())),
    Position = maps:get(<<"position">>, Query, 0),
    Bytes = maps:get(<<"bytes">>, Query, 1000),
    logger:error("~p", [T]),
    case to_node(Node0) of
        {ok, Node} ->
            case rpc:call(Node, ?MODULE, read_trace_file, [Name, Position, Bytes]) of
                {ok, Bin} ->
                    Meta = #{<<"position">> => Position + byte_size(Bin), <<"bytes">> => Bytes},
                    {200, #{meta => Meta, items => Bin}};
                {eof, Size} ->
                    Meta = #{<<"position">> => Size, <<"bytes">> => Bytes},
                    {200, #{meta => Meta, items => <<"">>}};
                {error, Reason} ->
                    logger:log(error, "read_file_failed ~p", [{Node, Name, Reason, Position, Bytes}]),
                    {400, #{code => 'READ_FILE_ERROR', message => Reason}};
                {badrpc, nodedown} ->
                    {400, #{code => 'RPC_ERROR', message => "BadRpc node down"}}
            end;
        {error, not_found} -> {400, #{code => 'NODE_ERROR', message => <<"Node not found">>}}
    end.

get_trace_size() ->
    TraceDir = emqx_trace:trace_dir(),
    Node = node(),
    case file:list_dir(TraceDir) of
        {ok, AllFiles} ->
            lists:foldl(fun(File, Acc) ->
                FullFileName = filename:join(TraceDir, File),
                Acc#{{Node, File} => filelib:file_size(FullFileName)}
                        end, #{}, lists:delete("zip", AllFiles));
        _ -> #{}
    end.

%% this is an rpc call for stream_log_file/2
read_trace_file(Name, Position, Limit) ->
    case emqx_trace:get_trace_filename(Name) of
        {error, _} = Error -> Error;
        {ok, TraceFile} ->
            TraceDir = emqx_trace:trace_dir(),
            TracePath = filename:join([TraceDir, TraceFile]),
            read_file(TracePath, Position, Limit)
    end.

read_file(Path, Offset, Bytes) ->
    case file:open(Path, [read, raw, binary]) of
        {ok, IoDevice} ->
            try
                _ = case Offset of
                        0 -> ok;
                        _ -> file:position(IoDevice, {bof, Offset})
                    end,
                case file:read(IoDevice, Bytes) of
                    {ok, Bin} -> {ok, Bin};
                    {error, Reason} -> {error, Reason};
                    eof ->
                        {ok, #file_info{size = Size}} = file:read_file_info(IoDevice),
                        {eof, Size}
                end
            after
                file:close(IoDevice)
            end;
        {error, Reason} -> {error, Reason}
    end.

to_node(Node) ->
    try {ok, binary_to_existing_atom(Node)}
    catch _:_ ->
        {error, not_found}
    end.

collect_file_size(Nodes, FileName, AllFiles) ->
    lists:foldl(fun(Node, Acc) ->
        Size = maps:get({Node, FileName}, AllFiles, 0),
        Acc#{Node => Size}
                end, #{}, Nodes).

status(false, _Start, _End, _Now) -> <<"stopped">>;
status(true, Start, _End, Now) when Now < Start -> <<"waiting">>;
status(true, _Start, End, Now) when Now >= End -> <<"stopped">>;
status(true, _Start, _End, _Now) -> <<"running">>.
