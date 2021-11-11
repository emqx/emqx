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

-module(emqx_trace_api).
-include_lib("emqx/include/logger.hrl").

%% API
-export([ list_trace/2
        , create_trace/2
        , update_trace/2
        , delete_trace/2
        , clear_traces/2
        , download_zip_log/2
        , stream_log_file/2
]).
-export([read_trace_file/3]).

-define(TO_BIN(_B_), iolist_to_binary(_B_)).
-define(RETURN_NOT_FOUND(N), return({error, 'NOT_FOUND', ?TO_BIN([N, "NOT FOUND"])})).

-import(minirest, [return/1]).

list_trace(_, Params) ->
    List =
        case Params of
            [{<<"enable">>, Enable}] -> emqx_trace:list(Enable);
            _ -> emqx_trace:list()
        end,
    return({ok, emqx_trace:format(List)}).

create_trace(_, Param) ->
    case emqx_trace:create(Param) of
        ok -> return(ok);
        {error, {already_existed, Name}} ->
            return({error, 'ALREADY_EXISTED', ?TO_BIN([Name, "Already Exists"])});
        {error, {duplicate_condition, Name}} ->
            return({error, 'DUPLICATE_CONDITION', ?TO_BIN([Name, "Duplication Condition"])});
        {error, Reason} ->
            return({error, 'INCORRECT_PARAMS', ?TO_BIN(Reason)})
    end.

delete_trace(#{name := Name}, _Param) ->
    case emqx_trace:delete(Name) of
        ok -> return(ok);
        {error, not_found} -> ?RETURN_NOT_FOUND(Name)
    end.

clear_traces(_, _) ->
    return(emqx_trace:clear()).

update_trace(#{name := Name, operation := Operation}, _Param) ->
    Enable = case Operation of disable -> false; enable -> true end,
    case emqx_trace:update(Name, Enable) of
        ok -> return({ok, #{enable => Enable, name => Name}});
        {error, not_found} -> ?RETURN_NOT_FOUND(Name)
    end.

%% if HTTP request headers include accept-encoding: gzip and file size > 300 bytes.
%% cowboy_compress_h will auto encode gzip format.
download_zip_log(#{name := Name}, _Param) ->
    case emqx_trace:get_trace_filename(Name) of
        {ok, TraceLog} ->
            TraceFiles = collect_trace_file(TraceLog),
            ZipDir = emqx_trace:zip_dir(),
            Zips = group_trace_file(ZipDir, TraceLog, TraceFiles),
            ZipFileName = ZipDir ++ TraceLog,
            {ok, ZipFile} = zip:zip(ZipFileName, Zips),
            emqx_trace:delete_files_after_send(ZipFileName, Zips),
            {ok, #{}, {sendfile, 0, filelib:file_size(ZipFile), ZipFile}};
        {error, Reason} ->
            return({error, Reason})
    end.

group_trace_file(ZipDir, TraceLog, TraceFiles) ->
    lists:foldl(fun(Res, Acc) ->
        case Res of
            {ok, Node, Bin} ->
                ZipName = ZipDir ++ Node ++ "-" ++ TraceLog,
                ok = file:write_file(ZipName, Bin),
                [ZipName | Acc];
            {error, Node, Reason} ->
                ?LOG(error, "download trace log error:~p", [{Node, TraceLog, Reason}]),
                Acc
        end
                end, [], TraceFiles).

collect_trace_file(TraceLog) ->
    Nodes = ekka_mnesia:running_nodes(),
    {Files, BadNodes} = rpc:multicall(Nodes, emqx_trace, trace_file, [TraceLog], 60000),
    BadNodes =/= [] andalso ?LOG(error, "download log rpc failed on ~p", [BadNodes]),
    Files.

%% _page as position and _limit as bytes for front-end reusing components
stream_log_file(#{name := Name}, Params) ->
    Node0 = proplists:get_value(<<"node">>, Params, atom_to_binary(node())),
    Position0 = proplists:get_value(<<"_page">>, Params, <<"0">>),
    Bytes0 = proplists:get_value(<<"_limit">>, Params, <<"500">>),
    Node = binary_to_existing_atom(Node0),
    Position = binary_to_integer(Position0),
    Bytes = binary_to_integer(Bytes0),
    case rpc:call(Node, ?MODULE, read_trace_file, [Name, Position, Bytes]) of
        {ok, Bin} ->
            Meta = #{<<"page">> => Position + byte_size(Bin), <<"limit">> => Bytes},
            return({ok, #{meta => Meta, items => Bin}});
        eof ->
            Meta = #{<<"page">> => Position, <<"limit">> => Bytes},
            return({ok, #{meta => Meta, items => <<"">>}});
        {error, Reason} ->
            logger:log(error, "read_file_failed by ~p", [{Name, Reason, Position, Bytes}]),
            return({error, Reason})
    end.

%% this is an rpc call for stream_log_file/2
read_trace_file(Name, Position, Limit) ->
    TraceDir = emqx_trace:trace_dir(),
    {ok, AllFiles} = file:list_dir(TraceDir),
    TracePrefix = "trace_" ++ binary_to_list(Name) ++ "_",
    Filter = fun(FileName) -> nomatch =/= string:prefix(FileName, TracePrefix) end,
    case lists:filter(Filter, AllFiles) of
        [TraceFile] ->
            TracePath = filename:join([TraceDir, TraceFile]),
            read_file(TracePath, Position, Limit);
        [] -> {error, not_found}
    end.

read_file(Path, Offset, Bytes) ->
    {ok, IoDevice} = file:open(Path, [read, raw, binary]),
    try
        _ = case Offset of
                0 -> ok;
                _ -> file:position(IoDevice, {bof, Offset})
            end,
        file:read(IoDevice, Bytes)
    after
        file:close(IoDevice)
    end.
