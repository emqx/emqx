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

-module(emqx_mod_trace_api).
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
-define(TEXT_HEADER, #{<<"content-type">> => <<"text/plain">>}).

-import(minirest, [return/1]).

-rest_api(#{name   => list_trace,
            method => 'GET',
            path   => "/trace/",
            func   => list_trace,
            descr  => "list all traces"}).

-rest_api(#{name   => create_trace,
            method => 'POST',
            path   => "/trace/",
            func   => create_trace,
            descr  => "create trace"}).

-rest_api(#{name   => delete_trace,
            method => 'DELETE',
            path   => "/trace/:bin:name",
            func   => delete_trace,
            descr  => "delete trace"}).

-rest_api(#{name   => clear_trace,
            method => 'DELETE',
            path   => "/trace/",
            func   => clear_traces,
            descr  => "clear all traces"}).

-rest_api(#{name   => update_trace,
            method => 'PUT',
            path   => "/trace/:bin:name/:atom:operation",
            func   => update_trace,
            descr  => "diable/enable trace"}).

-rest_api(#{name   => download_zip_log,
            method => 'GET',
            path   => "/trace/:bin:name/download",
            func   => download_zip_log,
            descr  => "download trace's log"}).

-rest_api(#{name   => stream_log_file,
            method => 'GET',
            path   => "/trace/:bin:name/log",
            func   => stream_log_file,
            descr  => "download trace's log"}).

list_trace(_, Params) ->
    List =
        case Params of
            [{<<"enable">>, Enable}] -> emqx_mod_trace:list(Enable);
            _ -> emqx_mod_trace:list()
        end,
    return({ok, emqx_mod_trace:format(List)}).

create_trace(_, Param) ->
    case emqx_mod_trace:create(Param) of
        ok -> return(ok);
        {error, {already_existed, Name}} ->
            return({error, 'ALREADY_EXISTED', ?TO_BIN([Name, "Already Exists"])});
        {error, {duplicate_condition, Name}} ->
            return({error, 'DUPLICATE_CONDITION', ?TO_BIN([Name, "Duplication Condition"])});
        {error, Reason} ->
            return({error, 'INCORRECT_PARAMS', ?TO_BIN(Reason)})
    end.

delete_trace(#{name := Name}, _Param) ->
    case emqx_mod_trace:delete(Name) of
        ok -> return(ok);
        {error, not_found} ->
            return({error, 'NOT_FOUND', ?TO_BIN([Name, "NOT FOUND"])})
    end.

clear_traces(_, _) ->
    return(emqx_mod_trace:clear()).

update_trace(#{name := Name, operation := Operation}, _Param) ->
    Enable = case Operation of disable -> false; enable -> true end,
    case emqx_mod_trace:update(Name, Enable) of
        ok -> return({ok, #{enable => Enable, name => Name}});
        {error, not_found} ->
            return({error, 'NOT_FOUND', ?TO_BIN([Name, "NOT FOUND"])})
    end.

%% if HTTP request headers include accept-encoding: gzip and file size > 300 bytes.
%% cowboy_compress_h will auto encode gzip format.
download_zip_log(#{name := Name}, _Param) ->
    case emqx_mod_trace:get_trace_filename(Name) of
        {ok, TraceLog} ->
            TraceFiles = collect_trace_file(TraceLog),
            ZipDir = emqx_mod_trace:zip_dir(),
            Zips = group_trace_file(ZipDir, TraceLog, TraceFiles),
            ZipFileName = ZipDir ++ TraceLog,
            {ok, ZipFile} = zip:zip(ZipFileName, Zips),
            emqx_mod_trace:delete_files_after_send(ZipFileName, Zips),
            minirest:return_file(ZipFile);
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
    {File, BadNodes} = rpc:multicall(Nodes, emqx_mod_trace, trace_file, [TraceLog], 60000),
    BadNodes =/= [] andalso ?LOG(error, "download log rpc failed on ~p", [BadNodes]),
    File.

stream_log_file(#{name := Name}, Params) ->
    Node0 = proplists:get_value(<<"node">>, Params, atom_to_binary(node())),
    Position0 = proplists:get_value(<<"_position">>, Params, <<"0">>),
    Bytes0 = proplists:get_value(<<"_bytes">>, Params, <<"500">>),
    Node = binary_to_existing_atom(Node0),
    Position = binary_to_integer(Position0),
    Bytes = binary_to_integer(Bytes0),
    case rpc:call(Node, ?MODULE, read_trace_file, [Name, Position, Bytes]) of
        {ok, Bin} ->
            Meta = #{<<"_position">> => Position + byte_size(Bin), <<"_bytes">> => Bytes},
            return({ok, #{meta => Meta, bin => Bin}});
        eof ->
            Meta = #{<<"_position">> => Position, <<"_bytes">> => Bytes},
            return({ok, #{meta => Meta, bin => <<"">>}});
        {error, Reason} ->
            logger:log(error, "read_file_failed by ~p", [{Name, Reason, Position, Bytes}]),
            return({error, Reason})
    end.

read_trace_file(Name, Position, Limit) ->
    TraceDir = emqx_mod_trace:trace_dir(),
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
