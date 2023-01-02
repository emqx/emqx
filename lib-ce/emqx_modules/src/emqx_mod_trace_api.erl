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

-module(emqx_mod_trace_api).

%% API
-export([ list_trace/2
        , create_trace/2
        , disable_trace/2
        , delete_trace/2
        , clear_traces/2
        , trace_file_detail/2
        , download_zip_log/2
        , stream_log_file/2
]).

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

-rest_api(#{name   => disable_trace,
            method => 'PUT',
            path   => "/trace/:bin:name/stop",
            func   => disable_trace,
            descr  => "stop trace"}).

-rest_api(#{name   => trace_file_detail,
            method => 'GET',
            path   => "/trace/:bin:name/detail",
            func   => trace_file_detail,
            descr  => "view trace file's detail"}).

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

-define(NOT_STARTED, {error, module_not_loaded}).
list_trace(Path, Params) ->
    case is_started() of
        true -> return(emqx_trace_api:list_trace(Path, Params));
        false -> return(?NOT_STARTED)
    end.

create_trace(Path, Params) ->
    case is_started() of
        true -> return(emqx_trace_api:create_trace(Path, Params));
        false -> return(?NOT_STARTED)
    end.

delete_trace(Path, Params) ->
    case is_started() of
        true -> return(emqx_trace_api:delete_trace(Path, Params));
        false -> return(?NOT_STARTED)
    end.

clear_traces(Path, Params) ->
    case is_started() of
        true -> return(emqx_trace_api:clear_traces(Path, Params));
        false -> return(?NOT_STARTED)
    end.

disable_trace(#{name := Name}, Params) ->
    case is_started() of
        true -> return(emqx_trace_api:update_trace(#{name => Name, operation => disable}, Params));
        false -> return(?NOT_STARTED)
    end.

trace_file_detail(Path, Params) ->
    case emqx_trace_api:trace_file_detail(Path, Params) of
        {ok, Detail} -> return({ok, Detail});
        {error, Reason} ->  return({error, 'NOT_FOUND', Reason})
    end.

download_zip_log(Path, Params) ->
    case emqx_trace_api:download_zip_log(Path, Params) of
        {ok, File} -> minirest:return_file(File);
        {error, Reason} ->  return({error, 'NOT_FOUND', Reason})
    end.

stream_log_file(Path, Params) ->
    case emqx_trace_api:stream_log_file(Path, Params) of
        {ok, File} -> return({ok, File});
        {error, Reason} -> return({error, 'NOT_FOUND', Reason})
    end.

is_started() ->
    undefined =/= erlang:whereis(emqx_trace).
