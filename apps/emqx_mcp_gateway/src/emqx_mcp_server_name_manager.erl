%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mcp_server_name_manager).

-feature(maybe_expr, enable).

-behaviour(gen_server).
-behaviour(emqx_db_backup).

-include_lib("emqx/include/logger.hrl").

%% Mnesia Tab APIs
-export([
    table_name/0,
    init_tables/0,
    create_tables/0,
    backup_tables/0
]).

%% API
-export([
    start_link/0
]).

-export([
    load_from_csv/1,
    get_server_name/1,
    add_server_name/2,
    delete_server_name/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([match_all_spec/0, fuzzy_filter_fun/1, run_fuzzy_filter/2, format_server_name/1]).

%% Types
-define(SERVER, ?MODULE).
-define(TAB, emqx_mcp_server_name).

-record(mcp_server_name, {
    username :: binary() | '_',
    server_name :: binary() | '_',
    meta = #{} :: map() | '_'
}).

table_name() -> ?TAB.

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------
-spec init_tables() -> ok.
init_tables() ->
    ok = mria:wait_for_tables(create_tables()).

create_tables() ->
    ok = mria:create_table(
        ?TAB,
        [
            {type, ordered_set},
            {storage, disc_copies},
            {local_content, true},
            {record_name, mcp_server_name},
            {attributes, record_info(fields, mcp_server_name)}
        ]
    ),
    [?TAB].

%%------------------------------------------------------------------------------
%% Data backup
%%------------------------------------------------------------------------------
backup_tables() -> {<<"mcp_server_name">>, [?TAB]}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_server_name(Username) ->
    case mnesia:dirty_read(?TAB, Username) of
        [] ->
            {error, not_found};
        [#mcp_server_name{server_name = ServerName}] ->
            {ok, ServerName}
    end.

add_server_name(Username, ServerName) ->
    Record = #mcp_server_name{username = Username, server_name = ServerName},
    mria:dirty_write(?TAB, Record).

delete_server_name(Username) ->
    mria:dirty_delete(?TAB, Username).

init([]) ->
    {ok, #{}, {continue, load_server_names}}.

handle_continue(load_server_names, State) ->
    case load_server_names() of
        ok -> ok;
        {error, Reason} -> ?SLOG(error, #{msg => load_server_names_failed, reason => Reason})
    end,
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Import From CSV
%%--------------------------------------------------------------------
load_server_names() ->
    case emqx:get_config([mcp, broker_suggested_server_name], #{}) of
        #{enable := false} ->
            ok;
        #{enable := true, bootstrap_file := FileName} when is_binary(FileName) ->
            case mnesia:dirty_first(?TAB) of
                '$end_of_table' ->
                    %% No data in the table, load from CSV
                    File = emqx_schema:naive_env_interpolation(FileName),
                    load_from_csv(File);
                _ ->
                    %% Already loaded
                    ok
            end;
        _ ->
            %% No bootstrap file specified
            ok
    end.

load_from_csv(File) ->
    maybe
        true ?= (core =:= mria_rlog:role()),
        {ok, ServerNamesBin} ?= file:read_file(File),
        Stream = emqx_utils_stream:csv(ServerNamesBin, #{nullable => true, filter_null => true}),
        ok ?= load_from_stream(Stream),
        ?SLOG(info, #{
            msg => "load_server_name_bootstrap_file_succeeded",
            file => File
        })
    else
        false -> ok;
        {error, _} = Error -> Error
    end.

load_from_stream(Stream) ->
    try
        Lines = emqx_utils_stream:consume(Stream),
        ServerNameRecords = [format_line(Line) || Line <- Lines],
        lists:foreach(
            fun(Record) ->
                mria:dirty_write(?TAB, Record)
            end,
            ServerNameRecords
        )
    catch
        error:Reason ->
            {error, Reason}
    end.

format_line(#{<<"username">> := Username, <<"server_name">> := ServerName}) ->
    #mcp_server_name{
        username = Username,
        server_name = ServerName
    };
format_line(Line) ->
    throw({error, {invalid_line_format, Line}}).

match_all_spec() ->
    [{#mcp_server_name{_ = '_'}, [], ['$_']}].

%% Fuzzy username funcs
fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = #mcp_server_name{username = Username},
    [{username, like, UsernameSubStr} | Fuzzy]
) ->
    binary:match(Username, UsernameSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

format_server_name(#mcp_server_name{username = Username, server_name = ServerName}) ->
    #{
        username => Username,
        server_name => ServerName
    }.
