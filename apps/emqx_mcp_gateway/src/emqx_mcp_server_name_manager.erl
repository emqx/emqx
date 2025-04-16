-module(emqx_mcp_server_name_manager).

-feature(maybe_expr, enable).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-export([create_tables/0]).

%% API
-export([start_link/0, load_from_csv/1, get_server_name/1]).

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

%% Types
-define(SERVER, ?MODULE).
-define(MCP_SERVER_NAME_TAB, emqx_mcp_server_name).

-record(mcp_server_name, {
    username :: binary(),
    server_name :: binary(),
    meta = #{} :: map()
}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------
create_tables() ->
    ok = mria:create_table(
        ?MCP_SERVER_NAME_TAB,
        [
            {type, ordered_set},
            {storage, disc_copies},
            {local_content, true},
            {record_name, mcp_server_name},
            {attributes, record_info(fields, mcp_server_name)}
        ]
    ),
    [?MCP_SERVER_NAME_TAB].

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_server_name(Username) ->
    case mnesia:dirty_read(?MCP_SERVER_NAME_TAB, Username) of
        [] ->
            {error, not_found};
        [#mcp_server_name{server_name = ServerName}] ->
            {ok, ServerName}
    end.

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
            case mnesia:dirty_first(?MCP_SERVER_NAME_TAB) of
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
                mnesia:dirty_write(?MCP_SERVER_NAME_TAB, Record)
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
