%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_logger).

-compile({no_auto_import, [error/1]}).

%% Logs
-export([ debug/1
        , debug/2
        , debug/3
        , info/1
        , info/2
        , info/3
        , warning/1
        , warning/2
        , warning/3
        , error/1
        , error/2
        , error/3
        , critical/1
        , critical/2
        , critical/3
        ]).

%% Configs
-export([ set_metadata_peername/1
        , set_metadata_clientid/1
        , set_proc_metadata/1
        , set_primary_log_level/1
        , set_log_handler_level/2
        , set_log_level/1
        , set_all_log_handlers_level/1
        ]).

-export([ get_primary_log_level/0
        , get_log_handlers/0
        , get_log_handler/1
        ]).

-export([parse_transform/2]).

-type(peername_str() :: list()).
-type(logger_dst() :: file:filename() | console | unknown).
-type(logger_handler_info() :: {logger:handler_id(), logger:level(), logger_dst()}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
-spec(debug(unicode:chardata()) -> ok).
debug(Msg) ->
    logger:debug(Msg).

-spec(debug(io:format(), [term()]) -> ok).
debug(Format, Args) ->
    logger:debug(Format, Args).

-spec(debug(logger:metadata(), io:format(), [term()]) -> ok).
debug(Metadata, Format, Args) when is_map(Metadata) ->
    logger:debug(Format, Args, Metadata).


-spec(info(unicode:chardata()) -> ok).
info(Msg) ->
    logger:info(Msg).

-spec(info(io:format(), [term()]) -> ok).
info(Format, Args) ->
    logger:info(Format, Args).

-spec(info(logger:metadata(), io:format(), [term()]) -> ok).
info(Metadata, Format, Args) when is_map(Metadata) ->
    logger:info(Format, Args, Metadata).


-spec(warning(unicode:chardata()) -> ok).
warning(Msg) ->
    logger:warning(Msg).

-spec(warning(io:format(), [term()]) -> ok).
warning(Format, Args) ->
    logger:warning(Format, Args).

-spec(warning(logger:metadata(), io:format(), [term()]) -> ok).
warning(Metadata, Format, Args) when is_map(Metadata) ->
    logger:warning(Format, Args, Metadata).


-spec(error(unicode:chardata()) -> ok).
error(Msg) ->
    logger:error(Msg).
-spec(error(io:format(), [term()]) -> ok).
error(Format, Args) ->
    logger:error(Format, Args).
-spec(error(logger:metadata(), io:format(), [term()]) -> ok).
error(Metadata, Format, Args) when is_map(Metadata) ->
    logger:error(Format, Args, Metadata).


-spec(critical(unicode:chardata()) -> ok).
critical(Msg) ->
    logger:critical(Msg).

-spec(critical(io:format(), [term()]) -> ok).
critical(Format, Args) ->
    logger:critical(Format, Args).

-spec(critical(logger:metadata(), io:format(), [term()]) -> ok).
critical(Metadata, Format, Args) when is_map(Metadata) ->
    logger:critical(Format, Args, Metadata).

-spec(set_metadata_clientid(emqx_types:clientid()) -> ok).
set_metadata_clientid(<<>>) ->
    ok;
set_metadata_clientid(ClientId) ->
    set_proc_metadata(#{clientid => ClientId}).

-spec(set_metadata_peername(peername_str()) -> ok).
set_metadata_peername(Peername) ->
    set_proc_metadata(#{peername => Peername}).

-spec(set_proc_metadata(logger:metadata()) -> ok).
set_proc_metadata(Meta) ->
    logger:update_process_metadata(Meta).

-spec(get_primary_log_level() -> logger:level()).
get_primary_log_level() ->
    #{level := Level} = logger:get_primary_config(),
    Level.

-spec(set_primary_log_level(logger:level()) -> ok | {error, term()}).
set_primary_log_level(Level) ->
    logger:set_primary_config(level, Level).

-spec(get_log_handlers() -> [logger_handler_info()]).
get_log_handlers() ->
    lists:map(fun log_hanlder_info/1, logger:get_handler_config()).

-spec(get_log_handler(logger:handler_id()) -> logger_handler_info()).
get_log_handler(HandlerId) ->
    {ok, Conf} = logger:get_handler_config(HandlerId),
    log_hanlder_info(Conf).

-spec(set_log_handler_level(logger:handler_id(), logger:level()) -> ok | {error, term()}).
set_log_handler_level(HandlerId, Level) ->
    logger:set_handler_config(HandlerId, level, Level).

%% @doc Set both the primary and all handlers level in one command
-spec(set_log_level(logger:handler_id()) -> ok | {error, term()}).
set_log_level(Level) ->
    case set_primary_log_level(Level) of
        ok -> set_all_log_handlers_level(Level);
        {error, Error} -> {error, {primary_logger_level, Error}}
    end.

%% @doc The parse transform for prefixing a module-specific logger header to the logs.
%% The logger header can be specified by "-logger_header(Header)", where Header
%% must be a string (list).
%% @end
parse_transform(AST, _Opts) ->
    trans(AST, "", []).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

log_hanlder_info(#{id := Id, level := Level, module := logger_std_h,
                   config := #{type := Type}}) when Type =:= standard_io;
                                                    Type =:= standard_error ->
    {Id, Level, console};
log_hanlder_info(#{id := Id, level := Level, module := logger_std_h,
                   config := Config = #{type := file}}) ->
    {Id, Level, maps:get(file, Config, atom_to_list(Id))};

log_hanlder_info(#{id := Id, level := Level, module := logger_disk_log_h,
                   config := #{file := Filename}}) ->
    {Id, Level, Filename};
log_hanlder_info(#{id := Id, level := Level, module := _OtherModule}) ->
    {Id, Level, unknown}.

%% set level for all log handlers in one command
set_all_log_handlers_level(Level) ->
    set_all_log_handlers_level(get_log_handlers(), Level, []).

set_all_log_handlers_level([{ID, Level, _Dst} | List], NewLevel, ChangeHistory) ->
    case set_log_handler_level(ID, NewLevel) of
        ok -> set_all_log_handlers_level(List, NewLevel, [{ID, Level} | ChangeHistory]);
        {error, Error} ->
            rollback(ChangeHistory),
            {error, {handlers_logger_level, {ID, Error}}}
    end;
set_all_log_handlers_level([], _NewLevel, _NewHanlder) ->
    ok.

rollback([{ID, Level} | List]) ->
    emqx_logger:set_log_handler_level(ID, Level),
    rollback(List);
rollback([]) -> ok.

%% @doc The following parse-transforms stripped off the module attribute named
%% `-logger_header(Header)` (if there's one) from the source code, and then
%% generate a function named '$logger_header'/0, which returns the logger header.
%% @end
trans([], LogHeader, ResAST) ->
    lists:reverse([header_fun(LogHeader) | ResAST]);
trans([{eof, L} | AST], LogHeader, ResAST) ->
    lists:reverse([{eof, L}, header_fun(LogHeader) | ResAST]) ++ AST;
trans([{attribute, _, module, _Mod} = M | AST], Header, ResAST) ->
    trans(AST, Header, [export_header_fun(), M | ResAST]);
trans([{attribute, _, logger_header, Header} | AST], _, ResAST) ->
    io_lib:printable_list(Header) orelse error({invalid_string, Header}),
    trans(AST, Header, ResAST);
trans([F | AST], LogHeader, ResAST) ->
    trans(AST, LogHeader, [F | ResAST]).

export_header_fun() ->
    {attribute,erl_anno:new(0),export,[{'$logger_header',0}]}.

header_fun(LogHeader) ->
    L = erl_anno:new(0),
    {function,L,'$logger_header',0,
         [{clause,L,
             [], [], [{string,L,pad(LogHeader)}]}]}.

pad("") -> "";
pad(Str) -> Str ++ " ".
