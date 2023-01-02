%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_trace_handler).

-include("emqx.hrl").
-include("logger.hrl").

-logger_header("[Tracer]").

%% APIs
-export([ running/0
        , install/3
        , install/4
        , uninstall/1
        , uninstall/2
        ]).

%% For logger handler filters callbacks
-export([ filter_clientid/2
        , filter_topic/2
        , filter_ip_address/2
        ]).

-export([handler_id/2]).

-type tracer() :: #{
                    name := binary(),
                    type := clientid | topic | ip_address,
                    filter := emqx_types:clientid() | emqx_types:topic() | emqx_trace:ip_address()
                   }.

-define(FORMAT,
    {logger_formatter, #{
        template => [
            time, " [", level, "] ",
            {clientid,
                [{peername, [clientid, "@", peername, " "], [clientid, " "]}],
                [{peername, [peername, " "], []}]
            },
            msg, "\n"
        ],
        single_line => false,
        max_size => unlimited,
        depth => unlimited
    }}
).

-define(CLIENT_FORMAT,
    {logger_formatter, #{
        template => [
            time, " [", level, "] ",
            {clientid,
                [{peername, [clientid, "@", peername, " "], [clientid, " "]}],
                [{peername, [peername, " "], []}]
            },
            msg, "\n"
        ],
        single_line => false,
        max_size => unlimited,
        depth => unlimited
    }}
).

-define(CONFIG(_LogFile_), #{
    type => halt,
    file => _LogFile_,
    max_no_bytes => 512 * 1024 * 1024,
    overload_kill_enable => true,
    overload_kill_mem_size => 50 * 1024 * 1024,
    overload_kill_qlen => 20000,
    %% disable restart
    overload_kill_restart_after => infinity
    }).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec install(Name :: binary() | list(),
              Type :: clientid | topic | ip_address,
              Filter ::emqx_types:clientid() | emqx_types:topic() | string(),
              Level :: logger:level() | all,
              LogFilePath :: string()) -> ok | {error, term()}.
install(Name, Type, Filter, Level, LogFile) ->
    Who = #{type => Type, filter => ensure_bin(Filter), name => ensure_bin(Name)},
    install(Who, Level, LogFile).

-spec install(Type :: clientid | topic | ip_address,
              Filter ::emqx_types:clientid() | emqx_types:topic() | string(),
              Level :: logger:level() | all,
              LogFilePath :: string()) -> ok | {error, term()}.
install(Type, Filter, Level, LogFile) ->
    install(Filter, Type, Filter, Level, LogFile).

-spec install(tracer(), logger:level() | all, string()) -> ok | {error, term()}.
install(Who, all, LogFile) ->
    install(Who, debug, LogFile);
install(Who, Level, LogFile) ->
    PrimaryLevel = emqx_logger:get_primary_log_level(),
    try logger:compare_levels(Level, PrimaryLevel) of
        lt ->
            {error,
                io_lib:format(
                    "Cannot trace at a log level (~s) "
                    "lower than the primary log level (~s)",
                    [Level, PrimaryLevel]
                )};
        _GtOrEq ->
            install_handler(Who, Level, LogFile)
    catch
        error:badarg ->
            {error, {invalid_log_level, Level}}
    end.

-spec uninstall(Type :: clientid | topic | ip_address,
                Name :: binary() | list()) -> ok | {error, term()}.
uninstall(Type, Name) ->
    HandlerId = handler_id(ensure_bin(Name), Type),
    uninstall(HandlerId).

-spec uninstall(HandlerId :: atom()) -> ok | {error, term()}.
uninstall(HandlerId) ->
    Res = logger:remove_handler(HandlerId),
    show_prompts(Res, HandlerId, "Stop trace"),
    Res.

%% @doc Return all running trace handlers information.
-spec running() ->
    [
        #{
            name => binary(),
            type => topic | clientid | ip_address,
            id => atom(),
            filter => emqx_types:topic() | emqx_types:clienetid() | emqx_trace:ip_address(),
            level => logger:level(),
            dst => file:filename() | console | unknown
        }
    ].
running() ->
    lists:foldl(fun filter_traces/2, [], emqx_logger:get_log_handlers(started)).

-spec filter_clientid(logger:log_event(), {string(), atom()}) -> logger:log_event() | ignore.
filter_clientid(#{meta := #{clientid := ClientId}} = Log, {ClientId, _Name}) -> Log;
filter_clientid(_Log, _ExpectId) -> ignore.

-spec filter_topic(logger:log_event(), {string(), atom()}) -> logger:log_event() | ignore.
filter_topic(#{meta := #{topic := Topic}} = Log, {TopicFilter, _Name}) ->
    case emqx_topic:match(Topic, TopicFilter) of
        true -> Log;
        false -> ignore
    end;
filter_topic(_Log, _ExpectId) -> ignore.

-spec filter_ip_address(logger:log_event(), {string(), atom()}) -> logger:log_event() | ignore.
filter_ip_address(#{meta := #{peername := Peername}} = Log, {IP, _Name}) ->
    case lists:prefix(IP, Peername) of
        true -> Log;
        false -> ignore
    end;
filter_ip_address(_Log, _ExpectId) -> ignore.

install_handler(Who = #{name := Name, type := Type}, Level, LogFile) ->
    HandlerId = handler_id(Name, Type),
    Config = #{ level => Level,
                formatter => formatter(Who),
                filter_default => stop,
                filters => filters(Who),
                config => ?CONFIG(LogFile)
                },
    Res = logger:add_handler(HandlerId, logger_disk_log_h, Config),
    show_prompts(Res, Who, "Start trace"),
    Res.

filters(#{type := clientid, filter := Filter, name := Name}) ->
    [{clientid, {fun ?MODULE:filter_clientid/2, {ensure_list(Filter), Name}}}];
filters(#{type := topic, filter := Filter, name := Name}) ->
    [{topic, {fun ?MODULE:filter_topic/2, {ensure_bin(Filter), Name}}}];
filters(#{type := ip_address, filter := Filter, name := Name}) ->
    [{ip_address, {fun ?MODULE:filter_ip_address/2, {ensure_list(Filter), Name}}}].

formatter(#{type := Type}) ->
    {logger_formatter,
        #{
            template => template(Type),
            single_line => false,
            max_size => unlimited,
            depth => unlimited
        }
    }.

%% Don't log clientid since clientid only supports exact match, all client ids are the same.
%% if clientid is not latin characters. the logger_formatter restricts the output must be `~tp`
%% (actually should use `~ts`), the utf8 characters clientid will become very difficult to read.
template(clientid) ->
    [time, " [", level, "] ", {peername, [peername, " "], []}, msg, "\n"];
%% TODO better format when clientid is utf8.
template(_) ->
    [ time, " [", level, "] ",
        {clientid,
            [{peername, [clientid, "@", peername, " "], [clientid, " "]}],
            [{peername, [peername, " "], []}]
        },
        msg, "\n"
    ].

filter_traces(#{id := Id, level := Level, dst := Dst, filters := Filters}, Acc) ->
    Init = #{id => Id, level => Level, dst => Dst},
    case Filters of
        [{Type, {_FilterFun, {Filter, Name}}}] when
                Type =:= topic orelse
                Type =:= clientid orelse
                Type =:= ip_address ->
            [Init#{type => Type, filter => Filter, name => Name} | Acc];
        _ ->
            Acc
    end.

handler_id(Name, Type) ->
    try
        do_handler_id(Name, Type)
    catch
        _ : _ ->
            Hash = emqx_misc:bin2hexstr_a_f_lower(crypto:hash(md5, Name)),
            do_handler_id(Hash, Type)
    end.

%% Handler ID must be an atom.
do_handler_id(Name, Type) ->
    TypeStr = atom_to_list(Type),
    NameStr = unicode:characters_to_list(Name, utf8),
    FullNameStr = "trace_" ++ TypeStr ++ "_" ++  NameStr,
    true = io_lib:printable_unicode_list(FullNameStr),
    FullNameBin = unicode:characters_to_binary(FullNameStr, utf8),
    binary_to_atom(FullNameBin, utf8).

ensure_bin(List) when is_list(List) -> iolist_to_binary(List);
ensure_bin(Bin) when is_binary(Bin) -> Bin.

ensure_list(Bin) when is_binary(Bin) -> unicode:characters_to_list(Bin, utf8);
ensure_list(List) when is_list(List) -> List.

show_prompts(ok, Who, Msg) ->
    ?LOG(info, Msg ++ " ~p " ++ "successfully~n", [Who]);
show_prompts({error, Reason}, Who, Msg) ->
    ?LOG(error, Msg ++ " ~p " ++ "failed with ~p~n", [Who, Reason]).
