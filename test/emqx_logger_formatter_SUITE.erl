%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_logger_formatter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

t_chars_limit(_Config) ->
    CharsLimit = 50,
    LogFilename = "./t_chars_limit.log",
    Formatter = {emqx_logger_formatter,
                  #{chars_limit => CharsLimit}},
    #{level := OrgLevel} = logger:get_primary_config(),
    Config =
        #{level => info,
          config => #{
              type => halt,
              file => LogFilename},
          formatter => Formatter},
    logger:add_handler(t_handler, logger_disk_log_h, Config),
    logger:set_primary_config(level, info),

    logger:info("hello"),
    logger:info(lists:duplicate(10, "hello")),
    logger_disk_log_h:filesync(t_handler),

    ct:pal("content : ~p", [file:read_file(LogFilename)]),
    [FirstLine, SecondLine] = readlines(LogFilename),

    ?assertMatch([_Date, _Time, _Level, "hello\n"], string:split(FirstLine, " ", all)),
    ?assert(length(SecondLine) =< 50),

    logger:set_primary_config(level, OrgLevel).


readlines(FileName) ->
    {ok, Device} = file:open(FileName, [read]),
    try get_all_lines(Device)
    after file:close(Device)
    end.

get_all_lines(Device) ->
    get_all_lines(Device, []).
get_all_lines(Device, All) ->
    case io:get_line(Device, "") of
        eof  ->
            lists:reverse(All);
        Line -> get_all_lines(Device, [Line | All])
    end.