%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_log).

-export([format/3]).

format(Format0, Args, #{
    depth := Depth,
    single_line := SingleLine,
    chars_limit := Limit
}) ->
    Opts = chars_limit_to_opts(Limit),
    Format1 = io_lib:scan_format(Format0, Args),
    Format = reformat(Format1, Depth, SingleLine),
    Text0 = io_lib:build_text(Format, Opts),
    Text =
        case SingleLine of
            true -> re:replace(Text0, ",?\r?\n\s*", ", ", [{return, list}, global, unicode]);
            false -> Text0
        end,
    trim(unicode:characters_to_binary(Text, utf8)).

chars_limit_to_opts(unlimited) -> [];
chars_limit_to_opts(Limit) -> [{chars_limit, Limit}].

%% Get rid of the leading spaces.
%% leave alone the trailing spaces.
trim(<<$\s, Rest/binary>>) -> trim(Rest);
trim(Bin) -> Bin.

reformat(Format, unlimited, false) ->
    Format;
reformat([#{control_char := C} = M | T], Depth, true) when C =:= $p ->
    [limit_depth(M#{width => 0}, Depth) | reformat(T, Depth, true)];
reformat([#{control_char := C} = M | T], Depth, true) when C =:= $P ->
    [M#{width => 0} | reformat(T, Depth, true)];
reformat([#{control_char := C} = M | T], Depth, Single) when C =:= $p; C =:= $w ->
    [limit_depth(M, Depth) | reformat(T, Depth, Single)];
reformat([H | T], Depth, Single) ->
    [H | reformat(T, Depth, Single)];
reformat([], _, _) ->
    [].

limit_depth(M0, unlimited) ->
    M0;
limit_depth(#{control_char := C0, args := Args} = M0, Depth) ->
    %To uppercase.
    C = C0 - ($a - $A),
    M0#{control_char := C, args := Args ++ [Depth]}.
