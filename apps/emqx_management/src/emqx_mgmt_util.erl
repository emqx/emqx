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

-module(emqx_mgmt_util).

-export([ strftime/1
        , datetime/1
        , kmg/1
        , ntoa/1
        , merge_maps/2
        ]).

-export([urldecode/1]).

-define(KB, 1024).
-define(MB, (1024*1024)).
-define(GB, (1024*1024*1024)).

%%--------------------------------------------------------------------
%% Strftime
%%--------------------------------------------------------------------

strftime({MegaSecs, Secs, _MicroSecs}) ->
    strftime(datetime(MegaSecs * 1000000 + Secs));

strftime(Secs) when is_integer(Secs) ->
    strftime(datetime(Secs));

strftime({{Y,M,D}, {H,MM,S}}) ->
    lists:flatten(
        io_lib:format(
            "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w", [Y, M, D, H, MM, S])).

datetime(Timestamp) when is_integer(Timestamp) ->
    Epoch = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
    Universal = calendar:gregorian_seconds_to_datetime(Timestamp + Epoch),
    calendar:universal_time_to_local_time(Universal).

kmg(Byte) when Byte > ?GB ->
    kmg(Byte / ?GB, "G");
kmg(Byte) when Byte > ?MB ->
    kmg(Byte / ?MB, "M");
kmg(Byte) when Byte > ?KB ->
    kmg(Byte / ?KB, "K");
kmg(Byte) ->
    Byte.
kmg(F, S) ->
    iolist_to_binary(io_lib:format("~.2f~s", [F, S])).

ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
    inet_parse:ntoa(IP).

merge_maps(Default, New) ->
    maps:fold(fun(K, V, Acc) ->
        case maps:get(K, Acc, undefined) of
            OldV when is_map(OldV),
                      is_map(V) -> Acc#{K => merge_maps(OldV, V)};
            _ -> Acc#{K => V}
        end
    end, Default, New).

urldecode(S) ->
    emqx_http_lib:uri_decode(S).

