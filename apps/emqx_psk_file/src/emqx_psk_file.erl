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

-module(emqx_psk_file).

-include("emqx_psk_file.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-import(proplists, [get_value/2]).

-export([load/1, unload/0]).

%% Hooks functions
-export([on_psk_lookup/2]).

-define(LF, 10).

%% Called when the plugin application start
load(Env) ->
    {ok, PskFile} = file:open(get_value(path, Env), [read, raw, binary, read_ahead]),
    preload_psks(PskFile, bin(get_value(delimiter, Env))),
    _ = file:close(PskFile),
    emqx:hook('tls_handshake.psk_lookup', fun ?MODULE:on_psk_lookup/2, []).

%% Called when the plugin application stop
unload() ->
    emqx:unhook('tls_handshake.psk_lookup', fun ?MODULE:on_psk_lookup/2).

on_psk_lookup(ClientPSKID, UserState) ->
    case ets:lookup(?PSK_FILE_TAB, ClientPSKID) of
        [#psk_entry{psk_str = PskStr}] ->
            {stop, PskStr};
        [] ->
            {ok, UserState}
    end.

preload_psks(FileHandler, Delimiter) ->
    case file:read_line(FileHandler) of
        {ok, Line} ->
            case binary:split(Line, Delimiter) of
                [Key, Rem] ->
                    ets:insert(
                      ?PSK_FILE_TAB,
                      #psk_entry{psk_id = Key, psk_str = trim_lf(Rem)}),
                    preload_psks(FileHandler, Delimiter);
                [Line] ->
                    ?LOG(warning, "[~p] - Invalid line: ~p, delimiter: ~p", [?MODULE, Line, Delimiter])
            end;
        eof ->
            ?LOG(info, "[~p] - PSK file is preloaded", [?MODULE]);
        {error, Reason} ->
            ?LOG(error, "[~p] - Read lines from PSK file: ~p", [?MODULE, Reason])
    end.

bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Bin) when is_binary(Bin) -> Bin.

%% Trim the tailing LF
trim_lf(<<>>) -> <<>>;
trim_lf(Bin) ->
    Size = byte_size(Bin),
    case binary:at(Bin, Size-1) of
        ?LF -> binary_part(Bin, 0, Size-1);
        _ -> Bin
    end.

