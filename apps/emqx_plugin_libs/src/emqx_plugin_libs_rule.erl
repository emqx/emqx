%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugin_libs_rule).
-elvis([{elvis_style, god_modules, disable}]).

%% preprocess and process template string with place holders
-export([
    proc_batch_sql/3
]).

%% type converting
-export([
    bool/1,
    int/1,
    float/1,
    float2str/2,
    map/1,
    utf8_bin/1,
    utf8_str/1,
    atom_key/1,
    unsafe_atom_key/1
]).

%% connectivity check
-export([
    http_connectivity/1,
    http_connectivity/2,
    tcp_connectivity/2,
    tcp_connectivity/3
]).

-compile({no_auto_import, [float/1]}).

-type uri_string() :: iodata().

-type tmpl_token() :: list({var, binary()} | {str, binary()}).

-spec proc_batch_sql(
    BatchReqs :: list({atom(), map()}),
    InsertPart :: binary(),
    Tokens :: tmpl_token()
) -> InsertSQL :: binary().
proc_batch_sql(BatchReqs, InsertPart, Tokens) ->
    ValuesPart = erlang:iolist_to_binary(
        lists:join($,, [
            emqx_placeholder:proc_sql_param_str(Tokens, Msg)
         || {_, Msg} <- BatchReqs
        ])
    ),
    <<InsertPart/binary, " values ", ValuesPart/binary>>.

unsafe_atom_key(Key) when is_atom(Key) ->
    Key;
unsafe_atom_key(Key) when is_binary(Key) ->
    binary_to_atom(Key, utf8);
unsafe_atom_key(Keys = [_Key | _]) ->
    [unsafe_atom_key(SubKey) || SubKey <- Keys];
unsafe_atom_key(Key) ->
    error({invalid_key, Key}).

atom_key(Key) when is_atom(Key) ->
    Key;
atom_key(Key) when is_binary(Key) ->
    try
        binary_to_existing_atom(Key, utf8)
    catch
        error:badarg -> error({invalid_key, Key})
    end;
%% nested keys
atom_key(Keys = [_Key | _]) ->
    [atom_key(SubKey) || SubKey <- Keys];
atom_key(Key) ->
    error({invalid_key, Key}).

-spec http_connectivity(uri_string()) -> ok | {error, Reason :: term()}.
http_connectivity(Url) ->
    http_connectivity(Url, 3000).

-spec http_connectivity(uri_string(), integer()) -> ok | {error, Reason :: term()}.
http_connectivity(Url, Timeout) ->
    case emqx_http_lib:uri_parse(Url) of
        {ok, #{host := Host, port := Port}} ->
            tcp_connectivity(Host, Port, Timeout);
        {error, Reason} ->
            {error, Reason}
    end.

-spec tcp_connectivity(
    Host :: inet:socket_address() | inet:hostname(),
    Port :: inet:port_number()
) ->
    ok | {error, Reason :: term()}.
tcp_connectivity(Host, Port) ->
    tcp_connectivity(Host, Port, 3000).

-spec tcp_connectivity(
    Host :: inet:socket_address() | inet:hostname(),
    Port :: inet:port_number(),
    Timeout :: integer()
) ->
    ok | {error, Reason :: term()}.
tcp_connectivity(Host, Port, Timeout) ->
    case gen_tcp:connect(Host, Port, emqx_utils:ipv6_probe([]), Timeout) of
        {ok, Sock} ->
            gen_tcp:close(Sock),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

utf8_bin(Str) when is_binary(Str); is_list(Str) ->
    unicode:characters_to_binary(Str);
utf8_bin(Str) ->
    unicode:characters_to_binary(emqx_utils_conv:bin(Str)).

utf8_str(Str) when is_binary(Str); is_list(Str) ->
    unicode:characters_to_list(Str);
utf8_str(Str) ->
    unicode:characters_to_list(emqx_utils_conv:str(Str)).

int(List) when is_list(List) ->
    try
        list_to_integer(List)
    catch
        error:badarg ->
            int(list_to_float(List))
    end;
int(Bin) when is_binary(Bin) ->
    try
        binary_to_integer(Bin)
    catch
        error:badarg ->
            int(binary_to_float(Bin))
    end;
int(Int) when is_integer(Int) -> Int;
int(Float) when is_float(Float) -> erlang:floor(Float);
int(true) ->
    1;
int(false) ->
    0;
int(Data) ->
    error({invalid_number, Data}).

float(List) when is_list(List) ->
    try
        list_to_float(List)
    catch
        error:badarg ->
            float(list_to_integer(List))
    end;
float(Bin) when is_binary(Bin) ->
    try
        binary_to_float(Bin)
    catch
        error:badarg ->
            float(binary_to_integer(Bin))
    end;
float(Num) when is_number(Num) -> erlang:float(Num);
float(Data) ->
    error({invalid_number, Data}).

float2str(Float, Precision) when is_float(Float) and is_integer(Precision) ->
    float_to_binary(Float, [{decimals, Precision}, compact]).

map(Bin) when is_binary(Bin) ->
    case emqx_utils_json:decode(Bin, [return_maps]) of
        Map = #{} -> Map;
        _ -> error({invalid_map, Bin})
    end;
map(List) when is_list(List) -> maps:from_list(List);
map(Map) when is_map(Map) -> Map;
map(Data) ->
    error({invalid_map, Data}).

bool(Bool) when
    Bool == true;
    Bool == <<"true">>;
    Bool == 1
->
    true;
bool(Bool) when
    Bool == false;
    Bool == <<"false">>;
    Bool == 0
->
    false;
bool(Bool) ->
    error({invalid_boolean, Bool}).
