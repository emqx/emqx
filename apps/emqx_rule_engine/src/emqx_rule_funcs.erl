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

-module(emqx_rule_funcs).

%% IoT Funcs
-export([ msgid/0
        , qos/0
        , flags/0
        , flag/1
        , topic/0
        , topic/1
        , clientid/0
        , clientip/0
        , peerhost/0
        , username/0
        , payload/0
        , payload/1
        , contains_topic/2
        , contains_topic/3
        , contains_topic_match/2
        , contains_topic_match/3
        ]).

%% Arithmetic Funcs
-export([ '+'/2
        , '-'/2
        , '*'/2
        , '/'/2
        , 'div'/2
        , mod/2
        , eq/2
        ]).

%% Math Funcs
-export([ abs/1
        , acos/1
        , acosh/1
        , asin/1
        , asinh/1
        , atan/1
        , atanh/1
        , ceil/1
        , cos/1
        , cosh/1
        , exp/1
        , floor/1
        , fmod/2
        , log/1
        , log10/1
        , log2/1
        , power/2
        , round/1
        , sin/1
        , sinh/1
        , sqrt/1
        , tan/1
        , tanh/1
        ]).

%% Bits Funcs
-export([ bitnot/1
        , bitand/2
        , bitor/2
        , bitxor/2
        , bitsl/2
        , bitsr/2
        ]).

%% Data Type Convertion
-export([ str/1
        , str_utf8/1
        , bool/1
        , int/1
        , float/1
        , map/1
        ]).

%% Data Type Validation Funcs
-export([ is_null/1
        , is_not_null/1
        , is_str/1
        , is_bool/1
        , is_int/1
        , is_float/1
        , is_num/1
        , is_map/1
        , is_array/1
        ]).

%% String Funcs
-export([ lower/1
        , ltrim/1
        , reverse/1
        , rtrim/1
        , strlen/1
        , substr/2
        , substr/3
        , trim/1
        , upper/1
        , split/2
        , split/3
        , concat/2
        , tokens/2
        , tokens/3
        , sprintf_s/2
        , pad/2
        , pad/3
        , pad/4
        , replace/3
        , replace/4
        , regex_match/2
        , regex_replace/3
        , ascii/1
        , find/2
        , find/3
        ]).

%% Map Funcs
-export([ map_new/0
        ]).

-export([ map_get/2
        , map_get/3
        , map_put/3
        ]).

%% For backword compatibility
-export([ mget/2
        , mget/3
        , mput/3
        ]).

%% Array Funcs
-export([ nth/2
        , length/1
        , sublist/2
        , sublist/3
        , first/1
        , last/1
        , contains/2
        ]).

%% Hash Funcs
-export([ md5/1
        , sha/1
        , sha256/1
        ]).

%% Data encode and decode
-export([ base64_encode/1
        , base64_decode/1
        , json_decode/1
        , json_encode/1
        ]).

%% Date functions
-export([ now_rfc3339/0
        , now_rfc3339/1
        , now_timestamp/0
        , now_timestamp/1
        ]).

-export(['$handle_undefined_function'/2]).

-compile({no_auto_import,
          [ abs/1
          , ceil/1
          , floor/1
          , round/1
          , map_get/2
          ]}).

-define(is_var(X), is_binary(X)).

%% @doc "msgid()" Func
msgid() ->
    fun(#{id := MsgId}) -> MsgId; (_) -> undefined end.

%% @doc "qos()" Func
qos() ->
    fun(#{qos := QoS}) -> QoS; (_) -> undefined end.

%% @doc "topic()" Func
topic() ->
    fun(#{topic := Topic}) -> Topic; (_) -> undefined end.

%% @doc "topic(N)" Func
topic(I) when is_integer(I) ->
    fun(#{topic := Topic}) ->
            lists:nth(I, emqx_topic:tokens(Topic));
       (_) -> undefined
    end.

%% @doc "flags()" Func
flags() ->
    fun(#{flags := Flags}) -> Flags; (_) -> #{} end.

%% @doc "flags(Name)" Func
flag(Name) ->
    fun(#{flags := Flags}) -> emqx_rule_maps:nested_get({var,Name}, Flags); (_) -> undefined end.

%% @doc "clientid()" Func
clientid() ->
    fun(#{from := ClientId}) -> ClientId; (_) -> undefined end.

%% @doc "username()" Func
username() ->
    fun(#{username := Username}) -> Username; (_) -> undefined end.

%% @doc "clientip()" Func
clientip() ->
    peerhost().

peerhost() ->
    fun(#{peerhost := Addr}) -> Addr; (_) -> undefined end.

payload() ->
    fun(#{payload := Payload}) -> Payload; (_) -> undefined end.

payload(Path) ->
    fun(#{payload := Payload}) when erlang:is_map(Payload) ->
            emqx_rule_maps:nested_get(map_path(Path), Payload);
       (_) -> undefined
    end.

%% @doc Check if a topic_filter contains a specific topic
%% TopicFilters = [{<<"t/a">>, #{qos => 0}].
-spec(contains_topic(emqx_mqtt_types:topic_filters(), emqx_types:topic())
        -> true | false).
contains_topic(TopicFilters, Topic) ->
    case find_topic_filter(Topic, TopicFilters, fun eq/2) of
        not_found -> false;
        _ -> true
    end.
contains_topic(TopicFilters, Topic, QoS) ->
    case find_topic_filter(Topic, TopicFilters, fun eq/2) of
        {_, #{qos := QoS}} -> true;
        _ -> false
    end.

-spec(contains_topic_match(emqx_mqtt_types:topic_filters(), emqx_types:topic())
        -> true | false).
contains_topic_match(TopicFilters, Topic) ->
    case find_topic_filter(Topic, TopicFilters, fun emqx_topic:match/2) of
        not_found -> false;
        _ -> true
    end.
contains_topic_match(TopicFilters, Topic, QoS) ->
    case find_topic_filter(Topic, TopicFilters, fun emqx_topic:match/2) of
        {_, #{qos := QoS}} -> true;
        _ -> false
    end.

find_topic_filter(Filter, TopicFilters, Func) ->
    try
        [case Func(Topic, Filter) of
            true -> throw(Result);
            false -> ok
         end || Result = #{topic := Topic} <- TopicFilters],
        not_found
    catch
        throw:Result -> Result
    end.

%%------------------------------------------------------------------------------
%% Arithmetic Funcs
%%------------------------------------------------------------------------------

%% plus 2 numbers
'+'(X, Y) when is_number(X), is_number(Y) ->
    X + Y;

%% concat 2 strings
'+'(X, Y) when is_binary(X), is_binary(Y) ->
    concat(X, Y).

'-'(X, Y) when is_number(X), is_number(Y) ->
    X - Y.

'*'(X, Y) when is_number(X), is_number(Y) ->
    X * Y.

'/'(X, Y) when is_number(X), is_number(Y) ->
    X / Y.

'div'(X, Y) when is_integer(X), is_integer(Y) ->
    X div Y.

mod(X, Y) when is_integer(X), is_integer(Y) ->
    X rem Y.

eq(X, Y) ->
    X == Y.

%%------------------------------------------------------------------------------
%% Math Funcs
%%------------------------------------------------------------------------------

abs(N) when is_integer(N) ->
    erlang:abs(N).

acos(N) when is_number(N) ->
    math:acos(N).

acosh(N) when is_number(N) ->
    math:acosh(N).

asin(N) when is_number(N)->
    math:asin(N).

asinh(N) when is_number(N) ->
    math:asinh(N).

atan(N) when is_number(N) ->
    math:atan(N).

atanh(N) when is_number(N)->
    math:atanh(N).

ceil(N) when is_number(N) ->
    erlang:ceil(N).

cos(N) when is_number(N)->
    math:cos(N).

cosh(N) when is_number(N) ->
    math:cosh(N).

exp(N) when is_number(N)->
    math:exp(N).

floor(N) when is_number(N) ->
    erlang:floor(N).

fmod(X, Y) when is_number(X), is_number(Y) ->
    math:fmod(X, Y).

log(N) when is_number(N) ->
    math:log(N).

log10(N) when is_number(N) ->
    math:log10(N).

log2(N) when is_number(N)->
    math:log2(N).

power(X, Y) when is_number(X), is_number(Y) ->
    math:pow(X, Y).

round(N) when is_number(N) ->
    erlang:round(N).

sin(N) when is_number(N) ->
    math:sin(N).

sinh(N) when is_number(N) ->
    math:sinh(N).

sqrt(N) when is_number(N) ->
    math:sqrt(N).

tan(N) when is_number(N) ->
    math:tan(N).

tanh(N) when is_number(N) ->
    math:tanh(N).

%%------------------------------------------------------------------------------
%% Bits Funcs
%%------------------------------------------------------------------------------

bitnot(I) when is_integer(I) ->
    bnot I.

bitand(X, Y) when is_integer(X), is_integer(Y) ->
    X band Y.

bitor(X, Y) when is_integer(X), is_integer(Y) ->
    X bor Y.

bitxor(X, Y) when is_integer(X), is_integer(Y) ->
    X bxor Y.

bitsl(X, I) when is_integer(X), is_integer(I) ->
    X bsl I.

bitsr(X, I) when is_integer(X), is_integer(I) ->
    X bsr I.

%%------------------------------------------------------------------------------
%% Data Type Convertion Funcs
%%------------------------------------------------------------------------------

str(Data) ->
    emqx_rule_utils:bin(Data).

str_utf8(Data) ->
    emqx_rule_utils:utf8_bin(Data).

bool(Data) ->
    emqx_rule_utils:bool(Data).

int(Data) ->
    emqx_rule_utils:int(Data).

float(Data) ->
    emqx_rule_utils:float(Data).

map(Data) ->
    emqx_rule_utils:map(Data).

%%------------------------------------------------------------------------------
%% NULL Funcs
%%------------------------------------------------------------------------------

is_null(undefined) -> true;
is_null(_Data) -> false.

is_not_null(Data) ->
    not is_null(Data).

is_str(T) when is_binary(T) -> true;
is_str(_) -> false.

is_bool(T) when is_boolean(T) -> true;
is_bool(_) -> false.

is_int(T) when is_integer(T) -> true;
is_int(_) -> false.

is_float(T) when erlang:is_float(T) -> true;
is_float(_) -> false.

is_num(T) when is_number(T) -> true;
is_num(_) -> false.

is_map(T) when erlang:is_map(T) -> true;
is_map(_) -> false.

is_array(T) when is_list(T) -> true;
is_array(_) -> false.

%%------------------------------------------------------------------------------
%% String Funcs
%%------------------------------------------------------------------------------

lower(S) when is_binary(S) ->
    string:lowercase(S).

ltrim(S) when is_binary(S) ->
    string:trim(S, leading).

reverse(S) when is_binary(S) ->
    iolist_to_binary(string:reverse(S)).

rtrim(S) when is_binary(S) ->
    string:trim(S, trailing).

strlen(S) when is_binary(S) ->
    string:length(S).

substr(S, Start) when is_binary(S), is_integer(Start) ->
    string:slice(S, Start).

substr(S, Start, Length) when is_binary(S),
                              is_integer(Start),
                              is_integer(Length) ->
    string:slice(S, Start, Length).

trim(S) when is_binary(S) ->
    string:trim(S).

upper(S) when is_binary(S) ->
    string:uppercase(S).

split(S, P) when is_binary(S),is_binary(P) ->
    [R || R <- string:split(S, P, all), R =/= <<>> andalso R =/= ""].

split(S, P, <<"notrim">>) ->
    string:split(S, P, all);

split(S, P, <<"leading_notrim">>) ->
    string:split(S, P, leading);
split(S, P, <<"leading">>) when is_binary(S),is_binary(P) ->
    [R || R <- string:split(S, P, leading), R =/= <<>> andalso R =/= ""];
split(S, P, <<"trailing_notrim">>) ->
    string:split(S, P, trailing);
split(S, P, <<"trailing">>) when is_binary(S),is_binary(P) ->
    [R || R <- string:split(S, P, trailing), R =/= <<>> andalso R =/= ""].

tokens(S, Separators) ->
    [list_to_binary(R) || R <- string:lexemes(binary_to_list(S), binary_to_list(Separators))].

tokens(S, Separators, <<"nocrlf">>) ->
    [list_to_binary(R) || R <- string:lexemes(binary_to_list(S), binary_to_list(Separators) ++ [$\r,$\n,[$\r,$\n]])].

concat(S1, S2) when is_binary(S1), is_binary(S2) ->
    unicode:characters_to_binary([S1, S2], unicode).

sprintf_s(Format, Args) when is_list(Args) ->
    erlang:iolist_to_binary(io_lib:format(binary_to_list(Format), Args)).

pad(S, Len) when is_binary(S), is_integer(Len) ->
   iolist_to_binary(string:pad(S, Len, trailing)).

pad(S, Len, <<"trailing">>) when is_binary(S), is_integer(Len) ->
   iolist_to_binary(string:pad(S, Len, trailing));

pad(S, Len, <<"both">>) when is_binary(S), is_integer(Len) ->
   iolist_to_binary(string:pad(S, Len, both));

pad(S, Len, <<"leading">>) when is_binary(S), is_integer(Len) ->
   iolist_to_binary(string:pad(S, Len, leading)).

pad(S, Len, <<"trailing">>, Char) when is_binary(S), is_integer(Len), is_binary(Char) ->
   iolist_to_binary(string:pad(S, Len, trailing, Char));

pad(S, Len, <<"both">>, Char) when is_binary(S), is_integer(Len), is_binary(Char) ->
   iolist_to_binary(string:pad(S, Len, both, Char));

pad(S, Len, <<"leading">>, Char) when is_binary(S), is_integer(Len), is_binary(Char) ->
   iolist_to_binary(string:pad(S, Len, leading, Char)).

replace(SrcStr, P, RepStr) when is_binary(SrcStr), is_binary(P), is_binary(RepStr) ->
    iolist_to_binary(string:replace(SrcStr, P, RepStr, all)).

replace(SrcStr, P, RepStr, <<"all">>) when is_binary(SrcStr), is_binary(P), is_binary(RepStr) ->
    iolist_to_binary(string:replace(SrcStr, P, RepStr, all));

replace(SrcStr, P, RepStr, <<"trailing">>) when is_binary(SrcStr), is_binary(P), is_binary(RepStr) ->
    iolist_to_binary(string:replace(SrcStr, P, RepStr, trailing));

replace(SrcStr, P, RepStr, <<"leading">>) when is_binary(SrcStr), is_binary(P), is_binary(RepStr) ->
    iolist_to_binary(string:replace(SrcStr, P, RepStr, leading)).

regex_match(Str, RE) ->
    case re:run(Str, RE, [global,{capture,none}]) of
        match -> true;
        nomatch -> false
    end.

regex_replace(SrcStr, RE, RepStr) ->
    re:replace(SrcStr, RE, RepStr, [global, {return,binary}]).

ascii(Char) when is_binary(Char) ->
    [FirstC| _] = binary_to_list(Char),
    FirstC.

find(S, P) when is_binary(S), is_binary(P) ->
    find_s(S, P, leading).

find(S, P, <<"trailing">>) when is_binary(S), is_binary(P) ->
    find_s(S, P, trailing);

find(S, P, <<"leading">>) when is_binary(S), is_binary(P) ->
    find_s(S, P, leading).

find_s(S, P, Dir) ->
    case string:find(S, P, Dir) of
        nomatch -> <<"">>;
        SubStr -> SubStr
    end.

%%------------------------------------------------------------------------------
%% Array Funcs
%%------------------------------------------------------------------------------

nth(N, L) when is_integer(N), is_list(L) ->
    lists:nth(N, L).

length(List) when is_list(List) ->
    erlang:length(List).

sublist(Len, List) when is_integer(Len), is_list(List) ->
    lists:sublist(List, Len).

sublist(Start, Len, List) when is_integer(Start), is_integer(Len), is_list(List) ->
    lists:sublist(List, Start, Len).

first(List) when is_list(List) ->
    hd(List).

last(List) when is_list(List) ->
    lists:last(List).

contains(Elm, List) when is_list(List) ->
    lists:member(Elm, List).

map_new() ->
    #{}.

map_get(Key, Map) ->
    map_get(Key, Map, undefined).

map_get(Key, Map, Default) ->
    case maps:find(Key, Map) of
        {ok, Val} -> Val;
        error when is_atom(Key) ->
            %% the map may have an equivalent binary-form key
            BinKey = emqx_rule_utils:bin(Key),
            case maps:find(BinKey, Map) of
                {ok, Val} -> Val;
                error -> Default
            end;
        error when is_binary(Key) ->
            try %% the map may have an equivalent atom-form key
                AtomKey = list_to_existing_atom(binary_to_list(Key)),
                case maps:find(AtomKey, Map) of
                    {ok, Val} -> Val;
                    error -> Default
                end
            catch error:badarg ->
                Default
            end;
        error ->
            Default
    end.

map_put(Key, Val, Map) ->
    case maps:find(Key, Map) of
        {ok, _} -> maps:put(Key, Val, Map);
        error when is_atom(Key) ->
            %% the map may have an equivalent binary-form key
            BinKey = emqx_rule_utils:bin(Key),
            case maps:find(BinKey, Map) of
                {ok, _} -> maps:put(BinKey, Val, Map);
                error -> maps:put(Key, Val, Map)
            end;
        error when is_binary(Key) ->
            try %% the map may have an equivalent atom-form key
                AtomKey = list_to_existing_atom(binary_to_list(Key)),
                case maps:find(AtomKey, Map) of
                    {ok, _} -> maps:put(AtomKey, Val, Map);
                    error -> maps:put(Key, Val, Map)
                end
            catch error:badarg ->
                maps:put(Key, Val, Map)
            end;
        error ->
            maps:put(Key, Val, Map)
    end.

mget(Key, Map) ->
    mget(Key, Map, undefined).

mget(Key, Map, Default) ->
    case maps:find(Key, Map) of
        {ok, Val} -> Val;
        error when is_atom(Key) ->
            %% the map may have an equivalent binary-form key
            BinKey = emqx_rule_utils:bin(Key),
            case maps:find(BinKey, Map) of
                {ok, Val} -> Val;
                error -> Default
            end;
        error when is_binary(Key) ->
            try %% the map may have an equivalent atom-form key
                AtomKey = list_to_existing_atom(binary_to_list(Key)),
                case maps:find(AtomKey, Map) of
                    {ok, Val} -> Val;
                    error -> Default
                end
            catch error:badarg ->
                Default
            end;
        error ->
            Default
    end.

mput(Key, Val, Map) ->
    case maps:find(Key, Map) of
        {ok, _} -> maps:put(Key, Val, Map);
        error when is_atom(Key) ->
            %% the map may have an equivalent binary-form key
            BinKey = emqx_rule_utils:bin(Key),
            case maps:find(BinKey, Map) of
                {ok, _} -> maps:put(BinKey, Val, Map);
                error -> maps:put(Key, Val, Map)
            end;
        error when is_binary(Key) ->
            try %% the map may have an equivalent atom-form key
                AtomKey = list_to_existing_atom(binary_to_list(Key)),
                case maps:find(AtomKey, Map) of
                    {ok, _} -> maps:put(AtomKey, Val, Map);
                    error -> maps:put(Key, Val, Map)
                end
            catch error:badarg ->
                maps:put(Key, Val, Map)
            end;
        error ->
            maps:put(Key, Val, Map)
    end.

%%------------------------------------------------------------------------------
%% Hash Funcs
%%------------------------------------------------------------------------------

md5(S) when is_binary(S) ->
    hash(md5, S).

sha(S) when is_binary(S) ->
    hash(sha, S).

sha256(S) when is_binary(S) ->
    hash(sha256, S).

hash(Type, Data) ->
    hexstring(crypto:hash(Type, Data)).

hexstring(<<X:128/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~32.16.0b", [X]));
hexstring(<<X:160/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~40.16.0b", [X]));
hexstring(<<X:256/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~64.16.0b", [X])).

%%------------------------------------------------------------------------------
%% Base64 Funcs
%%------------------------------------------------------------------------------

base64_encode(Data) when is_binary(Data) ->
    base64:encode(Data).

base64_decode(Data) when is_binary(Data) ->
    base64:decode(Data).

json_encode(Data) ->
    emqx_json:encode(Data).

json_decode(Data) ->
    emqx_json:decode(Data, [return_maps]).

%%--------------------------------------------------------------------
%% Date functions
%%--------------------------------------------------------------------

now_rfc3339() ->
    now_rfc3339(<<"second">>).

now_rfc3339(Unit) ->
    emqx_rule_utils:bin(
        calendar:system_time_to_rfc3339(
            now_timestamp(Unit), [{unit, time_unit(Unit)}])).

now_timestamp() ->
    erlang:system_time(second).

now_timestamp(Unit) ->
    erlang:system_time(time_unit(Unit)).

time_unit(<<"second">>) -> second;
time_unit(<<"millisecond">>) -> millisecond;
time_unit(<<"microsecond">>) -> microsecond;
time_unit(<<"nanosecond">>) -> nanosecond.

%% @doc This is for sql funcs that should be handled in the specific modules.
%% Here the emqx_rule_funcs module acts as a proxy, forwarding
%% the function handling to the worker module.
%% @end
'$handle_undefined_function'(schema_decode, [SchemaId, Data|MoreArgs]) ->
    emqx_schema_parser:decode(SchemaId, Data, MoreArgs);
'$handle_undefined_function'(schema_decode, Args) ->
    error({args_count_error, {schema_decode, Args}});

'$handle_undefined_function'(schema_encode, [SchemaId, Term|MoreArgs]) ->
    emqx_schema_parser:encode(SchemaId, Term, MoreArgs);
'$handle_undefined_function'(schema_encode, Args) ->
    error({args_count_error, {schema_encode, Args}});

'$handle_undefined_function'(sprintf, [Format|Args]) ->
    erlang:apply(fun sprintf_s/2, [Format, Args]);

'$handle_undefined_function'(Fun, Args) ->
    error({sql_function_not_supported, function_literal(Fun, Args)}).

map_path(Key) ->
    {path, [{key, P} || P <- string:split(Key, ".", all)]}.

function_literal(Fun, []) when is_atom(Fun) ->
    atom_to_list(Fun) ++ "()";
function_literal(Fun, [FArg | Args]) when is_atom(Fun), is_list(Args) ->
    WithFirstArg = io_lib:format("~s(~0p", [atom_to_list(Fun), FArg]),
    lists:foldl(fun(Arg, Literal) ->
        io_lib:format("~s, ~0p", [Literal, Arg])
    end, WithFirstArg, Args) ++ ")";
function_literal(Fun, Args) ->
    {invalid_func, {Fun, Args}}.
