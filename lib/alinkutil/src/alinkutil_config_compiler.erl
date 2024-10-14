%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022
%%% @doc
%%%
%%% @end
%%% Created : 25. 6月 2022 下午1:02
%%%-------------------------------------------------------------------
-module(alinkutil_config_compiler).
-author("yqfclid").

%% API
-export([compile/2, compile/3]).

%%%===================================================================
%%% API
%%%===================================================================
-spec(compile(Mod :: atom() | list() | binary(),
    D :: any()) -> {module, atom()} | {error, term()}).
compile(Mod, D) ->
    compile(Mod, get, D).


-spec(compile(Mod :: atom() | list() | binary(),
    Fun :: atom() | list() | binary(),
    D :: any()) -> {module, atom()} | {error, term()}).
compile(Mod, Fun, D) ->
    ModStr = "-module(" ++ alinkutil_type:to_list(Mod) ++ ").\n",
    FunStr = alinkutil_type:to_list(Fun),
    ExportStr = "-export([" ++ FunStr ++ "/0]).\n" ++ FunStr ++ "() -> ",
    FunctionStr = transform(D),
    TrailingChar = ".\n",
    CodeStr = ModStr ++ ExportStr ++ FunctionStr ++ TrailingChar,
    alinkutil_dynamic_compile:load_from_string(CodeStr).
%%%===================================================================
%%% Internal functions
%%%===================================================================
transform(D) when is_list(D) ->
    Contents =
        lists:foldl(
            fun(DecodedParam, Acc) ->
                Acc ++ transform(DecodedParam) ++ ","
            end, "", D),
    NContents = remove_trailing_comma(Contents),
    "[" ++ NContents ++ "]";
transform(D) when is_map(D) ->
    Contents =
        maps:fold(
            fun(K, V, Acc) ->
                Acc ++ transform(K) ++ " => " ++ transform(V) ++ ","
            end, "", D),
    NContents = remove_trailing_comma(Contents),
    "#{" ++ NContents ++ "}";
transform(D) when is_binary(D) ->
    "<<\"" ++ alinkutil_type:to_list(D) ++ "\">>";
transform(D) ->
    alinkutil_type:to_list(D).

remove_trailing_comma([]) -> [];
remove_trailing_comma(Str) ->
    case lists:last(Str) of
        $, ->
            [$, | Rest] = lists:reverse(Str),
            lists:reverse(Rest);
        _ ->
            Str
    end.