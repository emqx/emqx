%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_redact).

-export([redact/1, redact/2, redact_headers/1, is_redacted/2, is_redacted/3]).
-export([deobfuscate/2]).

-define(REDACT_VAL, "******").
-define(IS_KEY_HEADERS(K), (K == headers orelse K == <<"headers">> orelse K == "headers")).

%% NOTE: keep alphabetical order
is_sensitive_key(account_key) -> true;
is_sensitive_key("account_key") -> true;
is_sensitive_key(<<"account_key">>) -> true;
is_sensitive_key(aws_secret_access_key) -> true;
is_sensitive_key("aws_secret_access_key") -> true;
is_sensitive_key(<<"aws_secret_access_key">>) -> true;
is_sensitive_key(password) -> true;
is_sensitive_key("password") -> true;
is_sensitive_key(<<"password">>) -> true;
is_sensitive_key(private_key) -> true;
is_sensitive_key("private_key") -> true;
is_sensitive_key(<<"private_key">>) -> true;
is_sensitive_key(secret) -> true;
is_sensitive_key("secret") -> true;
is_sensitive_key(<<"secret">>) -> true;
is_sensitive_key(secret_access_key) -> true;
is_sensitive_key("secret_access_key") -> true;
is_sensitive_key(<<"secret_access_key">>) -> true;
is_sensitive_key(secret_key) -> true;
is_sensitive_key("secret_key") -> true;
is_sensitive_key(<<"secret_key">>) -> true;
is_sensitive_key(security_token) -> true;
is_sensitive_key("security_token") -> true;
is_sensitive_key(<<"security_token">>) -> true;
is_sensitive_key(sp_private_key) -> true;
is_sensitive_key(<<"sp_private_key">>) -> true;
is_sensitive_key(token) -> true;
is_sensitive_key("token") -> true;
is_sensitive_key(<<"token">>) -> true;
is_sensitive_key(jwt) -> true;
is_sensitive_key("jwt") -> true;
is_sensitive_key(<<"jwt">>) -> true;
is_sensitive_key(bind_password) -> true;
is_sensitive_key("bind_password") -> true;
is_sensitive_key(<<"bind_password">>) -> true;
is_sensitive_key(_) -> false.

redact(Term) ->
    do_redact(Term, fun is_sensitive_key/1).

redact(Term, Checker) ->
    do_redact(Term, fun(V) ->
        is_sensitive_key(V) orelse Checker(V)
    end).

redact_headers(Term) ->
    do_redact_headers(Term).

do_redact([], _Checker) ->
    [];
do_redact([X | Xs], Checker) ->
    %% Note: we could be dealing with an improper list
    [do_redact(X, Checker) | do_redact(Xs, Checker)];
do_redact(M, Checker) when is_map(M) ->
    maps:map(
        fun(K, V) ->
            do_redact(K, V, Checker)
        end,
        M
    );
do_redact({Headers, Value}, _Checker) when ?IS_KEY_HEADERS(Headers) ->
    {Headers, do_redact_headers(Value)};
do_redact({Key, Value}, Checker) ->
    case Checker(Key) of
        true ->
            {Key, redact_v(Value)};
        false ->
            {do_redact(Key, Checker), do_redact(Value, Checker)}
    end;
do_redact(T, Checker) when is_tuple(T) ->
    Elements = erlang:tuple_to_list(T),
    Redact = do_redact(Elements, Checker),
    erlang:list_to_tuple(Redact);
do_redact(Any, _Checker) ->
    Any.

do_redact(Headers, V, _Checker) when ?IS_KEY_HEADERS(Headers) ->
    do_redact_headers(V);
do_redact(K, V, Checker) ->
    case Checker(K) of
        true ->
            redact_v(V);
        false ->
            do_redact(V, Checker)
    end.

do_redact_headers(List) when is_list(List) ->
    lists:map(
        fun
            ({K, V} = Pair) ->
                case check_is_sensitive_header(K) of
                    true ->
                        {K, redact_v(V)};
                    _ ->
                        Pair
                end;
            (Any) ->
                Any
        end,
        List
    );
do_redact_headers(Map) when is_map(Map) ->
    maps:map(
        fun(K, V) ->
            case check_is_sensitive_header(K) of
                true ->
                    redact_v(V);
                _ ->
                    V
            end
        end,
        Map
    );
do_redact_headers(Value) ->
    Value.

check_is_sensitive_header(Key) ->
    Key1 = string:trim(emqx_utils_conv:str(Key)),
    is_sensitive_header(string:lowercase(Key1)).

is_sensitive_header("authorization") ->
    true;
is_sensitive_header("proxy-authorization") ->
    true;
is_sensitive_header(_Any) ->
    false.

redact_v(V) when is_binary(V) ->
    case emqx_placeholder:preproc_tmpl(V) of
        [{var, _}] ->
            V;
        _ ->
            do_redact_v(V)
    end;
redact_v([{str, Bin}]) when is_binary(Bin) ->
    %% The HOCON schema system may generate sensitive values with this format
    [{str, do_redact_v(Bin)}];
redact_v(V) ->
    do_redact_v(V).

do_redact_v(<<"file://", _/binary>> = V) ->
    V;
do_redact_v("file://" ++ _ = V) ->
    V;
do_redact_v(B) when is_binary(B) ->
    <<?REDACT_VAL>>;
do_redact_v(L) when is_list(L) ->
    ?REDACT_VAL;
do_redact_v(F) ->
    try
        %% this can happen in logs
        case emqx_secret:term(F) of
            {file, File} ->
                File;
            V ->
                do_redact_v(V)
        end
    catch
        _:_ ->
            %% most of the time
            ?REDACT_VAL
    end.

deobfuscate(NewConf, OldConf) ->
    deobfuscate(NewConf, OldConf, fun(_) -> false end).

deobfuscate(NewConf, OldConf, IsSensitiveFun) ->
    maps:fold(
        fun(K, V, Acc) ->
            case maps:find(K, OldConf) of
                error ->
                    case is_redacted(K, V, IsSensitiveFun) of
                        %% don't put redacted value into new config
                        true -> Acc;
                        false -> Acc#{K => V}
                    end;
                {ok, OldV} when is_map(V), is_map(OldV), ?IS_KEY_HEADERS(K) ->
                    Acc#{K => deobfuscate(V, OldV, fun check_is_sensitive_header/1)};
                {ok, OldV} when is_map(V), is_map(OldV) ->
                    Acc#{K => deobfuscate(V, OldV, IsSensitiveFun)};
                {ok, OldV} ->
                    case is_redacted(K, V, IsSensitiveFun) of
                        true ->
                            Acc#{K => OldV};
                        _ ->
                            Acc#{K => V}
                    end
            end
        end,
        #{},
        NewConf
    ).

is_redacted(K, V) ->
    do_is_redacted(K, V, fun is_sensitive_key/1).

is_redacted(K, V, Fun) ->
    do_is_redacted(K, V, fun(E) ->
        is_sensitive_key(E) orelse Fun(E)
    end).

do_is_redacted(K, ?REDACT_VAL, Fun) ->
    Fun(K);
do_is_redacted(K, <<?REDACT_VAL>>, Fun) ->
    Fun(K);
do_is_redacted(K, WrappedFun, Fun) when is_function(WrappedFun, 0) ->
    %% wrapped by `emqx_secret' or other module
    do_is_redacted(K, WrappedFun(), Fun);
do_is_redacted(_K, _V, _Fun) ->
    false.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

redact_test_() ->
    Case = fun(Type, KeyT) ->
        Key =
            case Type of
                atom -> KeyT;
                string -> erlang:atom_to_list(KeyT);
                binary -> erlang:atom_to_binary(KeyT)
            end,

        ?assert(is_sensitive_key(Key)),

        %% direct
        ?assertEqual({Key, ?REDACT_VAL}, redact({Key, foo})),
        ?assertEqual(#{Key => ?REDACT_VAL}, redact(#{Key => foo})),
        ?assertEqual({Key, Key, Key}, redact({Key, Key, Key})),
        ?assertEqual({[{Key, ?REDACT_VAL}], bar}, redact({[{Key, foo}], bar})),

        %% 1 level nested
        ?assertEqual([{Key, ?REDACT_VAL}], redact([{Key, foo}])),
        ?assertEqual([#{Key => ?REDACT_VAL}], redact([#{Key => foo}])),

        %% 2 level nested
        ?assertEqual(#{opts => [{Key, ?REDACT_VAL}]}, redact(#{opts => [{Key, foo}]})),
        ?assertEqual(#{opts => #{Key => ?REDACT_VAL}}, redact(#{opts => #{Key => foo}})),
        ?assertEqual({opts, [{Key, ?REDACT_VAL}]}, redact({opts, [{Key, foo}]})),

        %% 3 level nested
        ?assertEqual([#{opts => [{Key, ?REDACT_VAL}]}], redact([#{opts => [{Key, foo}]}])),
        ?assertEqual([{opts, [{Key, ?REDACT_VAL}]}], redact([{opts, [{Key, foo}]}])),
        ?assertEqual([{opts, [#{Key => ?REDACT_VAL}]}], redact([{opts, [#{Key => foo}]}]))
    end,

    Types = [atom, string, binary],
    Keys = [
        account_key,
        aws_secret_access_key,
        password,
        private_key,
        secret,
        secret_key,
        secret_access_key,
        security_token,
        token,
        bind_password
    ],
    [{case_name(Type, Key), fun() -> Case(Type, Key) end} || Key <- Keys, Type <- Types].

redact2_test_() ->
    Case = fun(Key, Checker) ->
        ?assertEqual({Key, ?REDACT_VAL}, redact({Key, foo}, Checker)),
        ?assertEqual(#{Key => ?REDACT_VAL}, redact(#{Key => foo}, Checker)),
        ?assertEqual({Key, Key, Key}, redact({Key, Key, Key}, Checker)),
        ?assertEqual({[{Key, ?REDACT_VAL}], bar}, redact({[{Key, foo}], bar}, Checker))
    end,

    Checker = fun(E) -> E =:= passcode end,

    Keys = [secret, passcode],
    [{case_name(atom, Key), fun() -> Case(Key, Checker) end} || Key <- Keys].

redact_improper_list_test_() ->
    %% improper lists: check that we don't crash
    %% may arise when we redact process states with pending `gen' requests
    [
        ?_assertEqual([alias | foo], redact([alias | foo])),
        ?_assertEqual([1, 2 | foo], redact([1, 2 | foo]))
    ].

deobfuscate_test() ->
    NewConf0 = #{foo => <<"bar0">>, password => <<"123456">>},
    ?assertEqual(NewConf0, deobfuscate(NewConf0, #{foo => <<"bar">>, password => <<"654321">>})),

    NewConf1 = #{foo => <<"bar1">>, password => <<?REDACT_VAL>>},
    ?assertEqual(
        #{foo => <<"bar1">>, password => <<"654321">>},
        deobfuscate(NewConf1, #{foo => <<"bar">>, password => <<"654321">>})
    ),

    %% Don't have password before and ignore to put redact_val into new config
    NewConf2 = #{foo => <<"bar2">>, password => ?REDACT_VAL},
    ?assertEqual(#{foo => <<"bar2">>}, deobfuscate(NewConf2, #{foo => <<"bar">>})),

    %% Don't have password before and should allow put non-redact-val into new config
    NewConf3 = #{foo => <<"bar3">>, password => <<"123456">>},
    ?assertEqual(NewConf3, deobfuscate(NewConf3, #{foo => <<"bar">>})),

    HeaderConf1 = #{<<"headers">> => #{<<"Authorization">> => <<"Bearer token">>}},
    HeaderConf1Obs = #{<<"headers">> => #{<<"Authorization">> => ?REDACT_VAL}},
    ?assertEqual(HeaderConf1, deobfuscate(HeaderConf1Obs, HeaderConf1)),

    ok.

redact_header_test_() ->
    Types = [string, binary, atom],
    Keys = [
        "auThorization",
        "Authorization",
        "authorizaTion",
        "proxy-authorizaTion",
        "proXy-authoriZaTion"
    ],

    Case = fun(Type, Key0) ->
        Converter =
            case Type of
                binary ->
                    fun erlang:list_to_binary/1;
                atom ->
                    fun erlang:list_to_atom/1;
                _ ->
                    fun(Any) -> Any end
            end,

        Name = Converter("headers"),
        Key = Converter(Key0),
        Value = Converter("value"),
        Value1 = redact_v(Value),
        ?assertMatch(
            {Name, [{Key, Value1}]},
            redact({Name, [{Key, Value}]})
        ),

        ?assertMatch(
            #{Name := #{Key := Value1}},
            redact(#{Name => #{Key => Value}})
        )
    end,

    [{case_name(Type, Key), fun() -> Case(Type, Key) end} || Key <- Keys, Type <- Types].

case_name(Type, Key) ->
    lists:concat([Type, "-", Key]).

-endif.
