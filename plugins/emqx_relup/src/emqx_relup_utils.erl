-module(emqx_relup_utils).

-export([
    exception_to_error/3,
    make_error/2
]).

-export([
    assert_propl_get/3,
    assert_propl_get/4,
    ts_filename/1,
    str/1,
    bin/1,
    major_vsn/1,
    fork_type/1,
    is_arch_compatible/2
]).

exception_to_error(Err, Reason, ST) ->
    {error, make_error(exception, #{reason => {Err, Reason}, stacktrace => ST})}.

make_error(Type, Details) when is_map(Details) ->
    Details#{err_type => Type}.

assert_propl_get(Key, Proplist, ErrMsg) ->
    assert_propl_get(Key, Proplist, ErrMsg, #{}).

assert_propl_get(Key, Proplist, ErrMsg, Meta) ->
    case proplists:get_value(Key, Proplist) of
        undefined ->
            throw(make_error(ErrMsg, maps:merge(#{key => Key, proplist => Proplist}, Meta)));
        Value ->
            Value
    end.

ts_filename(Name) ->
    {{YY, MM, DD}, {H, M, S}} = calendar:now_to_datetime(erlang:timestamp()),
    io_lib:format("~s_~4..0w-~2..0w-~2..0w_~2..0w-~2..0w-~2..0w", [Name, YY, MM, DD, H, M, S]).

str(S) when is_atom(S) -> atom_to_list(S);
str(S) when is_binary(S) -> binary_to_list(S);
str(S) when is_integer(S) -> integer_to_list(S);
str(S) when is_list(S) -> S.

bin(S) when is_list(S) ->
    iolist_to_binary(S);
bin(A) when is_atom(A) ->
    atom_to_binary(A, utf8);
bin(B) when is_binary(B) ->
    B.

major_vsn(Vsn) ->
    case string:split(Vsn, ".") of
        [Maj, _ | _] -> str(Maj);
        _ -> throw(make_error(invalid_version, #{vsn => str(Vsn)}))
    end.

fork_type(Vsn) ->
    case string:split(Vsn, ".") of
        [_Maj, _Minor, Rem] ->
            case string:split(Rem, "-") of
                [_Patch, _EmqxPatch] -> emqx;
                _ -> upstream
            end;
        _ ->
            upstream
    end.

is_arch_compatible("aarch64-apple-darwin" ++ Vsn1, "aarch64-apple-darwin" ++ Vsn2) ->
    major_vsn(Vsn1) == major_vsn(Vsn2);
is_arch_compatible("i386-apple-darwin" ++ Vsn1, "i386-apple-darwin" ++ Vsn2) ->
    major_vsn(Vsn1) == major_vsn(Vsn2);
is_arch_compatible("x86_64-apple-darwin" ++ Vsn1, "x86_64-apple-darwin" ++ Vsn2) ->
    major_vsn(Vsn1) == major_vsn(Vsn2);
is_arch_compatible("aarch64-unknown-freebsd" ++ Vsn1, "aarch64-unknown-freebsd" ++ Vsn2) ->
    major_vsn(Vsn1) == major_vsn(Vsn2);
is_arch_compatible("x86_64-unknown-freebsd" ++ Vsn1, "x86_64-unknown-freebsd" ++ Vsn2) ->
    major_vsn(Vsn1) == major_vsn(Vsn2);
is_arch_compatible("i386-unknown-freebsd" ++ Vsn1, "i386-unknown-freebsd" ++ Vsn2) ->
    major_vsn(Vsn1) == major_vsn(Vsn2);
is_arch_compatible(Vsn1, Vsn2) ->
    Vsn1 == Vsn2.
