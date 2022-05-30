%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(hocon_schema_example).
-include_lib("hocon/include/hoconsc.hrl").

-export([gen/2]).

-define(COMMENT, "#  ").
-define(COMMENT2, "## ").
-define(INDENT, "  ").
-define(NL, io_lib:nl()).
-define(DOC, "@doc ").
-define(TYPE, "@type ").
-define(PATH, "@path ").
-define(LINK, "@link ").
-define(DEFAULT, "@default ").
-define(BIND, ": ").

gen(Schema, undefined) ->
    gen(Schema, "# HOCON Example");
gen(Schema, Title) when is_list(Title) orelse is_binary(Title) ->
    gen(Schema, #{title => Title, body => <<>>});
gen(Schema, #{title := Title, body := Body} = Opts) ->
    File = maps:get(desc_file, Opts, undefined),
    Lang = maps:get(lang, Opts, "en"),
    [Roots | Fields0] = hocon_schema_json:gen(Schema, #{desc_file => File, lang => Lang}),
    Fields = lists:foldl(fun(F = #{full_name := Name}, Acc) -> Acc#{Name => F} end, #{}, Fields0),
    #{fields := RootKeys} = Roots,
    FmtOpts = #{tid => new_link_cache(), indent => "", comment => false},
    try
        Structs = lists:map(
            fun(Root) ->
                [
                    fmt_desc(Root, ""),
                    fmt_field(Root, Fields, "", FmtOpts)
                ]
            end,
            RootKeys
        ),
        [
            ?COMMENT2,
            Title,
            ?NL,
            ?NL,
            ?COMMENT2,
            Body,
            ?NL,
            ?NL,
            Structs
        ]
    after
        delete_link_cache(FmtOpts)
    end.

fmt_field(#{type := #{kind := struct, name := SubName}, name := Name} = Field, All, Path0, Opts) ->
    case maps:find(SubName, All) of
        {ok, #{fields := SubFields}} ->
            #{indent := Indent, comment := Comment} = Opts,
            Opts1 = Opts#{indent => Indent ++ ?INDENT},
            {PathName, ValName} = resolve_name(Name),
            Path = [str(PathName) | Path0],
            SubStructs =
                case maps:get(examples, Field, #{}) of
                    #{} = Example ->
                        fmt_field_with_example(Path, SubFields, Example, All, Opts1);
                    {union, UnionExamples} ->
                        Examples1 = filter_union_example(UnionExamples, SubFields),
                        fmt_field_with_example(Path, SubFields, Examples1, All, Opts1);
                    {array, ArrayExamples} ->
                        lists:flatmap(
                            fun(SubExample) ->
                                fmt_field_with_example(Path, SubFields, SubExample, All, Opts1)
                            end,
                            ArrayExamples
                        )
                end,
            [
                Indent,
                comment(Comment),
                ValName,
                " {",
                ?NL,
                lists:join(?NL, SubStructs),
                Indent,
                comment(Comment),
                " }",
                ?NL
            ];
        Unknown ->
            throw({error, {Path0, SubName, Unknown}})
    end;
fmt_field(#{type := #{kind := primitive, name := TypeName}} = Field, _All, Path, Opts) ->
    Name = str(maps:get(name, Field)),
    Fix = fmt_fix_header(Field, TypeName, [Name | Path], Opts),
    [Fix, fmt_examples(Name, Field, Opts)];
fmt_field(#{type := #{kind := singleton, name := SingleTon}} = Field, _All, Path, Opts) ->
    Name = str(maps:get(name, Field)),
    #{indent := Indent, comment := Comment} = Opts,
    Fix = fmt_fix_header(Field, "singleton", [Name | Path], Opts),
    [Fix, fmt(Indent, Comment, Name, SingleTon)];
fmt_field(#{type := #{kind := enum, symbols := Symbols}} = Field, _All, Path, Opts) ->
    TypeName = ["enum: ", lists:join(" | ", Symbols)],
    Name = str(maps:get(name, Field)),
    Fix = fmt_fix_header(Field, TypeName, [str(Name) | Path], Opts),
    [Fix, fmt_examples(Name, Field, Opts)];
fmt_field(#{type := #{kind := union, members := Members0} = Type} = Field, All, Path0, Opts) ->
    Name = str(maps:get(name, Field)),
    Names = lists:map(fun(#{name := N}) -> N end, Members0),
    Path = [Name | Path0],
    TypeStr = ["union() ", lists:join(" | ", Names)],
    Fix = fmt_fix_header(Field, TypeStr, Path, Opts),
    Link = fmt_union_link(Type, Path, Opts),
    Fix1 = [Fix, Link],
    case Link =:= "" andalso need_comment_example(union, Opts, Type, Path) of
        true ->
            #{indent := Indent} = Opts,
            Indent1 = Indent ++ ?INDENT,
            Opts1 = Opts#{indent => Indent1},
            case fmt_sub_fields(Opts1, Field, All, Path0) of
                [] -> fallback_to_example(Field, Fix1, Indent1, Name, Opts1, Indent, "");
                ValFields -> [Fix1, ValFields, ?NL]
            end;
        false ->
            [Fix1, ?NL]
    end;
fmt_field(#{type := #{kind := map, name := MapName} = Type} = Field, All, Path0, Opts) ->
    Name = str(maps:get(name, Field)),
    #{indent := Indent} = Opts,
    Path = [Name | Path0],
    Path1 = ["$" ++ str(MapName) | Path],
    Fix = fmt_fix_header(Field, "map_struct()", Path, Opts),
    Link = fmt_map_link(Path1, Type, All, Opts),
    Fix1 = [Fix, Link],
    case Link =:= "" andalso need_comment_example(map, Opts, Path1) of
        true ->
            Indent1 = Indent ++ ?INDENT,
            Opts1 = Opts#{indent => Indent1},
            ValFields = fmt_sub_fields(Opts1, Field, All, Path),
            [Fix1, Indent1, ?COMMENT, Name, ?BIND, ?NL, ValFields, ?NL];
        false ->
            [Fix1, ?NL]
    end;
fmt_field(#{type := #{kind := array} = Type} = Field, All, Path0, Opts) ->
    #{indent := Indent, comment := Comment} = Opts,
    Name = str(maps:get(name, Field)),
    Path = [Name | Path0],
    Fix = fmt_fix_header(Field, "array()", Path, Opts),
    Link = fmt_array_link(Type, Path, Opts),
    Fix1 = [Fix, Link],
    case Link =:= "" andalso need_comment_example(array, Opts, Type, Path) of
        true ->
            Indent1 = Indent ++ ?INDENT,
            Opts1 = Opts#{indent => Indent1},
            case fmt_sub_fields(Opts1, Field, All, Path) of
                [] ->
                    fallback_to_example(Field, Fix1, Indent1, Name, Opts1, Indent, "[]");
                ValFields ->
                    [
                        Fix1,
                        Indent1,
                        comment(Comment),
                        Name,
                        ?BIND,
                        "[",
                        ?NL,
                        ValFields,
                        ?NL,
                        Indent1,
                        comment(Comment),
                        "]",
                        ?NL
                    ]
            end;
        false ->
            [Fix1, ?NL]
    end.

fmt(Indent, Comment, Name, Value) ->
    [Indent, comment(Comment), Name, ?BIND, Value, ?NL].

fallback_to_example(Field, Fix1, Indent1, Name, Opts, Indent, Default) ->
    case Field of
        #{examples := Examples} ->
            [
                Fix1,
                Indent1,
                ?COMMENT,
                Name,
                ?BIND,
                fmt_example(Examples, Opts#{comment => true}),
                ?NL
            ];
        _ ->
            Default2 =
                case get_default(Field, Opts) of
                    undefined -> Default;
                    Default1 -> Default1
                end,
            [Fix1, Indent, ?COMMENT, Name, ?BIND, Default2, ?NL]
    end.

fmt_field_with_example(Path, SubFields, Examples, All, Opts1) ->
    lists:map(
        fun(F) ->
            #{name := N} = F,
            case maps:find(N, Examples) of
                {ok, SubExample} ->
                    fmt_field(F#{examples => SubExample}, All, Path, Opts1);
                error ->
                    fmt_field(F, All, Path, Opts1)
            end
        end,
        SubFields
    ).

fmt_sub_fields(Opts, Field, All, Path) ->
    Opts1 = Opts#{comment => true},
    SubFields = get_sub_fields(Field),
    [fmt_field(F, All, Path, Opts1) || F <- SubFields].

get_sub_fields(#{type := #{kind := array, elements := ElemT}, name := Name} = Field) ->
    case is_simple_type(ElemT) of
        true ->
            [];
        false ->
            Examples =
                case get_examples(Name, Field) of
                    undefined -> [];
                    Example0 -> Example0
                end,
            [
                #{
                    name => {"$INDEX", str(Name) ++ ".$INDEX"},
                    type => ElemT,
                    examples => {array, Examples}
                }
            ]
    end;
get_sub_fields(#{type := #{kind := union, members := Members}, name := Name} = Field) ->
    case is_simple_type(Members) of
        true ->
            [];
        false ->
            Example =
                case get_examples(Name, Field) of
                    undefined ->
                        [];
                    [Example0] when is_map(Example0) ->
                        [Value || #{value := Value} <- maps:values(Example0)];
                    %% TODO array
                    _ ->
                        []
                end,
            lists:map(
                fun(M) -> #{name => Name, type => M, examples => {union, Example}} end, Members
            )
    end;
get_sub_fields(#{type := #{kind := map, values := ValT, name := MapName0}} = Field) ->
    MapName = "$" ++ str(MapName0),
    case get_examples(MapName0, Field) of
        undefined ->
            [#{name => MapName, type => ValT}];
        [] ->
            [#{name => MapName, type => ValT}];
        Examples ->
            lists:map(
                fun(Example) ->
                    [{SubName, SubValue}] = maps:to_list(Example),
                    #{name => {MapName, SubName}, type => ValT, examples => SubValue}
                end,
                Examples
            )
    end.

filter_union_example(Examples0, SubFields) ->
    TargetKeys = lists:sort([binary_to_atom(Name) || #{name := Name} <- SubFields]),
    Examples =
        lists:filtermap(
            fun(Example) ->
                case lists:all(fun(K) -> lists:member(K, TargetKeys) end, maps:keys(Example)) of
                    true -> {true, ensure_bin_key(Example)};
                    false -> false
                end
            end,
            Examples0
        ),
    case Examples of
        [Example] -> Example;
        [] -> #{};
        Other -> throw({error, {find_union_example_failed, Examples, SubFields, Other}})
    end.

ensure_bin_key(Map) ->
    maps:fold(
        fun
            (K0, V0 = #{}, Acc) -> Acc#{bin(K0) => ensure_bin_key(V0)};
            (K0, V, Acc) -> Acc#{bin(K0) => V}
        end,
        #{},
        Map
    ).

fmt_desc(#{desc := Desc0}, Indent) ->
    Target = iolist_to_binary([?NL, Indent, ?COMMENT2]),
    Desc = string:trim(Desc0, both),
    replace_nl(Indent, true, Desc, Target);
fmt_desc(_, _) ->
    <<"">>.

fmt_type(Type, Indent) ->
    [Indent, ?COMMENT2, ?TYPE, Type, ?NL].

fmt_path(Path, Indent) -> [Indent, ?COMMENT2, ?PATH, hocon_schema:path(Path), ?NL].

fmt_fix_header(Field, Type, Path, #{indent := Indent}) ->
    [
        fmt_desc(Field, Indent),
        fmt_path(Path, Indent),
        fmt_type(Type, Indent),
        fmt_default(Field, Indent)
    ].

fmt_map_link(Path0, Type, All, Opts) ->
    case Type of
        #{values := #{name := ValueName}} ->
            fmt_map_link2(Path0, ValueName, All, Opts);
        #{values := #{members := Members}} ->
            lists:map(fun(M) -> fmt_map_link(Path0, M, All, Opts) end, Members);
        _ ->
            []
    end.

fmt_map_link2(Path0, ValueName, All, Opts) ->
    Paths =
        case maps:find(ValueName, All) of
            {ok, #{paths := SubPaths}} -> SubPaths;
            _ -> []
        end,
    PathStr = hocon_schema:path(Path0),
    Path = bin(PathStr),
    #{indent := Indent} = Opts,
    case find_link(Opts, {map, PathStr}) of
        {ok, Link} ->
            [Indent, ?COMMENT2, ?LINK, Link, ?NL];
        {error, not_found} ->
            case lists:member(Path, Paths) of
                true ->
                    insert_link(Opts, [{{map, binary_to_list(P)}, Path} || P <- Paths, P =/= Path]);
                false ->
                    ok
            end,
            ""
    end.

fmt_union_link(Type = #{members := Members}, Path, Opts = #{indent := Indent}) ->
    case find_link(Opts, {union, Type}) of
        {ok, Link} ->
            link(Link, Indent);
        {error, not_found} ->
            case is_simple_type(Members) of
                true -> ok;
                false -> insert_link(Opts, {{union, Type}, Path})
            end,
            ""
    end.

fmt_array_link(Type = #{elements := ElemT}, Path, Opts = #{indent := Indent}) ->
    case find_link(Opts, {array, Type}) of
        {ok, Link} ->
            link(Link, Indent);
        {error, not_found} ->
            case is_simple_type(ElemT) of
                true -> ok;
                false -> insert_link(Opts, {{array, Type}, Path})
            end,
            ""
    end.

link(Link, Indent) ->
    [Indent, ?COMMENT2, ?LINK, hocon_schema:path(Link), ?NL].

is_simple_type(Types) when is_list(Types) ->
    lists:all(
        fun(#{kind := Kind}) ->
            Kind =:= primitive orelse Kind =:= singleton
        end,
        Types
    );
is_simple_type(Type) ->
    is_simple_type([Type]).

need_comment_example(map, Opts, Path) ->
    case find_link(Opts, {map, hocon_schema:path(Path)}) of
        {ok, _} -> false;
        {error, not_found} -> true
    end.

need_comment_example(Type, Opts, Key, Link) when Type =:= union; Type =:= array ->
    case find_link(Opts, {union, Key}) of
        {ok, Link} -> true;
        {error, not_found} -> true;
        {ok, _} -> false
    end.

get_examples(_MapName, #{examples := Examples}) ->
    ensure_list(Examples);
get_examples(MapName, #{default := #{hocon := Hocon}}) ->
    case hocon:binary(Hocon) of
        {ok, Default} -> [#{MapName => Default}];
        {error, _} -> [#{MapName => Hocon}]
    end;
get_examples(_, _) ->
    undefined.

fmt_examples(Name, #{examples := {union, Examples}}, Opts) ->
    fmt_examples(Name, #{examples => Examples}, Opts);
fmt_examples(Name, #{examples := Examples}, Opts) ->
    #{indent := Indent, comment := Comment} = Opts,
    lists:map(
        fun(E) ->
            [Indent, comment(Comment), Name, ?BIND, fmt_example(E, Opts), ?NL]
        end,
        ensure_list(Examples)
    );
fmt_examples(Name, Field, Opts = #{indent := Indent, comment := Comment}) ->
    case get_default(Field, Opts) of
        undefined -> [Indent, ?COMMENT, Name, ?BIND, ?NL];
        Default -> fmt(Indent, Comment, Name, Default)
    end.

ensure_list(L) when is_list(L) -> L;
ensure_list(T) -> [T].

fmt_example(Value, #{indent := Indent0, comment := Comment}) ->
    case hocon_pp:do(Value, #{newline => "", embedded => true}) of
        [OneLine] ->
            [try_to_remove_quote(OneLine)];
        Lines ->
            Indent = Indent0 ++ ?INDENT,
            Target = iolist_to_binary([?NL, Indent, comment(Comment)]),
            [
                ?NL,
                Indent,
                comment(Comment),
                binary:replace(bin(Lines), [<<"\n">>], Target, [global]),
                ?NL
            ]
    end.

fmt_default(Field, Indent) ->
    case get_default(Field, #{indent => Indent, comment => true}) of
        undefined -> "";
        Default -> [Indent, ?COMMENT2, ?DEFAULT, Default, ?NL]
    end.

get_default(#{default := Default}, Opts) when is_map(Opts) ->
    #{indent := Indent, comment := Comment} = Opts,
    get_default(Default, Indent, Comment);
get_default(_, _Opts) ->
    undefined.

-define(RE, <<"^[A-Za-z0-9\"]+$">>).

get_default(#{oneliner := true, hocon := Content}, _Indent, _Comment) ->
    try_to_remove_quote(Content);
get_default(#{oneliner := false, hocon := Content}, Indent0, Comment) ->
    Target = iolist_to_binary([?NL, Indent0, comment2(Comment), ?INDENT]),
    replace_nl(Indent0, Comment, Content, Target);
get_default(Bin, _Indent, _Comment) ->
    Bin.

replace_nl(Indent0, Comment, Content, Target) ->
    [
        ?NL,
        Indent0,
        comment2(Comment),
        ?INDENT,
        binary:replace(Content, [<<"\n">>], Target, [global]),
        ?NL
    ].

try_to_remove_quote(Content) ->
    case re:run(Content, ?RE) of
        nomatch ->
            Content;
        _ ->
            case string:trim(Content, both, [$"]) of
                <<"">> -> Content;
                Other -> Other
            end
    end.

bin(S) when is_list(S) -> unicode:characters_to_binary(S, utf8);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom);
bin(Int) when is_integer(Int) -> integer_to_binary(Int);
bin(Bin) -> Bin.

str(A) when is_atom(A) -> atom_to_list(A);
str(S) when is_list(S) -> S;
str(B) when is_binary(B) -> binary_to_list(B);
str({KeyName, _ValName}) -> str(KeyName).

comment(true) -> ?COMMENT;
comment(false) -> "".

comment2(true) -> ?COMMENT2;
comment2(false) -> "".

new_link_cache() ->
    ets:new(?MODULE, [private, set, {keypos, 1}]).

delete_link_cache(#{tid := Tid}) ->
    ets:delete(Tid).

find_link(#{tid := Tid}, Key) ->
    case ets:lookup(Tid, Key) of
        [{_, Value}] -> {ok, Value};
        [] -> {error, not_found}
    end.

insert_link(#{tid := Tid}, Item) ->
    ets:insert(Tid, Item).

resolve_name({N1, N2}) -> {N1, N2};
resolve_name(N) -> {N, N}.
