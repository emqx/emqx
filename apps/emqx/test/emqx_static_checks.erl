%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_static_checks).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    logger:notice(
        asciiart:visible(
            $=,
            "If this test suite failed, and you are unsure why, read this:~n"
            "https://github.com/emqx/emqx/blob/master/apps/emqx/src/bpapi/README.md",
            []
        )
    ).

check_if_versions_consistent(OldData, NewData) ->
    %% OldData can contain a wider list of BPAPI versions
    %% than the release being checked.
    [] =:= NewData -- OldData.

t_run_check(_) ->
    try
        {ok, OldData} = file:consult(emqx_bpapi_static_checks:versions_file()),
        ?assert(emqx_bpapi_static_checks:run()),
        {ok, NewData} = file:consult(emqx_bpapi_static_checks:versions_file()),
        check_if_versions_consistent(OldData, NewData) orelse
            begin
                logger:critical(
                    asciiart:visible(
                        $=,
                        "BPAPI versions were changed, but not committed to the repo.\n\n"
                        "Versions file is generated automatically, to update it, run\n"
                        "'make && make static_checks' locally, and then add the\n"
                        "changed 'bpapi.versions' files to the commit.\n",
                        []
                    )
                ),
                error(version_mismatch)
            end,
        BpapiDumps = filelib:wildcard(
            filename:join(
                emqx_bpapi_static_checks:dumps_dir(),
                "*" ++ emqx_bpapi_static_checks:dump_file_extension()
            )
        ),
        logger:info("Backplane API dump files: ~p", [BpapiDumps]),
        ?assert(emqx_bpapi_static_checks:check_compat(BpapiDumps))
    catch
        error:version_mismatch ->
            error(tc_failed);
        EC:Err:Stack ->
            logger:critical("Test suite failed: ~p:~p~nStack:~p", [EC, Err, Stack]),
            error(tc_failed)
    end.

t_swagger_i18n_check(_) ->
    try
        ApiFiles = swagger_api_files(),
        I18nFiles = i18n_files(),
        I18nIndex = read_i18n_index(I18nFiles),
        lists:foreach(
            fun(File) ->
                check_swagger_file(File, I18nIndex)
            end,
            ApiFiles
        )
    catch
        EC:Err:Stack ->
            logger:critical("Swagger i18n static check failed: ~p:~p~nStack:~p", [EC, Err, Stack]),
            error(tc_failed)
    end.

swagger_api_files() ->
    All = filelib:fold_files(
        "apps",
        ".*\\.erl$",
        true,
        fun(File, Acc) ->
            case is_swagger_api_file(File) of
                true -> [File | Acc];
                false -> Acc
            end
        end,
        []
    ),
    lists:sort(All).

is_swagger_api_file(File) ->
    case lists:suffix(".erl_", File) orelse string:find(File, "/test/") =/= nomatch of
        true ->
            false;
        false ->
            case file:read_file(File) of
                {ok, Bin} ->
                    binary:match(Bin, <<"emqx_dashboard_swagger:spec(">>) =/= nomatch;
                _ ->
                    false
            end
    end.

i18n_files() ->
    filelib:wildcard("rel/i18n/*.hocon").

read_i18n_texts(Files) ->
    lists:map(
        fun(File) ->
            {ok, Bin} = file:read_file(File),
            {File, unicode:characters_to_list(Bin)}
        end,
        Files
    ).

read_i18n_index(Files) ->
    lists:foldl(
        fun(File, Acc0) ->
            {ok, C} = hocon:load(File),
            maps:fold(
                fun
                    (Ns0, Fields, Acc1) when is_map(Fields) ->
                        Ns = to_text(Ns0),
                        maps:fold(
                            fun
                                (Key0, DescAndLabel, Acc2) when is_map(DescAndLabel) ->
                                    case maps:get(<<"label">>, DescAndLabel, undefined) of
                                        undefined ->
                                            Acc2;
                                        Label0 ->
                                            Key = to_text(Key0),
                                            Label = to_text(Label0),
                                            ScopedKey = Ns ++ ":" ++ Key,
                                            Scoped0 = maps:get(scoped, Acc2),
                                            Global0 = maps:get(global, Acc2),
                                            Acc3 = Acc2#{
                                                scoped => maps:put(ScopedKey, Label, Scoped0)
                                            },
                                            case maps:is_key(Key, Global0) of
                                                true ->
                                                    Acc3;
                                                false ->
                                                    Acc3#{global => maps:put(Key, Label, Global0)}
                                            end
                                    end;
                                (_, _, Acc2) ->
                                    Acc2
                            end,
                            Acc1,
                            Fields
                        );
                    (_, _, Acc1) ->
                        Acc1
                end,
                Acc0,
                C
            )
        end,
        #{scoped => #{}, global => #{}},
        Files
    ).

check_swagger_file(File, I18nTexts) ->
    {ok, Bin} = file:read_file(File),
    Lines = binary:split(Bin, <<"\n">>, [global]),
    lists:foreach(
        fun({Kind, Data}) ->
            case Kind of
                summary_forbidden ->
                    error({forbidden_summary, File, Data});
                missing_desc ->
                    error({missing_desc_for_summary, File, Data});
                bad_desc_ref ->
                    error({bad_desc_ref, File, Data});
                missing_label ->
                    error({missing_i18n_ref, Data});
                label_period ->
                    error({label_ends_with_period, Data})
            end
        end,
        collect_violations(File, Lines, I18nTexts)
    ).

collect_violations(_File, Lines, I18nTexts) ->
    collect_violations(Lines, 1, none, [], I18nTexts).

collect_violations([], _LineNo, _State, Acc, _I18nTexts) ->
    lists:reverse(Acc);
collect_violations([LineBin | Rest], LineNo, State0, Acc0, I18nTexts) ->
    Line = binary_to_list(LineBin),
    Stripped = strip_comment(Line),
    {State1, Acc1} = step_collect(Stripped, LineNo, State0, Acc0, I18nTexts),
    collect_violations(Rest, LineNo + 1, State1, Acc1, I18nTexts).

step_collect(Line, LineNo, none, Acc, I18nTexts) ->
    case match_method_block_open(Line) of
        {ok, Indent, inline} ->
            TopIndent = Indent + 4,
            Depth = braces_delta(Line),
            maybe_finalize_state(
                #{
                    top_indent => TopIndent,
                    depth => Depth,
                    has_tags => false,
                    desc_args => [],
                    has_desc => false
                },
                LineNo,
                Acc,
                I18nTexts
            );
        {ok, Indent, pending} ->
            {{pending, Indent}, Acc};
        false ->
            {none, Acc}
    end;
step_collect(Line, _LineNo, {pending, Indent}, Acc, _I18nTexts) ->
    case re:run(Line, "^\\s*#\\{\\s*$", [{capture, none}]) of
        match ->
            TopIndent = Indent + 8,
            Depth = braces_delta(Line),
            {
                {in_block, #{
                    top_indent => TopIndent,
                    depth => Depth,
                    has_tags => false,
                    desc_args => [],
                    has_desc => false
                }},
                Acc
            };
        nomatch ->
            {none, Acc}
    end;
step_collect(Line, LineNo, {in_block, B0}, Acc0, I18nTexts) ->
    TopIndent = maps:get(top_indent, B0),
    HasTags0 = maps:get(has_tags, B0),
    DescArgs0 = maps:get(desc_args, B0),
    HasDesc0 = maps:get(has_desc, B0),
    Acc1 =
        case match_top_key(Line, TopIndent, "summary") of
            true -> [{summary_forbidden, LineNo} | Acc0];
            false -> Acc0
        end,
    HasTags = HasTags0 orelse match_top_key(Line, TopIndent, "tags"),
    {DescArgs, HasDesc} =
        case parse_top_desc_arg(Line, TopIndent) of
            {ok, Arg} -> {[Arg | DescArgs0], true};
            none -> {DescArgs0, HasDesc0};
            bad -> {DescArgs0, true}
        end,
    Depth = maps:get(depth, B0) + braces_delta(Line),
    B1 = B0#{depth => Depth, has_tags => HasTags, desc_args => DescArgs, has_desc => HasDesc},
    case Depth =< 0 of
        true ->
            {none, finalize_method_block(B1, LineNo, Acc1, I18nTexts)};
        false ->
            {{in_block, B1}, Acc1}
    end.

maybe_finalize_state(B, LineNo, Acc, I18nTexts) ->
    case maps:get(depth, B) =< 0 of
        true -> {none, finalize_method_block(B, LineNo, Acc, I18nTexts)};
        false -> {{in_block, B}, Acc}
    end.

finalize_method_block(B, LineNo, Acc, I18nTexts) ->
    case maps:get(has_tags, B) of
        false ->
            Acc;
        true ->
            case maps:get(has_desc, B) of
                false ->
                    [{missing_desc, LineNo} | Acc];
                true ->
                    DescArgs = maps:get(desc_args, B),
                    lists:foldl(
                        fun(Arg, A0) ->
                            validate_desc_arg(Arg, I18nTexts, A0)
                        end,
                        Acc,
                        DescArgs
                    )
            end
    end.

validate_desc_arg(Arg0, I18nIndex, Acc) ->
    Arg = normalize_arg(Arg0),
    case parse_desc_ref(Arg) of
        {one, Key} ->
            Global = maps:get(global, I18nIndex),
            case maps:get(Key, Global, undefined) of
                undefined ->
                    [{missing_label, Key} | Acc];
                Label ->
                    case label_has_trailing_period(Label) of
                        true -> [{label_period, Key} | Acc];
                        false -> Acc
                    end
            end;
        {two, Mod, Key} ->
            Scoped = maps:get(scoped, I18nIndex),
            ScopedKey = Mod ++ ":" ++ Key,
            case maps:get(ScopedKey, Scoped, undefined) of
                undefined ->
                    [{missing_label, Mod ++ ":" ++ Key} | Acc];
                Label ->
                    case label_has_trailing_period(Label) of
                        true -> [{label_period, Mod ++ ":" ++ Key} | Acc];
                        false -> Acc
                    end
            end;
        bad ->
            [{bad_desc_ref, Arg} | Acc]
    end.

label_has_trailing_period(Label) ->
    Trimmed = string:trim(Label),
    case Trimmed of
        [] -> false;
        _ -> lists:last(Trimmed) =:= $.
    end.

parse_desc_ref(Arg) ->
    case re:run(Arg, "^\"([^\"]+)\"$", [unicode, {capture, all_but_first, list}]) of
        {match, [Key]} ->
            {one, Key};
        nomatch ->
            case re:run(Arg, "^([A-Za-z0-9_]+)$", [unicode, {capture, all_but_first, list}]) of
                {match, [Key]} ->
                    {one, Key};
                nomatch ->
                    case
                        re:run(Arg, "^([A-Za-z0-9_]+),\"([^\"]+)\"$", [
                            unicode, {capture, all_but_first, list}
                        ])
                    of
                        {match, [Mod, Key]} ->
                            {two, Mod, Key};
                        nomatch ->
                            case
                                re:run(Arg, "^([A-Za-z0-9_]+),([A-Za-z0-9_]+)$", [
                                    unicode, {capture, all_but_first, list}
                                ])
                            of
                                {match, [Mod, Key]} ->
                                    {two, Mod, Key};
                                nomatch ->
                                    bad
                            end
                    end
            end
    end.

parse_top_desc_arg(Line, TopIndent) ->
    Pat = lists:flatten(
        io_lib:format(
            "^\\s{~B}(?:desc|description)\\s*=>\\s*\\?DESC\\(([^\\)]*)\\)",
            [TopIndent]
        )
    ),
    case re:run(Line, Pat, [unicode, {capture, all_but_first, list}]) of
        {match, [Arg]} ->
            {ok, Arg};
        nomatch ->
            case
                match_top_key(Line, TopIndent, "desc") orelse
                    match_top_key(Line, TopIndent, "description")
            of
                true -> bad;
                false -> none
            end
    end.

match_top_key(Line, TopIndent, Key) ->
    Pat = lists:flatten(io_lib:format("^\\s{~B}~s\\s*=>", [TopIndent, Key])),
    re:run(Line, Pat, [{capture, none}]) =:= match.

match_method_block_open(Line) ->
    PatInline = "^(\\s*)(?:get|post|put|delete|patch|head|options)\\s*=>\\s*#\\{\\s*$",
    case re:run(Line, PatInline, [unicode, {capture, all_but_first, list}]) of
        {match, [IndentStr]} ->
            {ok, length(IndentStr), inline};
        nomatch ->
            PatPending = "^(\\s*)(?:get|post|put|delete|patch|head|options)\\s*=>\\s*$",
            case re:run(Line, PatPending, [unicode, {capture, all_but_first, list}]) of
                {match, [IndentStr]} -> {ok, length(IndentStr), pending};
                nomatch -> false
            end
    end.

normalize_arg(Arg) ->
    re:replace(Arg, "\\s+", "", [global, {return, list}]).

strip_comment(Line) ->
    case re:run(Line, "%%.*$", [{capture, first, index}]) of
        {match, [{Idx, _Len}]} -> lists:sublist(Line, Idx);
        nomatch -> Line
    end.

braces_delta(Line) ->
    Open = length([C || C <- Line, C =:= ${]),
    Close = length([C || C <- Line, C =:= $}]),
    Open - Close.

to_text(Bin) when is_binary(Bin) ->
    unicode:characters_to_list(Bin);
to_text(List) when is_list(List) ->
    List;
to_text(Atom) when is_atom(Atom) ->
    atom_to_list(Atom).
