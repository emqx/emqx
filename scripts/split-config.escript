#!/usr/bin/env escript

%% This script reads up emqx.conf and split the sections
%% and dump sections to separate files.
%% Sections are grouped between CONFIG_SECTION_BGN and
%% CONFIG_SECTION_END pairs
%%
%% NOTE: this feature is so far not used in opensource
%% edition due to backward-compatibility reasons.

-mode(compile).

-define(BASE, <<"emqx">>).

main(_) ->
    {ok, Bin} = file:read_file("etc/emqx.conf"),
    Lines = binary:split(Bin, <<"\n">>, [global]),
    Sections0 = parse_sections(Lines),
    {Base1, Sections, IncludeNames1} = lists:foldl(
        fun({Name, Section}, {Base, Includes, IncludeNames}) ->
            case Name of
                <<"emqx">> -> {{Name, Section}, Includes, IncludeNames};
                <<"modules">> -> {Base, Includes, IncludeNames};
                Name -> {Base, [{Name, Section} | Includes], [Name | IncludeNames]}
            end
        end, {{}, [], []}, Sections0),
    ok = dump_sections(Sections),
    ok = dump_base(Base1, IncludeNames1).

parse_sections(Lines) ->
    {ok, P} = re:compile("#+\s*CONFIG_SECTION_(BGN|END)\s*=\s*([^\s-]+)\s*="),
    Parser =
        fun(Line) ->
                case re:run(Line, P, [{capture, all_but_first, binary}]) of
                    {match, [<<"BGN">>, Name]} -> {section_bgn, Name};
                    {match, [<<"END">>, Name]} -> {section_end, Name};
                    nomatch -> continue
                end
        end,
    parse_sections(Lines, Parser, ?BASE, #{?BASE => []}).

parse_sections([], _Parse, _Section, Sections) ->
    lists:map(fun({N, Lines}) -> {N, lists:reverse(Lines)} end,
              maps:to_list(Sections));
parse_sections([Line | Lines], Parse, Section, Sections) ->
    case Parse(Line) of
        {section_bgn, Name} ->
            ?BASE = Section, %% assert
            true = (Name =/= ?BASE), %% assert
            false = maps:is_key(Name, Sections), %% assert
            NewSections = Sections#{?BASE := maps:get(?BASE, Sections), Name => []},
            parse_sections(Lines, Parse, Name, NewSections);
        {section_end, Name} ->
            true = (Name =:= Section), %% assert
            parse_sections(Lines, Parse, ?BASE, Sections);
        continue ->
            Acc = maps:get(Section, Sections),
            parse_sections(Lines, Parse, Section, Sections#{Section => [Line | Acc]})
    end.

dump_sections([]) -> ok;
dump_sections([{Name, Lines0} | Rest]) ->
    Lines = [[L, "\n"] || L <- Lines0],
    save_conf(Name, Lines),
    dump_sections(Rest).

dump_base({Name, Lines0}, IncludeNames0) ->
    Includes = lists:map(fun(Name) ->
        iolist_to_binary(["include {{ platform_etc_dir }}/", Name, ".conf"])
    end, IncludeNames0),
    Lines = [[L, "\n"] || L <-  Lines0 ++ Includes],
    save_conf(Name, Lines).

save_conf(Name, Lines) ->
    Filename = filename:join(["etc", iolist_to_binary([Name, ".conf.seg"])]),
    Lines = [[L, "\n"] || L <-  Lines0 ++ Includes],
    ok = file:write_file(Filename, Lines).