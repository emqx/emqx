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
    {value, _, Sections1} = lists:keytake(<<"modules">>, 1, Sections0),
    {value, {N, Base}, Sections2} = lists:keytake(<<"emqx">>, 1, Sections1),
    IncludeNames = proplists:get_keys(Sections2),
    Includes = lists:map(fun(Name) ->
        iolist_to_binary(["include {{ platform_etc_dir }}/", Name, ".conf"])
    end, IncludeNames),
    ok = dump_sections([{N, Base ++ Includes}| Sections2]).

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
    case is_skipped(Name) of
        true ->
            dump_sections(Rest);
        false ->
            Filename = filename:join(["etc", iolist_to_binary([Name, ".conf.seg"])]),
            Lines = [[L, "\n"] || L <- Lines0],
            ok = file:write_file(Filename, Lines),
            dump_sections(Rest)
    end.

is_skipped(Name) ->
    Name =:= <<"modules">>.
