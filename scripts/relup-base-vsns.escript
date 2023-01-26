#!/usr/bin/env escript
%% -*- mode: erlang; -*-

-mode(compile).

-define(RED, "\e[31m").
-define(RESET, "\e[39m").

usage() ->
"A script to manage the released versions of EMQX for relup and hot
upgrade/downgrade.

We store a \"database\" of released versions as an `eterm' file, which
is a mapping from a given version `Vsn' to its OTP version and a list
of previous versions from which one can upgrade to `Vsn' (the
\"from_versions\" list).  That allow us to more easily/explicitly keep
track of allowed version upgrades/downgrades, as well as OTP changes
between releases

In the examples below, `VERSION_DB_PATH' represents the path to the
`eterm' file containing the version database to be used.

Usage:

  * List the previous base versions from which `TO_VSN' may be
    upgraded to.  Used to list versions for which relup files are to
    be made.

    relup-base-vsns.escript base-vsns TO_VSN VERSION_DB_PATH


  * Show the OTP version with which `Vsn' was built.

    relup-base-vsns.escript otp-vsn-for VSN VERSION_DB_PATH


  * Automatically inserts a new version into the database.  Previous
    versions with the same Major and Minor numbers as `Vsn' are
    considered to be upgradeable from, and versions with higher Major
    and Minor numbers will automatically include `Vsn' in their
    \"from_versions\" list.

    For example, if inserting 4.4.8 when 4.5.0 and 4.5.1 exists,
    versions `BASE_FROM_VSN'...4.4.7 will be considered 4.4.8's
    \"from_versions\", and 4.4.8 will be included into 4.5.0 and
    4.5.1's from versions.

    relup-base-vsns.escript insert-new-vsn NEW_VSN BASE_FROM_VSN OTP_VSN VERSION_DB_PATH

  * Check if the version database is consistent considering `VSN'.

    relup-base-vsns.escript check-vsn-db VSN VERSION_DB_PATH
".

main(["base-vsns", To0, VsnDB]) ->
    VsnMap = read_db(VsnDB),
    To = strip_pre_release(To0),
    #{from_versions := Froms} = fetch_version(To, VsnMap),
    AvailableVersionsIndex = available_versions_index(),
    lists:foreach(
     fun(From) ->
       io:format(user, "~s~n", [From])
     end,
     filter_froms(Froms, AvailableVersionsIndex)),
    halt(0);
main(["otp-vsn-for", Vsn0, VsnDB]) ->
    VsnMap = read_db(VsnDB),
    Vsn = strip_pre_release(Vsn0),
    #{otp := OtpVsn} = fetch_version(Vsn, VsnMap),
    io:format(user, "~s~n", [OtpVsn]),
    halt(0);
main(["insert-new-vsn", NewVsn0, BaseFromVsn0, OtpVsn0, VsnDB]) ->
    VsnMap = read_db(VsnDB),
    NewVsn = strip_pre_release(NewVsn0),
    validate_version(NewVsn),
    BaseFromVsn = strip_pre_release(BaseFromVsn0),
    validate_version(BaseFromVsn),
    OtpVsn = list_to_binary(OtpVsn0),
    case VsnMap of
      #{NewVsn := _} ->
          print_warning("Version ~s already in DB!~n", [NewVsn]),
          halt(1);
      #{BaseFromVsn := _} ->
          ok;
      _ ->
          print_warning("Version ~s not found in DB!~n", [BaseFromVsn]),
          halt(1)
    end,
    NewVsnMap = insert_new_vsn(VsnMap, NewVsn, OtpVsn, BaseFromVsn),
    NewVsnList =
        lists:sort(
         fun({Vsn1, _}, {Vsn2, _}) ->
           parse_vsn(Vsn1) < parse_vsn(Vsn2)
         end, maps:to_list(NewVsnMap)),
    {ok, FD} = file:open(VsnDB, [write]),
    io:format(FD, "%% -*- mode: erlang; -*-\n\n", []),
    lists:foreach(
      fun(Entry) ->
        io:format(FD, "~p.~n", [Entry])
      end,
      NewVsnList),
    file:close(FD),
    halt(0);
main(["check-vsn-db", NewVsn0, VsnDB]) ->
    VsnMap = read_db(VsnDB),
    NewVsn = strip_pre_release(NewVsn0),
    case check_all_vsns_schema(VsnMap) of
        [] -> ok;
        Problems ->
            print_warning("Invalid Version DB ~s!~n", [VsnDB]),
            print_warning("Problems found:~n"),
            lists:foreach(
              fun(Problem) ->
                print_warning("  ~p~n", [Problem])
              end, Problems),
            halt(1)
    end,
    case VsnMap of
        #{NewVsn := _} ->
            io:format(user, "ok~n", []),
            halt(0);
        _ ->
            Candidates = find_insertion_candidates(NewVsn, VsnMap),
            print_warning("Version ~s not found in the version DB!~n", [NewVsn]),
            [] =/= Candidates
                andalso print_warning("Candidates for to insert this version into:~n"),
            lists:foreach(
              fun(Vsn) ->
                io:format(user, "  ~s~n", [Vsn])
              end, Candidates),
            print_warning(
              "To insert this version automatically, run:~n"
              "./scripts/relup-base-vsns insert-new-vsn NEW-VSN BASE-FROM-VSN NEW-OTP-VSN ~s~n"
              "And commit the results.  Be sure to revise the changes.~n"
              "Otherwise, edit the file manually~n",
              [VsnDB]),
            halt(1)
    end;
main(_) ->
    io:format(user, usage(), []),
    halt(1).

strip_pre_release(Vsn0) ->
    case re:run(Vsn0, "[0-9]+\\.[0-9]+\\.[0-9]+", [{capture, all, binary}]) of
        {match, [Vsn]} ->
            Vsn;
        _ ->
            print_warning("Invalid Version: ~s ~n", [Vsn0]),
            halt(1)
    end.

fetch_version(Vsn, VsnMap) ->
    case VsnMap of
        #{Vsn := VsnData} ->
            VsnData;
        _ ->
            print_warning("Version not found in releases: ~s ~n", [Vsn]),
            halt(1)
    end.

filter_froms(Froms0, AvailableVersionsIndex) ->
    Froms1 =
        case get_system() of
            %% we do not support relup for windows
            {"windows", _} ->
                [];
            %% debian11 is introduced since v4.4.2 and e4.4.2
            %% exclude tags before them
            {"debian11", _} ->
                lists:filter(
                  fun(Vsn) ->
                          not lists:member(Vsn, [<<"4.4.0">>, <<"4.4.1">>])
                  end, Froms0);
            %% amzn2 is introduced since v4.4.12 and e4.4.12
            %% exclude tags before them
            {"amzn2", _} ->
                Excluded = [list_to_binary(["4.4.", integer_to_list(X)]) || X <- lists:seq(0,11)],
                lists:filter(fun(Vsn) -> not lists:member(Vsn, Excluded) end, Froms0);
            %% ubuntu22.04 is introduced since v4.4.15 and e4.4.15
            %% exclude tags before them
            {"ubuntu22.04", _} ->
                Excluded = [list_to_binary(["4.4.", integer_to_list(X)]) || X <- lists:seq(0,14)],
                lists:filter(fun(Vsn) -> not lists:member(Vsn, Excluded) end, Froms0);
            %% macos arm64 (M1/M2) packages are introduced since v4.4.12 and e4.4.12
            %% exclude tags before them
            {"macos" ++ _, "aarch64" ++ _} ->
                Excluded = [list_to_binary(["4.4.", integer_to_list(X)]) || X <- lists:seq(0,11)],
                lists:filter(fun(Vsn) -> not lists:member(Vsn, Excluded) end, Froms0);
            {_, _} ->
                Froms0
        end,
    lists:filter(
      fun(V) -> maps:get(V, AvailableVersionsIndex, false) end,
      Froms1).

get_system() ->
    Arch = erlang:system_info(system_architecture),
    case os:getenv("SYSTEM") of
        false ->
            {string:trim(os:cmd("./scripts/get-distro.sh")), Arch};
        System ->
            {System, Arch}
    end.

%% assumes that's X.Y.Z, without pre-releases
parse_vsn(VsnBin) ->
    {match, [Major0, Minor0, Patch0]} = re:run(VsnBin, "([0-9]+)\\.([0-9]+)\\.([0-9]+)",
                                               [{capture, all_but_first, binary}]),
    [Major, Minor, Patch] = lists:map(fun binary_to_integer/1, [Major0, Minor0, Patch0]),
    {Major, Minor, Patch}.

parsed_vsn_to_bin({Major, Minor, Patch}) ->
    iolist_to_binary(io_lib:format("~b.~b.~b", [Major, Minor, Patch])).

find_insertion_candidates(NewVsn, VsnMap) ->
    ParsedNewVsn = parse_vsn(NewVsn),
    [Vsn
     || Vsn <- maps:keys(VsnMap),
        ParsedVsn <- [parse_vsn(Vsn)],
        ParsedVsn > ParsedNewVsn].

check_all_vsns_schema(VsnMap) ->
    maps:fold(
      fun(Vsn, Val, Acc) ->
        Problems =
          [{Vsn, should_be_binary} || not is_binary(Vsn)] ++
          [{Vsn, must_have_map_value} || not is_map(Val)] ++
          [{Vsn, {must_contain_keys, [otp, from_versions]}}
           || case Val of
                  #{otp := _, from_versions := _} ->
                      false;
                  _ ->
                      true
              end] ++
          [{Vsn, otp_version_must_be_binary}
           || case Val of
                  #{otp := Otp} when is_binary(Otp) ->
                      false;
                  _ ->
                      true
              end] ++
          [{Vsn, versions_must_be_list_of_binaries}
           || case Val of
                  #{from_versions := Froms} when is_list(Froms) ->
                      not lists:all(fun is_binary/1, Froms);
                  _ ->
                      true
              end],
        Problems ++ Acc
      end,
      [],
      VsnMap).

insert_new_vsn(VsnMap0, NewVsn, OtpVsn, BaseFromVsn) ->
    ParsedNewVsn = parse_vsn(NewVsn),
    ParsedBaseFromVsn = parse_vsn(BaseFromVsn),
    %% candidates to insert this version into (they are "future" versions)
    Candidates = find_insertion_candidates(NewVsn, VsnMap0),
    %% Past versions we can upgrade from
    Froms = [Vsn || Vsn <- maps:keys(VsnMap0),
                    ParsedVsn <- [parse_vsn(Vsn)],
                    ParsedVsn >= ParsedBaseFromVsn,
                    ParsedVsn < ParsedNewVsn],
    VsnMap1 =
        lists:foldl(
          fun(FutureVsn, Acc) ->
            FutureData0 = #{from_versions := Froms0} = maps:get(FutureVsn, Acc),
            FutureData = FutureData0#{from_versions => lists:usort(Froms0 ++ [NewVsn])},
            Acc#{FutureVsn => FutureData}
          end,
          VsnMap0,
          Candidates),
    VsnMap1#{NewVsn => #{otp => OtpVsn, from_versions => Froms}}.

validate_version(Vsn) ->
    ParsedVsn = parse_vsn(Vsn),
    VsnBack = parsed_vsn_to_bin(ParsedVsn),
    case VsnBack =:= Vsn of
        true -> ok;
        false ->
            print_warning("Invalid version ~p !~n", [Vsn]),
            print_warning("Versions MUST be of the form X.Y.Z "
                          "and not prefixed by `e` or `v`~n"),
            halt(1)
    end.

available_versions_index() ->
    Output = os:cmd("git tag -l"),
    AllVersions =
        lists:filtermap(
          fun(Line) ->
            case re:run(Line, "^[ve]([0-9]+)\\.([0-9]+)\\.([0-9]+)$",
                        [{capture, all_but_first, binary}]) of
                {match, [Major, Minor, Patch]} ->
                    Vsn = iolist_to_binary(io_lib:format("~s.~s.~s", [Major, Minor, Patch])),
                    {true, Vsn};
                _ -> false
            end
          end, string:split(Output, "\n", all)),
    %% FIXME: `maps:from_keys' is available only in OTP 24, but we
    %% still build with 23.  Switch to that once we drop OTP 23.
    maps:from_list([{Vsn, true} || Vsn <- AllVersions]).

read_db(VsnDB) ->
    {ok, VsnList} = file:consult(VsnDB),
    maps:from_list(VsnList).

print_warning(Msg) ->
    print_warning(Msg, []).

print_warning(Msg, Args) ->
    io:format(standard_error, ?RED ++ Msg ++ ?RESET, Args).
