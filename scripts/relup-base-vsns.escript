#!/usr/bin/env escript
%% -*- mode: erlang; -*-

-mode(compile).

-define(RED, "\e[31m").
-define(RESET, "\e[39m").

main(["base-vsns", To0, VsnDB]) ->
    {ok, [VsnMap]} = file:consult(VsnDB),
    To = strip_pre_release(To0),
    #{from_versions := Froms} = fetch_version(To, VsnMap),
    lists:foreach(
     fun(From) ->
       io:format(user, "~s~n", [From])
     end,
     filter_froms(Froms)),
    halt(0);
main(["otp-vsn-for", Vsn0, VsnDB]) ->
    {ok, [VsnMap]} = file:consult(VsnDB),
    Vsn = strip_pre_release(Vsn0),
    #{otp := OtpVsn} = fetch_version(Vsn, VsnMap),
    io:format(user, "~s~n", [OtpVsn]),
    halt(0);
main(["insert-new-vsn", NewVsn0, BaseFromVsn0, OtpVsn0, VsnDB]) ->
    {ok, [VsnMap]} = file:consult(VsnDB),
    NewVsn = strip_pre_release(NewVsn0),
    BaseFromVsn = strip_pre_release(BaseFromVsn0),
    OtpVsn = list_to_binary(OtpVsn0),
    case VsnMap of
      #{NewVsn := _} ->
          io:format(user, ?RED ++ "Version ~s already in DB!~n" ++ ?RESET, [NewVsn]),
          halt(1);
      #{BaseFromVsn := _} ->
          ok;
      _ ->
          io:format(user, ?RED ++ "Version ~s not found in DB!~n" ++ ?RESET, [BaseFromVsn]),
          halt(1)
    end,
    NewVsnMap = insert_new_vsn(VsnMap, NewVsn, OtpVsn, BaseFromVsn),
    file:write_file(VsnDB, io_lib:format("%% -*- mode: erlang; -*-\n\n~p.", [NewVsnMap])),
    halt(0);
main(["check-vsn-db", NewVsn0, VsnDB]) ->
    {ok, [VsnMap]} = file:consult(VsnDB),
    NewVsn = strip_pre_release(NewVsn0),
    case check_all_vsns_schema(VsnMap) of
        [] -> ok;
        Problems ->
            io:format(user, ?RED ++ "Invalid Version DB ~s!~n", [VsnDB]),
            io:format(user, "Problems found:~n", []),
            lists:foreach(
              fun(Problem) ->
                io:format(user, "  ~p~n", [Problem])
              end, Problems),
            io:format(user, ?RESET, []),
            halt(1)
    end,
    case VsnMap of
        #{NewVsn := _} ->
            io:format(user, "ok~n", []),
            halt(0);
        _ ->
            Candidates = find_insertion_candidates(NewVsn, VsnMap),
            io:format(user, ?RED ++ "Version ~s not found in the version DB!~n", [NewVsn]),
            [] =/= Candidates
                andalso io:format(user, "Candidates for to insert this version into:~n", []),
            lists:foreach(
              fun(Vsn) ->
                io:format(user, "  ~s~n", [Vsn])
              end, Candidates),
            io:format(
              user,
              "To insert this version automatically, run:~n"
              "./scripts/relup-base-vsns insert-new-vsn NEW-VSN BASE-FROM-VSN NEW-OTP-VSN ~s~n"
              "And commit the results.  Be sure to revise the changes.~n"
              "Otherwise, edit the file manually~n"
              ?RESET,
              [VsnDB]),
            halt(1)
    end.

strip_pre_release(Vsn0) ->
    case re:run(Vsn0, "[0-9]+\\.[0-9]+\\.[0-9]+", [{capture, all, binary}]) of
        {match, [Vsn]} ->
            Vsn;
        _ ->
            io:format(user, [?RED, "Invalid Version: ~s ~n", ?RESET], [Vsn0]),
            halt(1)
    end.

fetch_version(Vsn, VsnMap) ->
    case VsnMap of
        #{Vsn := VsnData} ->
            VsnData;
        _ ->
            io:format(user, [?RED, "Version not found in releases: ~s ~n", ?RESET], [Vsn]),
            halt(1)
    end.

filter_froms(Froms) ->
    case os:getenv("SYSTEM") of
        %% debian11 is introduced since v4.4.2 and e4.4.2
        %% exclude tags before them
        "debian11" ->
            lists:filter(
              fun(Vsn) ->
                not lists:member(Vsn, [<<"4.4.0">>, <<"4.4.1">>])
              end, Froms);
        _ ->
            Froms
    end.

%% assumes that's X.Y.Z, without pre-releases
parse_vsn(VsnBin) ->
    {match, [Major0, Minor0, Patch0]} = re:run(VsnBin, "([0-9]+)\\.([0-9]+)\\.([0-9]+)",
                                               [{capture, all_but_first, binary}]),
    [Major, Minor, Patch] = lists:map(fun binary_to_integer/1, [Major0, Minor0, Patch0]),
    {Major, Minor, Patch}.

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
