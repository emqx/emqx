#!/usr/bin/env escript

%% This script injects implicit relup instructions for emqx applications.

-mode(compile).

-define(ERROR(FORMAT, ARGS), io:format(standard_error, "[inject-relup] " ++ FORMAT ++"~n", ARGS)).
-define(INFO(FORMAT, ARGS), io:format(user, "[inject-relup] " ++ FORMAT ++"~n", ARGS)).

usage() ->
  "Usage: " ++ escript:script_name() ++ " <path-to-relup-file-or-dir>".

main([DirOrFile]) ->
  case filelib:is_dir(DirOrFile) of
    true -> ok = inject_dir(DirOrFile);
    false ->
      case filelib:is_regular(DirOrFile) of
        true -> inject_file(DirOrFile);
        false ->
          ?ERROR("not a valid file: ~p", [DirOrFile]),
          erlang:halt(1)
      end
  end;
main(_Args) ->
  ?ERROR("~s", [usage()]),
  erlang:halt(1).

inject_dir(Dir) ->
  RelupFiles = filelib:wildcard(filename:join([Dir, "**", "relup"])),
  lists:foreach(fun inject_file/1, RelupFiles).

inject_file(File) ->
  EmqxVsn = emqx_vsn_from_rel_file(File),
  case file:script(File) of
    {ok, {CurrRelVsn, UpVsnRUs, DnVsnRUs}} ->
      ?INFO("injecting instructions to: ~p", [File]),
      UpdatedContent = {CurrRelVsn, inject_relup_instrs(up, EmqxVsn, CurrRelVsn, UpVsnRUs),
                          inject_relup_instrs(down, EmqxVsn, CurrRelVsn, DnVsnRUs)},
      ok = file:write_file(File, term_to_text(UpdatedContent));
    {ok, _BadFormat} ->
      ?ERROR("bad formatted relup file: ~p", [File]),
      error({bad_relup_format, File});
    {error, Reason} ->
      ?ERROR("read relup file ~p failed: ~p", [File, Reason]),
      error({read_relup_error, Reason})
  end.

inject_relup_instrs(Type, EmqxVsn, CurrRelVsn, RUs) ->
  [{Vsn, Desc, append_emqx_relup_instrs(Type, EmqxVsn, CurrRelVsn, Vsn, Instrs)}
   || {Vsn, Desc, Instrs} <- RUs].

%% The `{apply, emqx_relup, post_release_upgrade, []}` will be appended to the end of
%% the instruction lists.
append_emqx_relup_instrs(Type, EmqxVsn, CurrRelVsn, Vsn, Instrs) ->
  CallbackFun = relup_callback_func(Type),
  Extra = #{}, %% we may need some extended args
  case lists:reverse(Instrs) of
    [{apply, {emqx_relup, CallbackFun, _}} | _] ->
      Instrs;
    RInstrs ->
      Instrs2 = lists:reverse(
          [ {apply, {emqx_relup, CallbackFun, [CurrRelVsn, Vsn, Extra]}}
          , {load, {emqx_relup, brutal_purge, soft_purge}}
          | RInstrs]),
      %% we have to put 'load_object_code' before 'point_of_no_return'
      %% so here we simply put it to the beginning
      [{load_object_code, {emqx, EmqxVsn, [emqx_relup]}} | Instrs2]
  end.

relup_callback_func(up) -> post_release_upgrade;
relup_callback_func(down) -> post_release_downgrade.

emqx_vsn_from_rel_file(RelupFile) ->
  RelDir = filename:dirname(RelupFile),
  RelFile = filename:join([RelDir, "emqx.rel"]),
  case file:script(RelFile) of
    {ok, {release, {_RelName, _RelVsn}, _Erts, Apps}} ->
      case lists:keysearch(emqx, 1, Apps) of
        {value, {emqx, EmqxVsn}} ->
          EmqxVsn;
        false ->
          error({emqx_vsn_cannot_found, RelFile})
      end;
    {ok, _BadFormat} ->
      ?ERROR("bad formatted .rel file: ~p", [RelFile]);
    {error, Reason} ->
      ?ERROR("read .rel file ~p failed: ~p", [RelFile, Reason])
  end.

term_to_text(Term) ->
  io_lib:format("~p.", [Term]).
