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
  case file:script(File) of
    {ok, {CurrRelVsn, UpVsnRUs, DnVsnRUs}} ->
      ?INFO("injecting instructions to: ~p", [File]),
      UpdatedContent = {CurrRelVsn, inject_relup_instrs(up, CurrRelVsn, UpVsnRUs),
                          inject_relup_instrs(down, CurrRelVsn, DnVsnRUs)},
      ok = file:write_file(File, term_to_text(UpdatedContent));
    {ok, _BadFormat} ->
      ?ERROR("bad formatted relup file: ~p", [File]);
    {error, Reason} ->
      ?ERROR("read relup file ~p failed: ~p", [File, Reason])
  end.

inject_relup_instrs(Type, CurrRelVsn, RUs) ->
  [{Vsn, Desc, append_emqx_relup_instrs(Type, CurrRelVsn, Vsn, Instrs)} || {Vsn, Desc, Instrs} <- RUs].

%% The `{apply, emqx_relup, post_release_upgrade, []}` will be appended to the end of
%% the instruction lists.
append_emqx_relup_instrs(Type, CurrRelVsn, Vsn, Instrs) ->
  CallbackFun = relup_callback_func(Type),
  case lists:reverse(Instrs) of
    [{apply, {emqx_relup, CallbackFun, _}} | _] ->
      Instrs;
    RInstrs ->
      lists:reverse([ {load_object_code, {emqx, CurrRelVsn, [emqx_relup]}}
                    , {load, {emqx_relup, brutal_purge, soft_purge}}
                    , {apply, {emqx_relup, CallbackFun, [CurrRelVsn, Vsn]}}
                    | RInstrs])
  end.

relup_callback_func(up) -> post_release_upgrade;
relup_callback_func(down) -> post_release_downgrade.

term_to_text(Term) ->
  io_lib:format("~p.", [Term]).
