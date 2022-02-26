#!/usr/bin/env escript

%% This script injects implicit relup instructions for emqx applications.

-mode(compile).

-define(ERROR(FORMAT, ARGS), io:format(standard_error, "[inject-relup] " ++ FORMAT ++"~n", ARGS)).
-define(INFO(FORMAT, ARGS), io:format(user, "[inject-relup] " ++ FORMAT ++"~n", ARGS)).

usage() ->
  "Usage: " ++ escript:script_name() ++ " <path-to-release-dir>".

main([Dir]) ->
  case filelib:is_dir(Dir) of
    true ->
      ok = inject(Dir);
    false ->
      ?ERROR("not a valid dir: ~p", [Dir]),
      erlang:halt(1)
  end;
main(_Args) ->
  ?ERROR("~s", [usage()]),
  erlang:halt(1).

inject(Dir) ->
  RelupFiles = filelib:wildcard(filename:join([Dir, "releases", "*", "relup"])),
  lists:foreach(fun(File) ->
      case file:consult(File) of
        {ok, [{CurrRelVsn, UpVsnRUs, DnVsnRUs}]} ->
          ?INFO("injecting instructions to: ~p", [File]),
          UpdatedContent = [{CurrRelVsn, inject_relup_instrs(CurrRelVsn, UpVsnRUs),
                              inject_relup_instrs(CurrRelVsn, DnVsnRUs)}],
          file:write_file("/tmp/" ++ filename:basename(File), term_to_text(UpdatedContent));
        {ok, _BadFormat} ->
          ?ERROR("bad formatted relup file: ~p", [File]);
        {error, Reason} ->
          ?ERROR("read relup file ~p failed: ~p", [File, Reason])
      end
    end, RelupFiles).

inject_relup_instrs(CurrRelVsn, RUs) ->
  [{Vsn, Desc, append_emqx_relup_instrs(CurrRelVsn, Vsn, Instrs)} || {Vsn, Desc, Instrs} <- RUs].

%% The `{apply, emqx_relup, post_release_upgrade, []}` will be appended to the end of
%% the instruction lists.
append_emqx_relup_instrs(CurrRelVsn, Vsn, Instrs) ->
  case lists:reverse(Instrs) of
    [{apply, {emqx_relup, post_release_upgrade, _}} | _] ->
      Instrs;
    RInstrs ->
      lists:reverse([instr_post_release_upgrade(CurrRelVsn, Vsn) | RInstrs])
  end.

instr_post_release_upgrade(CurrRelVsn, Vsn) ->
  {apply, {emqx_relup, post_release_upgrade, [CurrRelVsn, Vsn]}}.

term_to_text(Term) ->
  io_lib:format("~p.", [Term]).