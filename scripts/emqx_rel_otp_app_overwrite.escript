#!/usr/bin/env escript

%% This script is part of 'relup' process to overwrite the OTP app versions (incl. ERTS) in rel files from upgrade base
%% so that 'rebar relup' call will not generate instructions for restarting OTP apps or restarting the emulator.
%%
%% It simply read OTP app version (incl. ERTS) from the rel file of *NEW* Release ($RelVsn) and write back to the ones
%% in *OLD* versions ($BASE_VERSIONS)
%%
%% note, we use NEW to overwrite OLD is because the modified NEW rel file will be overwritten by next 'rebar relup'
%%

-mode(compile).

main([Dir, RelVsn, BASE_VERSIONS]) ->
    {ErtsVsn, Overwrites} = get_otp_apps(rel_file(Dir, RelVsn), RelVsn),
    lists:foreach(fun(BaseVer) ->
                          base_rel_overwrites(BaseVer, Dir, ErtsVsn, Overwrites)
                  end, string:tokens(BASE_VERSIONS, ",")).

get_otp_apps(RelFile, RelVsn) ->
    case file:consult(RelFile) of
        {ok, [{release, {"emqx", RelVsn}, {erts, ErtsVsn}, AppList}]} ->
            Apps = lists:filter(fun(X) -> lists:member(element(1, X), otp_apps()) end, AppList),
            {ErtsVsn, Apps};
        {error, Reason} ->
            io:format(standard_error, "failed_to_read_file ~p for release ~p~nreason=~p~n", [RelFile, RelVsn, Reason]),
            halt(1)
    end.

base_rel_overwrites(RelVsn, Dir, ErtsVsn, Overwrites) ->
    RelFile = rel_file(Dir, RelVsn),
    file:copy(RelFile, RelFile++".bak"),
    {ok, [{release, {"emqx", RelVsn}, {erts, _BaseErtsVsn}, BaseAppList}]} = file:consult(RelFile),
    NewData = [ {release, {"emqx", RelVsn}, {erts, ErtsVsn},
                 lists:map(fun(X) ->
                                   Name = element(1, X),
                                   case lists:keyfind(Name, 1, Overwrites) of
                                       false -> X;
                                       Y when is_tuple(Y) -> Y
                                   end
                           end, BaseAppList)
                }
              ],
    ok = file:write_file(RelFile, io_lib:format("~p.", NewData)).

rel_file(Dir, RelVsn)->
    filename:join([Dir, RelVsn, "emqx.rel"]).

otp_apps() ->
    {ok, [Apps]} = file:consult("scripts/rel_otp_apps.eterm"),
    true = is_list(Apps),
    lists:map(fun(App) when is_atom(App) -> App;
                 ({App, _}) -> App %% handle like {mnesia, load}
              end, Apps).
