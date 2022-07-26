#!/usr/bin/env escript
%% This script is part of 'relup' process to overwrite the OTP app versions (incl. ERTS) in rel files from upgrade base
%% so that 'rebar relup' call will not generate instructions for restarting OTP apps or restarting the emulator.
%%
%% It simply read OTP app version (incl. ERTS) from the rel file of *NEW* Release ($RelVsn) and write back to the ones
%% in *OLD* versions ($BASE_VERSIONS)
%%
%% note, we use NEW to overwrite OLD is because the modified NEW rel file will be overwritten by next 'rebar relup'
%%
main([Dir, Profile, RelVsn, BASE_VERSIONS]) ->
    {ErtsVsn, Overwrites} = get_otp_apps(rel_file(Profile, Dir, RelVsn), RelVsn),
    lists:foreach(fun(BaseVer) ->
                          base_rel_overwrites(BaseVer, Profile, Dir, ErtsVsn, Overwrites)
                  end, string:tokens(BASE_VERSIONS, ",")).

get_otp_apps(RelFile, RelVsn) ->
    {ok, [{release, {"emqx", RelVsn}, {erts, ErtsVsn}, AppList}]} = file:consult(RelFile),
    Apps = lists:filter(fun(X) -> lists:member(element(1, X), otp_apps()) end, AppList),
    {ErtsVsn, Apps}.

base_rel_overwrites(RelVsn, Profile, Dir, ErtsVsn, Overwrites) ->
    RelFile = rel_file(Profile, Dir, RelVsn),
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

rel_file("emqx-edge", Dir, RelVsn)->
    rel_file("emqx", Dir, RelVsn);
rel_file(Profile, Dir, RelVsn)->
    filename:join([Dir, RelVsn, Profile++".rel"]).


%% Couldn't find a good way to get this list dynamicly.
otp_apps() ->
    [ kernel
    , stdlib
    , sasl
    , crypto
    , public_key
    , asn1
    , syntax_tools
    , ssl
    , os_mon
    , inets
    , compiler
    , runtime_tools
    , mnesia
    , xmerl
    ].
