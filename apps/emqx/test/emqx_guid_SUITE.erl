%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_guid_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_guid_gen(_) ->
    Guid1 = emqx_guid:gen(),
    Guid2 = emqx_guid:gen(),
    <<_:128>> = Guid1,
    ?assert((Guid2 >= Guid1)),
    {Ts1, _, 0} = emqx_guid:new(),
    erlang:yield(),
    Ts2 = emqx_guid:timestamp(emqx_guid:gen()),
    case Ts2 > Ts1 of
        true ->
            ok;
        false ->
            ct:fail({Ts1, Ts2})
    end.

t_guid_hexstr(_) ->
    Guid = emqx_guid:gen(),
    ?assertEqual(Guid, emqx_guid:from_hexstr(emqx_guid:to_hexstr(Guid))).
