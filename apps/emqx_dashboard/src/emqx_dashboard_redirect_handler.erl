%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_redirect_handler).

-moduledoc "Tiny cowboy handler that issues an HTTP redirect to a configured Location.".

-export([init/2]).

init(Req0, State) ->
    Location = maps:get(location, State),
    Status = maps:get(status, State, 308),
    Headers = #{
        <<"location">> => Location,
        <<"cache-control">> => <<"no-store">>
    },
    Req = cowboy_req:reply(Status, Headers, <<>>, Req0),
    {ok, Req, State}.
