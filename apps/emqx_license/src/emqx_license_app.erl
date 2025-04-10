%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_app).
-feature(maybe_expr, enable).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    Reader = fun emqx_license:read_license/0,
    case validate_license(Reader) of
        ok ->
            ok = emqx_license:load(),
            emqx_license_sup:start_link(Reader);
        {error, 'SINGLE_NODE_LICENSE'} ->
            {error,
                "SINGLE_NODE_LICENSE, make sure this node and peer nodes are configured with a valid license"}
    end.

stop(_State) ->
    ok = emqx_license:unload(),
    ok.

%% License violation check is done here (but not before boot)
%% because we must allow default single-node license to join cluster,
%% then check if the **fetched** license from peer node allows clustering.
validate_license(Reader) ->
    maybe
        {ok, License} ?= Reader(),
        ok ?= emqx_license_checker:no_violation(License)
    end.
