%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso).

-include_lib("hocon/include/hoconsc.hrl").

-export([types/0, modules/0, provider/1, backends/0]).
%%------------------------------------------------------------------------------
%% Callbacks
%%------------------------------------------------------------------------------
-type request() :: map().
-type parsed_config() :: #{
    backend => atom(),
    atom() => term()
}.
-type state() :: #{atom() => term()}.
-type raw_config() :: #{binary() => term()}.
-type config() :: parsed_config() | raw_config().

-callback hocon_ref() -> ?R_REF(Module :: atom(), Name :: atom() | binary()).
-callback login_ref() -> ?R_REF(Module :: atom(), Name :: atom() | binary()).
-callback create(Config :: config()) ->
    {ok, State :: state()} | {error, Reason :: term()}.
-callback update(Config :: config(), State :: state()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}.
-callback destroy(State :: state()) -> ok.
-callback login(request(), State :: state()) ->
    {ok, Token :: binary()} | {error, Reason :: term()}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------
types() ->
    maps:keys(backends()).

modules() ->
    maps:values(backends()).

provider(Backend) ->
    maps:get(Backend, backends()).

backends() ->
    #{ldap => emqx_dashboard_sso_ldap}.
