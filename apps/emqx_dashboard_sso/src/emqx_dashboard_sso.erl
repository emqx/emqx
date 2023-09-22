%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-export([
    hocon_ref/1,
    login_ref/1,
    create/2,
    update/3,
    destroy/2,
    login/3
]).

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
-type hocon_ref() :: ?R_REF(Module :: atom(), Name :: atom() | binary()).

-callback hocon_ref() -> hocon_ref().
-callback login_ref() -> hocon_ref().
-callback create(Config :: config()) ->
    {ok, State :: state()} | {error, Reason :: term()}.
-callback update(Config :: config(), State :: state()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}.
-callback destroy(State :: state()) -> ok.
-callback login(request(), State :: state()) ->
    {ok, dashboard_user_role(), Token :: binary()} | {error, Reason :: term()}.

%%------------------------------------------------------------------------------
%% Callback Interface
%%------------------------------------------------------------------------------
-spec hocon_ref(Mod :: module()) -> hocon_ref().
hocon_ref(Mod) ->
    Mod:hocon_ref().

-spec login_ref(Mod :: module()) -> hocon_ref().
login_ref(Mod) ->
    Mod:login_ref().

create(Mod, Config) ->
    Mod:create(Config).

update(Mod, Config, State) ->
    Mod:update(Config, State).

destroy(Mod, State) ->
    Mod:destroy(State).

login(Mod, Req, State) ->
    Mod:login(Req, State).

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
