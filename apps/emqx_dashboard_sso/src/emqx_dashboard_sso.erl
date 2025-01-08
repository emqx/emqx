%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    login/3,
    convert_certs/3
]).

-export([types/0, modules/0, provider/1, backends/0, format/1]).

%%------------------------------------------------------------------------------
%% Callbacks
%%------------------------------------------------------------------------------
-type request() :: map().
-type parsed_config() :: #{
    backend => atom(),
    atom() => term()
}.

%% Note: if a backend has a resource, it must be stored in the state and named resource_id
-type state() :: #{resource_id => binary(), atom() => term()}.
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
    {ok, dashboard_user_role(), Token :: binary()}
    | {redirect, tuple()}
    | {error, Reason :: term()}.

-callback convert_certs(
    Dir :: file:filename_all(),
    config()
) -> config().

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

convert_certs(Mod, Dir, Config) ->
    Mod:convert_certs(Dir, Config).

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
    #{
        ldap => emqx_dashboard_sso_ldap,
        saml => emqx_dashboard_sso_saml,
        oidc => emqx_dashboard_sso_oidc
    }.

format(Args) ->
    lists:foldl(fun combine/2, <<>>, Args).

combine(Arg, Bin) when is_binary(Arg) ->
    <<Bin/binary, Arg/binary>>;
combine(Arg, Bin) when is_list(Arg) ->
    case io_lib:printable_unicode_list(Arg) of
        true ->
            ArgBin = unicode:characters_to_binary(Arg),
            <<Bin/binary, ArgBin/binary>>;
        _ ->
            generic_combine(Arg, Bin)
    end;
combine(Arg, Bin) ->
    generic_combine(Arg, Bin).

generic_combine(Arg, Bin) ->
    Str = io_lib:format("~0p", [Arg]),
    erlang:iolist_to_binary([Bin, Str]).
