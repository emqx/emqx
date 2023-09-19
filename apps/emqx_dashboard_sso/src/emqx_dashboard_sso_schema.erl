%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% Hocon
-export([namespace/0, roots/0, fields/1, tags/0, desc/1]).
-export([
    common_backend_schema/1,
    backend_schema/1,
    username_password_schema/0
]).
-import(hoconsc, [ref/2, mk/2, enum/1]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------
namespace() -> dashboard_sso.

tags() ->
    [<<"Dashboard Single Sign-On">>].

roots() -> [dashboard_sso].

fields(dashboard_sso) ->
    lists:map(
        fun({Type, Module}) ->
            {Type, mk(Module:hocon_ref(), #{required => {false, recursively}})}
        end,
        maps:to_list(emqx_dashboard_sso:backends())
    ).

desc(dashboard_sso) ->
    "Dashboard Single Sign-On";
desc(_) ->
    undefined.

-spec common_backend_schema(list(atom())) -> proplists:proplist().
common_backend_schema(Backend) ->
    [
        {enable,
            mk(
                boolean(), #{
                    desc => ?DESC(backend_enable),
                    required => false,
                    default => false
                }
            )},
        backend_schema(Backend)
    ].

backend_schema(Backend) ->
    {backend,
        mk(enum(Backend), #{
            required => true,
            desc => ?DESC(backend)
        })}.

username_password_schema() ->
    [
        {username,
            mk(
                binary(),
                #{
                    desc => ?DESC(username),
                    'maxLength' => 100,
                    example => <<"admin">>
                }
            )},
        {password,
            mk(
                binary(),
                #{
                    desc => ?DESC(password),
                    'maxLength' => 100,
                    example => <<"public">>
                }
            )}
    ].
