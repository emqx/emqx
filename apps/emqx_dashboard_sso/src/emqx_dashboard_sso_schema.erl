%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% Hocon
-export([namespace/0, fields/1, desc/1]).

-export([
    common_backend_schema/1,
    backend_schema/1,
    username_password_schema/0
]).

-import(hoconsc, [ref/2, mk/2, enum/1]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------
namespace() -> dashboard.

fields(sso) ->
    lists:map(
        fun({Type, Module}) ->
            {Type,
                mk(
                    emqx_dashboard_sso:hocon_ref(Module),
                    #{required => {false, recursively}}
                )}
        end,
        maps:to_list(emqx_dashboard_sso:backends())
    ).

desc(sso) ->
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
                    %% importance => ?IMPORTANCE_NO_DOC,
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
