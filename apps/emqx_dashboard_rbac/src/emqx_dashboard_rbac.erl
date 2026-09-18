%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    check_rbac/3,
    role/1,
    valid_dashboard_role/1,
    valid_api_role/1
]).

-dialyzer({nowarn_function, role/1}).
%%=====================================================================
%% API
check_rbac(Req, Username, Extra) ->
    Role = role(Extra),
    Method = cowboy_req:method(Req),
    case relative_path(Req) of
        {ok, Path} ->
            check_rbac(Role, Method, Path, Username, Req);
        _ ->
            false
    end.

%% `cowboy_req:path/1' is the raw request target, while `cowboy_router' dispatches
%% on the percent-decoded, dot-segment-collapsed path (`cowboy_router:split_path/1')
%% and does not expose that path back to the request. Resolve it the same way
%% before matching, otherwise an equivalent alias such as `/%63onfigs' would reach
%% the same handler while missing a rule here.
%%
%% `uri_string:normalize/1' decodes the unreserved percent-escapes and removes the
%% dot segments exactly like the router, but it keeps the trailing empty segment
%% that the router drops, hence `without_trailing_slashes/1'.
relative_path(Req) ->
    RawPath = cowboy_req:path(Req),
    Path =
        try
            uri_string:normalize(RawPath)
        catch
            _:_ -> RawPath
        end,
    case emqx_dashboard_swagger:get_relative_uri(Path) of
        {ok, RelPath} -> {ok, without_trailing_slashes(RelPath)};
        Error -> Error
    end.

%% `cowboy_router:split_path/1' ignores the trailing empty segment, so
%% `/configs/' and `/configs/.' reach the same handler as `/configs'.
without_trailing_slashes(<<>>) ->
    <<>>;
without_trailing_slashes(Path) ->
    case binary:last(Path) of
        $/ -> without_trailing_slashes(binary:part(Path, 0, byte_size(Path) - 1));
        _ -> Path
    end.

%% For compatibility
role(#?ADMIN{role = undefined}) ->
    ?ROLE_SUPERUSER;
role(#?ADMIN{role = Role}) ->
    Role;
%% For compatibility
role([]) ->
    ?ROLE_SUPERUSER;
role(#{role := Role}) ->
    Role;
role(Role) when is_binary(Role) ->
    Role.

valid_dashboard_role(Role) ->
    valid_role(dashboard, Role).

valid_api_role(Role) ->
    valid_role(api, Role).

%% ===================================================================

valid_role(Type, Role) ->
    case lists:member(Role, role_list(Type)) of
        true ->
            ok;
        _ ->
            {error, <<"Role does not exist">>}
    end.

%% ===================================================================
check_rbac(?ROLE_SUPERUSER, _, _, _, _) ->
    true;
%% `GET /configs' negotiated to `text/plain' is the cleartext HOCON dump which
%% pairs with `PUT /configs' (export/reload), so it is reserved to administrators.
%% The JSON variant of the same endpoint is redacted and stays available to viewers.
check_rbac(?ROLE_VIEWER, <<"GET">>, <<"/configs">>, _Username, Req) ->
    not wants_plaintext_config_dump(Req);
check_rbac(?ROLE_VIEWER, <<"GET">>, _, _, _) ->
    true;
check_rbac(?ROLE_API_PUBLISHER, <<"POST">>, <<"/publish">>, _, _) ->
    true;
check_rbac(?ROLE_API_PUBLISHER, <<"POST">>, <<"/publish/bulk">>, _, _) ->
    true;
%% everyone should allow to logout
check_rbac(?ROLE_VIEWER, <<"POST">>, <<"/logout">>, _, _) ->
    true;
%% viewer should allow to change self password and (re)setup multi-factor auth for self,
%% superuser should allow to change any user
check_rbac(?ROLE_VIEWER, <<"POST">>, <<"/users/", SubPath/binary>>, Username, _) ->
    case binary:split(SubPath, <<"/">>, [global]) of
        [Username, <<"change_pwd">>] -> true;
        [Username, <<"mfa">>] -> true;
        _ -> false
    end;
check_rbac(?ROLE_VIEWER, <<"DELETE">>, <<"/users/", SubPath/binary>>, Username) ->
    case binary:split(SubPath, <<"/">>, [global]) of
        [Username, <<"mfa">>] -> true;
        _ -> false
    end;
check_rbac(_, _, _, _, _) ->
    false.

%% Keeps in sync with the `Accept' negotiation in
%% `emqx_mgmt_api_configs:configs/3': it prefers `text/plain', and a missing
%% `Accept' header (or `*/*') resolves to the first preference, so both count
%% as a request for the plaintext HOCON dump.
wants_plaintext_config_dump(Req) ->
    Accept = cowboy_req:header(<<"accept">>, Req, <<"*/*">>),
    Accepts = [
        begin
            [T | _] = binary:split(string:trim(S), <<";">>),
            T
        end
     || S <- re:split(Accept, ",")
    ],
    lists:member(<<"*/*">>, Accepts) orelse lists:member(<<"text/plain">>, Accepts).

role_list(dashboard) ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER];
role_list(api) ->
    [?ROLE_API_VIEWER, ?ROLE_API_PUBLISHER, ?ROLE_API_SUPERUSER].

%% ===================================================================
%% Unit tests
%% ===================================================================
-ifdef(TEST).

wants_plaintext_config_dump_test_() ->
    [
        %% No Accept header (or `*/*') negotiates to the first preference of
        %% `GET /configs', which is `text/plain'.
        ?_assert(wants_plaintext_config_dump(fake_req(<<"GET">>, <<"/configs">>))),
        ?_assert(wants_plaintext_config_dump(fake_req(<<"GET">>, <<"/configs">>, <<"*/*">>))),
        ?_assert(
            wants_plaintext_config_dump(fake_req(<<"GET">>, <<"/configs">>, <<"text/plain">>))
        ),
        ?_assert(
            wants_plaintext_config_dump(
                fake_req(<<"GET">>, <<"/configs">>, <<"application/json, */*;q=0.8">>)
            )
        ),
        ?_assert(
            wants_plaintext_config_dump(
                fake_req(<<"GET">>, <<"/configs">>, <<"text/html, text/plain;q=0.9">>)
            )
        ),
        ?_assertNot(
            wants_plaintext_config_dump(
                fake_req(<<"GET">>, <<"/configs">>, <<"application/json">>)
            )
        ),
        ?_assertNot(
            wants_plaintext_config_dump(
                fake_req(<<"GET">>, <<"/configs">>, <<"application/xml">>)
            )
        )
    ].

check_rbac_configs_test_() ->
    Get = <<"GET">>,
    Put = <<"PUT">>,
    User = <<"u">>,
    [
        %% The administrator keeps the cleartext dump: it is the export half of
        %% the `GET /configs' -> `PUT /configs' (export/reload) workflow.
        ?_assert(check_rbac(?ROLE_SUPERUSER, Get, <<"/configs">>, User, plaintext_req())),
        ?_assert(check_rbac(?ROLE_SUPERUSER, Get, <<"/configs">>, User, no_accept_req())),
        ?_assert(check_rbac(?ROLE_SUPERUSER, Get, <<"/configs">>, User, json_req())),
        ?_assert(check_rbac(?ROLE_SUPERUSER, Get, <<"/other">>, User, plaintext_req())),
        ?_assert(check_rbac(?ROLE_SUPERUSER, Put, <<"/configs">>, User, plaintext_req())),
        %% A viewer is denied the cleartext dump only; the redacted JSON path and
        %% the other endpoints are untouched.
        ?_assertNot(check_rbac(?ROLE_VIEWER, Get, <<"/configs">>, User, plaintext_req())),
        ?_assertNot(check_rbac(?ROLE_VIEWER, Get, <<"/configs">>, User, no_accept_req())),
        ?_assertNot(
            check_rbac(
                ?ROLE_VIEWER,
                Get,
                <<"/configs">>,
                User,
                fake_req(<<"GET">>, <<"/configs">>, <<"application/json, */*;q=0.8">>)
            )
        ),
        ?_assert(check_rbac(?ROLE_VIEWER, Get, <<"/configs">>, User, json_req())),
        ?_assert(check_rbac(?ROLE_VIEWER, Get, <<"/other">>, User, plaintext_req())),
        ?_assertNot(check_rbac(?ROLE_VIEWER, Put, <<"/configs">>, User, plaintext_req())),
        %% The new clause must narrow the viewer only: no other role gains access
        %% to `GET /configs' by matching it.
        ?_assertNot(check_rbac(?ROLE_API_PUBLISHER, Get, <<"/configs">>, User, plaintext_req())),
        ?_assertNot(check_rbac(?ROLE_API_PUBLISHER, Get, <<"/configs">>, User, no_accept_req())),
        ?_assertNot(check_rbac(?ROLE_API_PUBLISHER, Get, <<"/configs">>, User, json_req())),
        ?_assertNot(check_rbac(?ROLE_API_PUBLISHER, Get, <<"/other">>, User, plaintext_req()))
    ].

plaintext_req() ->
    fake_req(<<"GET">>, <<"/configs">>, <<"text/plain">>).

no_accept_req() ->
    fake_req(<<"GET">>, <<"/configs">>).

json_req() ->
    fake_req(<<"GET">>, <<"/configs">>, <<"application/json">>).

fake_req(Method, Path) ->
    fake_req(Method, Path, undefined).

fake_req(Method, Path, Accept) ->
    Headers =
        case Accept of
            undefined -> #{};
            _ -> #{<<"accept">> => Accept}
        end,
    #{method => Method, path => <<"/api/v5", Path/binary>>, headers => Headers}.

%% The exported entry point must evaluate the path `cowboy_router' matched, so a
%% percent-encoded or dot-segment alias of `/configs' must not bypass the rule.
check_rbac_path_aliases_test_() ->
    Viewer = #{role => ?ROLE_VIEWER},
    [
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/%63onfigs">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/conf%69gs">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/./configs">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/x/../configs">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        %% `cowboy_router' drops the trailing empty segment as well.
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/configs/">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/configs/.">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        %% The redacted JSON variant stays reachable through the same alias.
        ?_assert(
            check_rbac(
                fake_req(<<"GET">>, <<"/%63onfigs">>, <<"application/json">>), <<"u">>, Viewer
            )
        )
    ].

-endif.
