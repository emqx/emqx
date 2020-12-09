-module(http_auth_server).

-export([ start/2
        , stop/0
        ]).

-define(SUPERUSER, [[{"username", "superuser"}, {"clientid", "superclient"}]]).

-define(ACL, [[{<<"username">>, <<"testuser">>},
               {<<"clientid">>, <<"client1">>},
               {<<"access">>, <<"1">>},
               {<<"topic">>, <<"users/testuser/1">>},
               {<<"ipaddr">>, <<"127.0.0.1">>},
               {<<"mountpoint">>, <<"null">>}],
              [{<<"username">>, <<"xyz">>},
               {<<"clientid">>, <<"client2">>},
               {<<"access">>, <<"2">>},
               {<<"topic">>, <<"a/b/c">>},
               {<<"ipaddr">>, <<"192.168.1.3">>},
               {<<"mountpoint">>, <<"null">>}],
              [{<<"username">>, <<"testuser1">>},
               {<<"clientid">>, <<"client1">>},
               {<<"access">>, <<"2">>},
               {<<"topic">>, <<"topic">>},
               {<<"ipaddr">>, <<"127.0.0.1">>},
               {<<"mountpoint">>, <<"null">>}],
              [{<<"username">>, <<"testuser2">>},
               {<<"clientid">>, <<"client2">>},
               {<<"access">>, <<"1">>},
               {<<"topic">>, <<"topic">>},
               {<<"ipaddr">>, <<"127.0.0.1">>},
               {<<"mountpoint">>, <<"null">>}]]).

-define(AUTH, [[{<<"clientid">>, <<"client1">>},
                {<<"username">>, <<"testuser1">>},
                {<<"password">>, <<"pass1">>}],
               [{<<"clientid">>, <<"client2">>},
                {<<"username">>, <<"testuser2">>},
                {<<"password">>, <<"pass2">>}]]).

%%------------------------------------------------------------------------------
%% REST Interface
%%------------------------------------------------------------------------------

-rest_api(#{ name   => auth
           , method => 'GET'
           , path   => "/mqtt/auth"
           , func   => authenticate
           , descr  => "Authenticate user access permission"
           }).

-rest_api(#{ name   => is_superuser
           , method => 'GET'
           , path   => "/mqtt/superuser"
           , func   => is_superuser
           , descr  => "Is super user"
           }).

-rest_api(#{ name   => acl
           , method => 'GET'
           , path   => "/mqtt/acl"
           , func   => check_acl
           , descr  => "Check acl"
           }).

-rest_api(#{ name   => auth
           , method => 'POST'
           , path   => "/mqtt/auth"
           , func   => authenticate
           , descr  => "Authenticate user access permission"
           }).

-rest_api(#{ name   => is_superuser
           , method => 'POST'
           , path   => "/mqtt/superuser"
           , func   => is_superuser
           , descr  => "Is super user"
           }).

-rest_api(#{ name   => acl
           , method => 'POST'
           , path   => "/mqtt/acl"
           , func   => check_acl
           , descr  => "Check acl"
           }).

-export([ authenticate/2
        , is_superuser/2
        , check_acl/2
        ]).

authenticate(_Binding, Params) ->
    return(check(Params, ?AUTH)).

is_superuser(_Binding, Params) ->
    return(check(Params, ?SUPERUSER)).

check_acl(_Binding, Params) ->
    return(check(Params, ?ACL)).

return(allow) -> {200, <<"allow">>};
return(deny) -> {400, <<"deny">>}.

start(http, Inet) ->
    application:ensure_all_started(minirest),
    Handlers = [{"/", minirest:handler(#{modules => [?MODULE]})}],
    Dispatch = [{"/[...]", minirest, Handlers}],
    minirest:start_http(http_auth_server, #{socket_opts => [Inet, {port, 8991}]}, Dispatch);

start(https, Inet) ->
    application:ensure_all_started(minirest),
    Handlers = [{"/", minirest:handler(#{modules => [?MODULE]})}],
    Dispatch = [{"/[...]", minirest, Handlers}],
    minirest:start_https(http_auth_server, #{socket_opts => [Inet, {port, 8991} | certopts()]}, Dispatch).

%% @private
certopts() ->
    Certfile = filename:join(["etc", "certs", "cert.pem"]),
    Keyfile = filename:join(["etc", "certs", "key.pem"]),
    CaCert = filename:join(["etc", "certs", "cacert.pem"]),
    [{verify, verify_peer},
     {certfile, emqx_ct_helpers:deps_path(emqx, Certfile)},
     {keyfile, emqx_ct_helpers:deps_path(emqx, Keyfile)},
     {cacertfile, emqx_ct_helpers:deps_path(emqx, CaCert)}] ++ emqx_ct_helpers:client_ssl().

stop() ->
    minirest:stop_http(http_auth_server).

-spec check(HttpReqParams :: list(), DefinedConf :: list()) -> allow | deny.
check(_Params, []) ->
    %ct:pal("check auth_result: deny~n"),
    deny;
check(Params, [ConfRecord|T]) ->
    % ct:pal("Params: ~p, ConfRecord:~p ~n", [Params, ConfRecord]),
    case match_config(Params, ConfRecord) of
        not_match ->
            check(Params, T);
        matched -> allow
     end.

match_config([], _ConfigColumn) ->
    %ct:pal("match_config auth_result: matched~n"),
    matched;

match_config([Param|T], ConfigColumn) ->
    %ct:pal("Param: ~p, ConfigColumn:~p ~n", [Param, ConfigColumn]),
    case lists:member(Param, ConfigColumn) of
        true ->
            match_config(T, ConfigColumn);
        false ->
           not_match
    end.
