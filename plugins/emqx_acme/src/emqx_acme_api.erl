-module(emqx_acme_api).

-include("emqx_acme.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-export([handle/3]).

-define(JSON_HDR, #{<<"content-type">> => <<"application/json">>}).
-define(HTML_HDR, #{
    <<"content-type">> => <<"text/html; charset=utf-8">>,
    <<"cache-control">> => <<"no-store, no-cache, must-revalidate">>,
    <<"pragma">> => <<"no-cache">>,
    <<"expires">> => <<"0">>
}).

handle(get, [<<"status">>], _Request) ->
    case emqx_acme_issuer:status() of
        {error, Reason} ->
            leader_error(Reason);
        Status when is_map(Status) ->
            {ok, 200, ?JSON_HDR, Status}
    end;
handle(get, [<<"context">>], _Request) ->
    %% Used by the plugin UI to render the "Disable Dashboard HTTP" button
    %% and display the live HTTP/HTTPS bind ports.
    %% The plugin API gateway sanitizes the Cowboy request and does not
    %% pass through scheme/host, so the *frontend* is the source of truth
    %% for "am I currently on HTTPS?" via window.location.protocol.
    {ok, 200, ?JSON_HDR, #{
        dashboard_http_listener => listener_running(http),
        dashboard_https_listener => listener_running(https),
        dashboard_http_port => listener_port(http),
        dashboard_https_port => listener_port(https)
    }};
handle(get, [<<"ui">>], _Request) ->
    {ok, 200, ?HTML_HDR, ui_html()};
handle(post, [<<"issue">>], _Request) ->
    handle_async_kickoff(fun emqx_acme_issuer:issue/0);
handle(post, [<<"renew">>], _Request) ->
    handle_async_kickoff(fun emqx_acme_issuer:renew/0);
handle(post, [<<"disable_dashboard_http">>], _Request) ->
    %% Server-side guard: refuse if no HTTPS listener exists, since that's
    %% the only check we can do reliably (the plugin API gateway strips the
    %% Cowboy request's scheme). The UI handles the "you're currently on
    %% HTTP" case by greying out the button.
    do_disable_dashboard_http();
handle(get, [<<"listeners">>], _Request) ->
    {ok, 200, ?JSON_HDR, #{listeners => list_eligible_listeners()}};
handle(post, [<<"apply_listener">>], Request) ->
    Body = maps:get(body, Request, #{}),
    case maps:get(<<"id">>, Body, undefined) of
        Id when is_binary(Id) ->
            do_apply_listener(Id);
        _ ->
            {error, 400, ?JSON_HDR, #{
                code => <<"BAD_REQUEST">>,
                message => <<"missing required field: id">>
            }}
    end;
handle(_Method, _Path, _Request) ->
    {error, 404, ?JSON_HDR, #{
        code => <<"NOT_FOUND">>,
        message => <<"Endpoint not found">>
    }}.

%% /issue and /renew kick off background workers — the gen_server call
%% is short, so the plugin-API gateway's 5-second budget never trips
%% and the dashboard never sees `plugin_api_callback_crash`. Outcome is
%% surfaced via `/status` (the UI polls it).
handle_async_kickoff(Fun) ->
    case Fun() of
        {ok, started} ->
            {ok, 202, ?JSON_HDR, #{result => started}};
        {error, {already_running, Action}} ->
            {error, 409, ?JSON_HDR, #{
                code => <<"ALREADY_RUNNING">>,
                message => iolist_to_binary(
                    io_lib:format("Another ~p is already running", [Action])
                )
            }};
        {error, Reason} ->
            leader_error(Reason)
    end.

%% A 503 is the right shape for "cluster member can't be reached right
%% now" — the operator should retry; nothing about the request is wrong.
leader_error(leader_unavailable) ->
    {error, 503, ?JSON_HDR, #{
        code => <<"LEADER_UNAVAILABLE">>,
        message => <<"ACME leader node is not reachable; try again shortly">>
    }};
leader_error(no_core_nodes) ->
    {error, 503, ?JSON_HDR, #{
        code => <<"NO_CORE_NODES">>,
        message => <<"No core node is currently running">>
    }}.

do_disable_dashboard_http() ->
    case emqx_conf:get([dashboard, listeners, https], undefined) of
        undefined ->
            {error, 409, ?JSON_HDR, #{
                code => <<"NO_HTTPS_LISTENER">>,
                message => <<"Cannot disable HTTP: no dashboard HTTPS listener is configured">>
            }};
        _ ->
            %% The dashboard listener config validator uses bind = 0 as the
            %% "disabled" sentinel — schema-rejected `null`/tombstone, but
            %% bind:0 is accepted and the dashboard's diff_listeners stops
            %% the running listener.
            Update = #{<<"listeners">> => #{<<"http">> => #{<<"bind">> => 0}}},
            case emqx_conf:update([dashboard], Update, #{override_to => cluster}) of
                {ok, _} ->
                    ?LOG(info, #{msg => "dashboard_http_listener_disabled"}),
                    {ok, 200, ?JSON_HDR, #{result => ok}};
                {error, Reason} ->
                    ?LOG(error, #{msg => "dashboard_http_disable_failed", reason => Reason}),
                    {error, 500, ?JSON_HDR, #{
                        code => <<"DISABLE_FAILED">>,
                        message => iolist_to_binary(io_lib:format("~p", [Reason]))
                    }}
            end
    end.

%% A dashboard listener is considered "running" if it has a config block
%% and bind != 0 (the dashboard's "disabled" sentinel).
listener_running(Type) ->
    case emqx_conf:get([dashboard, listeners, Type], undefined) of
        undefined -> false;
        #{bind := 0} -> false;
        #{bind := {_, 0}} -> false;
        _ -> true
    end.

%% The port the dashboard listener is bound to, or null if no listener
%% block exists / the listener is administratively disabled (bind = 0).
%% The dashboard schema accepts bind as either a port int or an
%% {Address, Port} tuple, so handle both shapes.
listener_port(Type) ->
    case emqx_conf:get([dashboard, listeners, Type], undefined) of
        undefined -> null;
        #{bind := 0} -> null;
        #{bind := {_, 0}} -> null;
        #{bind := Port} when is_integer(Port) -> Port;
        #{bind := {_, Port}} when is_integer(Port) -> Port;
        _ -> null
    end.

ui_html() ->
    maybe
        Dir = code:priv_dir(emqx_acme),
        true ?= is_list(Dir),
        {ok, Bin} ?= file:read_file(filename:join(Dir, "ui.html")),
        Bin
    else
        _ ->
            <<
                "<!doctype html><html><body><h1>ACME plugin UI unavailable</h1>",
                "<p>Missing priv/ui.html</p></body></html>"
            >>
    end.

%%--------------------------------------------------------------------
%% /listeners — listener enumeration + bundle-binding state
%%--------------------------------------------------------------------

%% Enumerate ssl/wss listeners from the global runtime config and the
%% synthetic "dashboard:https" entry. `bound_to_bundle` is true iff the
%% listener's current cert wiring matches the plugin's cert_bundle_name
%% (for ssl/wss: ssl_options.managed_certs.bundle_name; for dashboard:
%% ssl_options.certfile == bundle chain path).
list_eligible_listeners() ->
    Settings = emqx_acme_config:settings(),
    BundleName = maps:get(cert_bundle_name, Settings),
    BundlePaths = bundle_paths(BundleName),
    SslWss = lists:flatmap(
        fun(Type) -> describe_listeners_of_type(Type, BundleName) end,
        [ssl, wss]
    ),
    Dashboard = describe_dashboard_https(BundlePaths),
    SslWss ++ [Dashboard].

describe_listeners_of_type(Type, BundleName) ->
    case emqx:get_config([listeners, Type], #{}) of
        Map when is_map(Map) ->
            [
                describe_one_listener(Type, Name, Conf, BundleName)
             || {Name, Conf} <- maps:to_list(Map)
            ];
        _ ->
            []
    end.

describe_one_listener(Type, Name, Conf, BundleName) ->
    %% bind is schema-typed ip_port() — either a bare integer port or
    %% {inet:ip_address(), Port}. enable is bool. managed_certs.bundle_name,
    %% when present, is a binary from the HOCON schema.
    #{bind := Bind, enable := Enabled, ssl_options := SslOpts} = Conf,
    Bound = ssl_bundle_name(SslOpts),
    #{
        id => listener_id(Type, Name),
        kind => atom_to_binary(Type),
        label => type_label(Type),
        bind => format_bind(Bind),
        enabled => Enabled,
        bound_to_bundle => Bound =:= BundleName,
        current_bundle => Bound
    }.

describe_dashboard_https(BundlePaths) ->
    case emqx:get_config([dashboard, listeners, https], undefined) of
        undefined ->
            #{
                id => <<"dashboard:https">>,
                kind => <<"dashboard_https">>,
                label => <<"Dashboard HTTPS">>,
                bind => null,
                enabled => false,
                bound_to_bundle => false,
                current_bundle => null,
                configured => false
            };
        #{bind := Bind, ssl_options := #{certfile := CertFile}} ->
            BoundToBundle =
                case BundlePaths of
                    {ok, #{chain := ChainPath}} ->
                        iolist_to_binary(CertFile) =:= iolist_to_binary(ChainPath);
                    _ ->
                        false
                end,
            #{
                id => <<"dashboard:https">>,
                kind => <<"dashboard_https">>,
                label => <<"Dashboard HTTPS">>,
                bind => format_bind(Bind),
                enabled => is_dashboard_enabled(Bind),
                bound_to_bundle => BoundToBundle,
                current_bundle => null,
                configured => true
            }
    end.

bundle_paths(BundleName) ->
    case emqx_managed_certs:list_managed_files(?global_ns, BundleName) of
        {ok, #{?FILE_KIND_CHAIN := #{path := ChainPath}, ?FILE_KIND_KEY := #{path := KeyPath}}} ->
            {ok, #{chain => ChainPath, key => KeyPath}};
        _ ->
            {error, bundle_empty}
    end.

ssl_bundle_name(#{managed_certs := #{bundle_name := B}}) when is_binary(B) -> B;
ssl_bundle_name(_) -> null.

listener_id(Type, Name) ->
    iolist_to_binary([atom_to_binary(Type), ":", atom_to_binary(Name)]).

type_label(ssl) -> <<"MQTT/TLS">>;
type_label(wss) -> <<"MQTT/WebSocket/TLS">>.

%% Dashboard listener uses bind=0 as the "administratively disabled"
%% sentinel — same convention as listener_running/1 above.
is_dashboard_enabled(0) -> false;
is_dashboard_enabled({_, 0}) -> false;
is_dashboard_enabled(_) -> true.

%% bind is the schema's ip_port(): bare integer = INADDR_ANY:Port, or
%% {inet:ip_address(), Port} where the address tuple is 4-tuple (v4) or
%% 8-tuple (v6). inet:ntoa/1 formats both correctly (v6 includes :: shorthand).
format_bind(Port) when is_integer(Port) ->
    iolist_to_binary([<<"0.0.0.0:">>, integer_to_binary(Port)]);
format_bind({Addr, Port}) when is_tuple(Addr), is_integer(Port) ->
    iolist_to_binary([inet:ntoa(Addr), $:, integer_to_binary(Port)]).

%%--------------------------------------------------------------------
%% /apply_listener — bind one listener to the plugin's bundle
%%--------------------------------------------------------------------

%% Dispatch on the id's "kind:name" prefix. The plugin's bundle name and
%% dashboard_https_port come from the persisted plugin config, not from
%% the request — the UI doesn't (and shouldn't) parameterise either.
do_apply_listener(<<"dashboard:https">>) ->
    Settings = emqx_acme_config:settings(),
    BundleName = maps:get(cert_bundle_name, Settings),
    Port = maps:get(dashboard_https_port, Settings, ?DEFAULT_DASHBOARD_HTTPS_PORT),
    case bundle_paths(BundleName) of
        {ok, _} ->
            case emqx_acme_issuer:enable_dashboard_https(BundleName, Port) of
                ok -> {ok, 200, ?JSON_HDR, #{result => ok}};
                {error, Reason} -> apply_error(Reason)
            end;
        {error, bundle_empty} ->
            no_cert_error()
    end;
do_apply_listener(Id) ->
    case parse_ssl_wss_id(Id) of
        {ok, Type, Name} ->
            apply_ssl_wss(Type, Name, Id);
        {error, not_found} ->
            listener_not_found_error(Id);
        {error, bad_format} ->
            {error, 400, ?JSON_HDR, #{
                code => <<"BAD_REQUEST">>,
                message => <<"unknown listener id">>
            }}
    end.

apply_ssl_wss(Type, Name, Id) ->
    Settings = emqx_acme_config:settings(),
    BundleName = maps:get(cert_bundle_name, Settings),
    case bundle_paths(BundleName) of
        {ok, _} ->
            %% migrate_one_listener/3 is shared with the iteration-driven
            %% first-issuance path and treats not_found as ok-and-skip
            %% (so listing a not-yet-created listener in config doesn't
            %% fail issuance). For the imperative single-listener API
            %% the operator named the listener explicitly, so a missing
            %% one is almost certainly a typo — check before dispatching
            %% to surface a 404 instead of a misleading 200.
            case emqx_config:find_listener_conf(Type, Name, []) of
                {ok, _} ->
                    case emqx_acme_issuer:migrate_one_listener(BundleName, Type, Name) of
                        ok -> {ok, 200, ?JSON_HDR, #{result => ok}};
                        {error, Reason} -> apply_error(Reason)
                    end;
                {not_found, _, _} ->
                    listener_not_found_error(Id)
            end;
        {error, bundle_empty} ->
            no_cert_error()
    end.

%% Only ssl/wss are accepted here — tcp/ws/quic have no cert to wire up,
%% and the dashboard https case is dispatched separately above.
%% binary_to_existing_atom (not binary_to_atom) on the name guards the
%% atom table — an unauth'd PUT with arbitrary garbage in the name field
%% cannot leak atoms. For a real listener the atom was interned when
%% config was loaded; a missing one is reported as not_found.
parse_ssl_wss_id(Id) ->
    case binary:split(Id, <<":">>) of
        [<<"ssl">>, Name] -> name_to_atom(ssl, Name);
        [<<"wss">>, Name] -> name_to_atom(wss, Name);
        _ -> {error, bad_format}
    end.

name_to_atom(Type, Name) ->
    try
        {ok, Type, binary_to_existing_atom(Name)}
    catch
        error:badarg -> {error, not_found}
    end.

listener_not_found_error(Id) ->
    {error, 404, ?JSON_HDR, #{
        code => <<"LISTENER_NOT_FOUND">>,
        message => iolist_to_binary(io_lib:format("Listener ~s not found", [Id]))
    }}.

no_cert_error() ->
    {error, 409, ?JSON_HDR, #{
        code => <<"NO_CERTIFICATE">>,
        message => <<"No certificate has been issued for this bundle yet">>
    }}.

apply_error(Reason) ->
    {error, 500, ?JSON_HDR, #{
        code => <<"APPLY_FAILED">>,
        message => iolist_to_binary(io_lib:format("~p", [Reason]))
    }}.
