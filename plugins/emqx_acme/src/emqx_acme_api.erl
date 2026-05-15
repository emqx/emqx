-module(emqx_acme_api).

-include("emqx_acme.hrl").

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
