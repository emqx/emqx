%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_tls_client_hostname_check_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(ssl),
    Root = emqx_cth_tls:gen_cert(#{key => ec, issuer => root}),
    CAFile = filename:join(?config(priv_dir, Config), "ca.pem"),
    ok = emqx_cth_tls:write_pem(CAFile, element(1, Root)),
    [{root, Root}, {cacertfile, CAFile} | Config].

end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc """
A certificate without subjectAltName whose CN matches the server name is accepted
with `san_or_common_name`, and refused with the default `san_only`.
""".
t_no_san_cn_matches(Config) ->
    Server = start_server(gen_leaf(Config, "broker.example.com", #{}), Config),
    ?assertMatch(
        {error, {tls_alert, {bad_certificate, _}}},
        connect(Server, "broker.example.com", san_only, Config)
    ),
    ?assertMatch(
        {error, {tls_alert, {bad_certificate, _}}},
        connect(Server, "broker.example.com", undefined, Config)
    ),
    ?assertMatch({ok, _}, connect(Server, "broker.example.com", san_or_common_name, Config)),
    %% the match is case-insensitive
    ?assertMatch({ok, _}, connect(Server, "Broker.Example.COM", san_or_common_name, Config)).

-doc """
A certificate without subjectAltName whose CN does not match the server name is
refused with either setting.
""".
t_no_san_cn_mismatch(Config) ->
    Server = start_server(gen_leaf(Config, "other.example.com", #{}), Config),
    ?assertMatch(
        {error, {tls_alert, {bad_certificate, _}}},
        connect(Server, "broker.example.com", san_only, Config)
    ),
    ?assertMatch(
        {error, {tls_alert, {bad_certificate, _}}},
        connect(Server, "broker.example.com", san_or_common_name, Config)
    ).

-doc """
A wildcard CN matches one label on the left only, the way OTP 28.4 matched CNs.
""".
t_no_san_wildcard_cn(Config) ->
    Server = start_server(gen_leaf(Config, "*.example.com", #{}), Config),
    ?assertMatch({ok, _}, connect(Server, "broker.example.com", san_or_common_name, Config)),
    ?assertMatch(
        {error, {tls_alert, {bad_certificate, _}}},
        connect(Server, "a.broker.example.com", san_or_common_name, Config)
    ),
    ?assertMatch(
        {error, {tls_alert, {bad_certificate, _}}},
        connect(Server, "broker.example.com", san_only, Config)
    ).

-doc """
A certificate with a subjectAltName is matched against the SAN only, with either
setting: a matching SAN is accepted, and a CN that matches while the SAN does not
is refused.
""".
t_san_unaffected(Config) ->
    Exts = #{subject_alt_name => [{dns, "broker.example.com"}]},
    Server = start_server(gen_leaf(Config, "other.example.com", Exts), Config),
    ?assertMatch({ok, _}, connect(Server, "broker.example.com", san_only, Config)),
    ?assertMatch({ok, _}, connect(Server, "broker.example.com", san_or_common_name, Config)),
    ?assertMatch(
        {error, {tls_alert, {bad_certificate, _}}},
        connect(Server, "other.example.com", san_only, Config)
    ),
    ?assertMatch(
        {error, {tls_alert, {bad_certificate, _}}},
        connect(Server, "other.example.com", san_or_common_name, Config)
    ).

-doc """
Other verification failures are not masked: a SAN-less certificate from an
untrusted CA is refused even when its CN matches.
""".
t_untrusted_ca_not_masked(Config) ->
    OtherRoot = emqx_cth_tls:gen_cert(#{key => ec, issuer => root}),
    Leaf = emqx_cth_tls:gen_cert(#{
        key => ec,
        issuer => OtherRoot,
        subject => #{name => "broker.example.com"}
    }),
    Server = start_server(Leaf, Config),
    ?assertMatch(
        {error, {tls_alert, {unknown_ca, _}}},
        connect(Server, "broker.example.com", san_or_common_name, Config)
    ).

-doc """
The option only installs a verify fun with `verify_peer`, and `san_only` adds no
option at all.
""".
t_client_opts(_Config) ->
    Opts = #{enable => true, verify => verify_peer},
    Keys = fun(O) -> proplists:get_keys(emqx_tls_lib:to_client_opts(O)) end,
    ?assertNot(lists:member(verify_fun, Keys(Opts))),
    ?assertNot(lists:member(verify_fun, Keys(Opts#{hostname_check => san_only}))),
    ?assert(lists:member(verify_fun, Keys(Opts#{hostname_check => san_or_common_name}))),
    ?assertNot(
        lists:member(
            verify_fun,
            Keys(Opts#{verify => verify_none, hostname_check => san_or_common_name})
        )
    ),
    ?assertEqual(
        [],
        emqx_tls_lib:hostname_check_opts(#{verify => verify_peer, hostname_check => san_only})
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

gen_leaf(Config, CN, Exts) ->
    emqx_cth_tls:gen_cert(#{
        key => ec,
        issuer => ?config(root, Config),
        subject => #{name => CN},
        extensions => Exts
    }).

%% Accepts connections and completes the handshake until the test process exits.
start_server({{'Certificate', CertDer, _}, {KeyType, KeyDer, _}}, _Config) ->
    {ok, LSock} = ssl:listen(0, [
        {cert, CertDer},
        {key, {KeyType, KeyDer}},
        {reuseaddr, true},
        {active, false}
    ]),
    {ok, {_, Port}} = ssl:sockname(LSock),
    Parent = self(),
    Pid = spawn_link(fun() -> accept_loop(LSock, Parent) end),
    ok = ssl:controlling_process(LSock, Pid),
    Port.

accept_loop(LSock, Parent) ->
    case ssl:transport_accept(LSock) of
        {ok, TSock} ->
            _ = spawn(fun() ->
                MRef = monitor(process, Parent),
                _ = ssl:handshake(TSock, 5_000),
                receive
                    {'DOWN', MRef, process, _, _} -> ok
                after 5_000 -> ok
                end
            end),
            accept_loop(LSock, Parent);
        {error, _} ->
            ok
    end.

connect(Port, ServerName, HostnameCheck, Config) ->
    Opts0 = #{
        enable => true,
        verify => verify_peer,
        cacertfile => ?config(cacertfile, Config),
        server_name_indication => ServerName
    },
    Opts =
        case HostnameCheck of
            undefined -> Opts0;
            _ -> Opts0#{hostname_check => HostnameCheck}
        end,
    case ssl:connect("127.0.0.1", Port, emqx_tls_lib:to_client_opts(Opts), 5_000) of
        {ok, Sock} ->
            ok = ssl:close(Sock),
            {ok, Sock};
        Error ->
            Error
    end.
