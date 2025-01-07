%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_gateway_conf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_gateway_test_utils,
    [
        assert_confs/2,
        maybe_unconvert_listeners/1
    ]
).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf, emqx_auth, emqx_auth_mnesia, emqx_gateway],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_CaseName, Conf) ->
    _ = emqx_gateway_conf:unload_gateway(stomp),
    ct:sleep(500),
    Conf.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

-define(SVR_CA, <<
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV\n"
    "BAYTAkNOMREwDwYDVQQIDAhoYW5nemhvdTEMMAoGA1UECgwDRU1RMQ8wDQYDVQQD\n"
    "DAZSb290Q0EwHhcNMjAwNTA4MDgwNjUyWhcNMzAwNTA2MDgwNjUyWjA/MQswCQYD\n"
    "VQQGEwJDTjERMA8GA1UECAwIaGFuZ3pob3UxDDAKBgNVBAoMA0VNUTEPMA0GA1UE\n"
    "AwwGUm9vdENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzcgVLex1\n"
    "EZ9ON64EX8v+wcSjzOZpiEOsAOuSXOEN3wb8FKUxCdsGrsJYB7a5VM/Jot25Mod2\n"
    "juS3OBMg6r85k2TWjdxUoUs+HiUB/pP/ARaaW6VntpAEokpij/przWMPgJnBF3Ur\n"
    "MjtbLayH9hGmpQrI5c2vmHQ2reRZnSFbY+2b8SXZ+3lZZgz9+BaQYWdQWfaUWEHZ\n"
    "uDaNiViVO0OT8DRjCuiDp3yYDj3iLWbTA/gDL6Tf5XuHuEwcOQUrd+h0hyIphO8D\n"
    "tsrsHZ14j4AWYLk1CPA6pq1HIUvEl2rANx2lVUNv+nt64K/Mr3RnVQd9s8bK+TXQ\n"
    "KGHd2Lv/PALYuwIDAQABo1AwTjAdBgNVHQ4EFgQUGBmW+iDzxctWAWxmhgdlE8Pj\n"
    "EbQwHwYDVR0jBBgwFoAUGBmW+iDzxctWAWxmhgdlE8PjEbQwDAYDVR0TBAUwAwEB\n"
    "/zANBgkqhkiG9w0BAQsFAAOCAQEAGbhRUjpIred4cFAFJ7bbYD9hKu/yzWPWkMRa\n"
    "ErlCKHmuYsYk+5d16JQhJaFy6MGXfLgo3KV2itl0d+OWNH0U9ULXcglTxy6+njo5\n"
    "CFqdUBPwN1jxhzo9yteDMKF4+AHIxbvCAJa17qcwUKR5MKNvv09C6pvQDJLzid7y\n"
    "E2dkgSuggik3oa0427KvctFf8uhOV94RvEDyqvT5+pgNYZ2Yfga9pD/jjpoHEUlo\n"
    "88IGU8/wJCx3Ds2yc8+oBg/ynxG8f/HmCC1ET6EHHoe2jlo8FpU/SgGtghS1YL30\n"
    "IWxNsPrUP+XsZpBJy/mvOhE5QXo6Y35zDqqj8tI7AGmAWu22jg==\n"
    "-----END CERTIFICATE-----\n"
>>).

-define(SVR_CERT, <<
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDEzCCAfugAwIBAgIBAjANBgkqhkiG9w0BAQsFADA/MQswCQYDVQQGEwJDTjER\n"
    "MA8GA1UECAwIaGFuZ3pob3UxDDAKBgNVBAoMA0VNUTEPMA0GA1UEAwwGUm9vdENB\n"
    "MB4XDTIwMDUwODA4MDcwNVoXDTMwMDUwNjA4MDcwNVowPzELMAkGA1UEBhMCQ04x\n"
    "ETAPBgNVBAgMCGhhbmd6aG91MQwwCgYDVQQKDANFTVExDzANBgNVBAMMBlNlcnZl\n"
    "cjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALNeWT3pE+QFfiRJzKmn\n"
    "AMUrWo3K2j/Tm3+Xnl6WLz67/0rcYrJbbKvS3uyRP/stXyXEKw9CepyQ1ViBVFkW\n"
    "Aoy8qQEOWFDsZc/5UzhXUnb6LXr3qTkFEjNmhj+7uzv/lbBxlUG1NlYzSeOB6/RT\n"
    "8zH/lhOeKhLnWYPXdXKsa1FL6ij4X8DeDO1kY7fvAGmBn/THh1uTpDizM4YmeI+7\n"
    "4dmayA5xXvARte5h4Vu5SIze7iC057N+vymToMk2Jgk+ZZFpyXrnq+yo6RaD3ANc\n"
    "lrc4FbeUQZ5a5s5Sxgs9a0Y3WMG+7c5VnVXcbjBRz/aq2NtOnQQjikKKQA8GF080\n"
    "BQkCAwEAAaMaMBgwCQYDVR0TBAIwADALBgNVHQ8EBAMCBeAwDQYJKoZIhvcNAQEL\n"
    "BQADggEBAJefnMZpaRDHQSNUIEL3iwGXE9c6PmIsQVE2ustr+CakBp3TZ4l0enLt\n"
    "iGMfEVFju69cO4oyokWv+hl5eCMkHBf14Kv51vj448jowYnF1zmzn7SEzm5Uzlsa\n"
    "sqjtAprnLyof69WtLU1j5rYWBuFX86yOTwRAFNjm9fvhAcrEONBsQtqipBWkMROp\n"
    "iUYMkRqbKcQMdwxov+lHBYKq9zbWRoqLROAn54SRqgQk6c15JdEfgOOjShbsOkIH\n"
    "UhqcwRkQic7n1zwHVGVDgNIZVgmJ2IdIWBlPEC7oLrRrBD/X1iEEXtKab6p5o22n\n"
    "KB5mN+iQaE+Oe2cpGKZJiJRdM+IqDDQ=\n"
    "-----END CERTIFICATE-----\n"
>>).

-define(SVR_KEY, <<
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIIEowIBAAKCAQEAs15ZPekT5AV+JEnMqacAxStajcraP9Obf5eeXpYvPrv/Stxi\n"
    "sltsq9Le7JE/+y1fJcQrD0J6nJDVWIFUWRYCjLypAQ5YUOxlz/lTOFdSdvotevep\n"
    "OQUSM2aGP7u7O/+VsHGVQbU2VjNJ44Hr9FPzMf+WE54qEudZg9d1cqxrUUvqKPhf\n"
    "wN4M7WRjt+8AaYGf9MeHW5OkOLMzhiZ4j7vh2ZrIDnFe8BG17mHhW7lIjN7uILTn\n"
    "s36/KZOgyTYmCT5lkWnJeuer7KjpFoPcA1yWtzgVt5RBnlrmzlLGCz1rRjdYwb7t\n"
    "zlWdVdxuMFHP9qrY206dBCOKQopADwYXTzQFCQIDAQABAoIBAQCuvCbr7Pd3lvI/\n"
    "n7VFQG+7pHRe1VKwAxDkx2t8cYos7y/QWcm8Ptwqtw58HzPZGWYrgGMCRpzzkRSF\n"
    "V9g3wP1S5Scu5C6dBu5YIGc157tqNGXB+SpdZddJQ4Nc6yGHXYERllT04ffBGc3N\n"
    "WG/oYS/1cSteiSIrsDy/91FvGRCi7FPxH3wIgHssY/tw69s1Cfvaq5lr2NTFzxIG\n"
    "xCvpJKEdSfVfS9I7LYiymVjst3IOR/w76/ZFY9cRa8ZtmQSWWsm0TUpRC1jdcbkm\n"
    "ZoJptYWlP+gSwx/fpMYftrkJFGOJhHJHQhwxT5X/ajAISeqjjwkWSEJLwnHQd11C\n"
    "Zy2+29lBAoGBANlEAIK4VxCqyPXNKfoOOi5dS64NfvyH4A1v2+KaHWc7lqaqPN49\n"
    "ezfN2n3X+KWx4cviDD914Yc2JQ1vVJjSaHci7yivocDo2OfZDmjBqzaMp/y+rX1R\n"
    "/f3MmiTqMa468rjaxI9RRZu7vDgpTR+za1+OBCgMzjvAng8dJuN/5gjlAoGBANNY\n"
    "uYPKtearBmkqdrSV7eTUe49Nhr0XotLaVBH37TCW0Xv9wjO2xmbm5Ga/DCtPIsBb\n"
    "yPeYwX9FjoasuadUD7hRvbFu6dBa0HGLmkXRJZTcD7MEX2Lhu4BuC72yDLLFd0r+\n"
    "Ep9WP7F5iJyagYqIZtz+4uf7gBvUDdmvXz3sGr1VAoGAdXTD6eeKeiI6PlhKBztF\n"
    "zOb3EQOO0SsLv3fnodu7ZaHbUgLaoTMPuB17r2jgrYM7FKQCBxTNdfGZmmfDjlLB\n"
    "0xZ5wL8ibU30ZXL8zTlWPElST9sto4B+FYVVF/vcG9sWeUUb2ncPcJ/Po3UAktDG\n"
    "jYQTTyuNGtSJHpad/YOZctkCgYBtWRaC7bq3of0rJGFOhdQT9SwItN/lrfj8hyHA\n"
    "OjpqTV4NfPmhsAtu6j96OZaeQc+FHvgXwt06cE6Rt4RG4uNPRluTFgO7XYFDfitP\n"
    "vCppnoIw6S5BBvHwPP+uIhUX2bsi/dm8vu8tb+gSvo4PkwtFhEr6I9HglBKmcmog\n"
    "q6waEQKBgHyecFBeM6Ls11Cd64vborwJPAuxIW7HBAFj/BS99oeG4TjBx4Sz2dFd\n"
    "rzUibJt4ndnHIvCN8JQkjNG14i9hJln+H3mRss8fbZ9vQdqG+2vOWADYSzzsNI55\n"
    "RFY7JjluKcVkp/zCDeUxTU3O6sS+v6/3VE11Cob6OYQx3lN5wrZ3\n"
    "-----END RSA PRIVATE KEY-----\n"
>>).

-define(SVR_CERT2, <<
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDEzCCAfugAwIBAgIBATANBgkqhkiG9w0BAQsFADA/MQswCQYDVQQGEwJDTjER\n"
    "MA8GA1UECAwIaGFuZ3pob3UxDDAKBgNVBAoMA0VNUTEPMA0GA1UEAwwGUm9vdENB\n"
    "MB4XDTIwMDUwODA4MDY1N1oXDTMwMDUwNjA4MDY1N1owPzELMAkGA1UEBhMCQ04x\n"
    "ETAPBgNVBAgMCGhhbmd6aG91MQwwCgYDVQQKDANFTVExDzANBgNVBAMMBkNsaWVu\n"
    "dDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMy4hoksKcZBDbY680u6\n"
    "TS25U51nuB1FBcGMlF9B/t057wPOlxF/OcmbxY5MwepS41JDGPgulE1V7fpsXkiW\n"
    "1LUimYV/tsqBfymIe0mlY7oORahKji7zKQ2UBIVFhdlvQxunlIDnw6F9popUgyHt\n"
    "dMhtlgZK8oqRwHxO5dbfoukYd6J/r+etS5q26sgVkf3C6dt0Td7B25H9qW+f7oLV\n"
    "PbcHYCa+i73u9670nrpXsC+Qc7Mygwa2Kq/jwU+ftyLQnOeW07DuzOwsziC/fQZa\n"
    "nbxR+8U9FNftgRcC3uP/JMKYUqsiRAuaDokARZxVTV5hUElfpO6z6/NItSDvvh3i\n"
    "eikCAwEAAaMaMBgwCQYDVR0TBAIwADALBgNVHQ8EBAMCBeAwDQYJKoZIhvcNAQEL\n"
    "BQADggEBABchYxKo0YMma7g1qDswJXsR5s56Czx/I+B41YcpMBMTrRqpUC0nHtLk\n"
    "M7/tZp592u/tT8gzEnQjZLKBAhFeZaR3aaKyknLqwiPqJIgg0pgsBGITrAK3Pv4z\n"
    "5/YvAJJKgTe5UdeTz6U4lvNEux/4juZ4pmqH4qSFJTOzQS7LmgSmNIdd072rwXBd\n"
    "UzcSHzsJgEMb88u/LDLjj1pQ7AtZ4Tta8JZTvcgBFmjB0QUi6fgkHY6oGat/W4kR\n"
    "jSRUBlMUbM/drr2PVzRc2dwbFIl3X+ZE6n5Sl3ZwRAC/s92JU6CPMRW02muVu6xl\n"
    "goraNgPISnrbpR6KjxLZkVembXzjNNc=\n"
    "-----END CERTIFICATE-----\n"
>>).

-define(SVR_KEY2, <<
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIIEpAIBAAKCAQEAzLiGiSwpxkENtjrzS7pNLblTnWe4HUUFwYyUX0H+3TnvA86X\n"
    "EX85yZvFjkzB6lLjUkMY+C6UTVXt+mxeSJbUtSKZhX+2yoF/KYh7SaVjug5FqEqO\n"
    "LvMpDZQEhUWF2W9DG6eUgOfDoX2milSDIe10yG2WBkryipHAfE7l1t+i6Rh3on+v\n"
    "561LmrbqyBWR/cLp23RN3sHbkf2pb5/ugtU9twdgJr6Lve73rvSeulewL5BzszKD\n"
    "BrYqr+PBT5+3ItCc55bTsO7M7CzOIL99BlqdvFH7xT0U1+2BFwLe4/8kwphSqyJE\n"
    "C5oOiQBFnFVNXmFQSV+k7rPr80i1IO++HeJ6KQIDAQABAoIBAGWgvPjfuaU3qizq\n"
    "uti/FY07USz0zkuJdkANH6LiSjlchzDmn8wJ0pApCjuIE0PV/g9aS8z4opp5q/gD\n"
    "UBLM/a8mC/xf2EhTXOMrY7i9p/I3H5FZ4ZehEqIw9sWKK9YzC6dw26HabB2BGOnW\n"
    "5nozPSQ6cp2RGzJ7BIkxSZwPzPnVTgy3OAuPOiJytvK+hGLhsNaT+Y9bNDvplVT2\n"
    "ZwYTV8GlHZC+4b2wNROILm0O86v96O+Qd8nn3fXjGHbMsAnONBq10bZS16L4fvkH\n"
    "5G+W/1PeSXmtZFppdRRDxIW+DWcXK0D48WRliuxcV4eOOxI+a9N2ZJZZiNLQZGwg\n"
    "w3A8+mECgYEA8HuJFrlRvdoBe2U/EwUtG74dcyy30L4yEBnN5QscXmEEikhaQCfX\n"
    "Wm6EieMcIB/5I5TQmSw0cmBMeZjSXYoFdoI16/X6yMMuATdxpvhOZGdUGXxhAH+x\n"
    "xoTUavWZnEqW3fkUU71kT5E2f2i+0zoatFESXHeslJyz85aAYpP92H0CgYEA2e5A\n"
    "Yozt5eaA1Gyhd8SeptkEU4xPirNUnVQHStpMWUb1kzTNXrPmNWccQ7JpfpG6DcYl\n"
    "zUF6p6mlzY+zkMiyPQjwEJlhiHM2NlL1QS7td0R8ewgsFoyn8WsBI4RejWrEG9td\n"
    "EDniuIw+pBFkcWthnTLHwECHdzgquToyTMjrBB0CgYEA28tdGbrZXhcyAZEhHAZA\n"
    "Gzog+pKlkpEzeonLKIuGKzCrEKRecIK5jrqyQsCjhS0T7ZRnL4g6i0s+umiV5M5w\n"
    "fcc292pEA1h45L3DD6OlKplSQVTv55/OYS4oY3YEJtf5mfm8vWi9lQeY8sxOlQpn\n"
    "O+VZTdBHmTC8PGeTAgZXHZUCgYA6Tyv88lYowB7SN2qQgBQu8jvdGtqhcs/99GCr\n"
    "H3N0I69LPsKAR0QeH8OJPXBKhDUywESXAaEOwS5yrLNP1tMRz5Vj65YUCzeDG3kx\n"
    "gpvY4IMp7ArX0bSRvJ6mYSFnVxy3k174G3TVCfksrtagHioVBGQ7xUg5ltafjrms\n"
    "n8l55QKBgQDVzU8tQvBVqY8/1lnw11Vj4fkE/drZHJ5UkdC1eenOfSWhlSLfUJ8j\n"
    "ds7vEWpRPPoVuPZYeR1y78cyxKe1GBx6Wa2lF5c7xjmiu0xbRnrxYeLolce9/ntp\n"
    "asClqpnHT8/VJYTD7Kqj0fouTTZf0zkig/y+2XERppd8k+pSKjUCPQ==\n"
    "-----END RSA PRIVATE KEY-----\n"
>>).

-define(CONF_STOMP_BAISC_1, #{
    <<"idle_timeout">> => <<"10s">>,
    <<"mountpoint">> => <<"t/">>,
    <<"frame">> =>
        #{
            <<"max_headers">> => 20,
            <<"max_headers_length">> => 2000,
            <<"max_body_length">> => 2000
        }
}).
-define(CONF_STOMP_BAISC_2, #{
    <<"idle_timeout">> => <<"20s">>,
    <<"mountpoint">> => <<"t2/">>,
    <<"frame">> =>
        #{
            <<"max_headers">> => 30,
            <<"max_headers_length">> => 3000,
            <<"max_body_length">> => 3000
        }
}).
-define(CONF_STOMP_LISTENER_1, #{<<"bind">> => <<"61613">>}).
-define(CONF_STOMP_LISTENER_2, #{<<"bind">> => <<"61614">>}).
-define(CONF_STOMP_LISTENER_SSL, #{
    <<"bind">> => <<"61614">>,
    <<"ssl_options">> =>
        #{
            <<"cacertfile">> => ?SVR_CA,
            <<"certfile">> => ?SVR_CERT,
            <<"keyfile">> => ?SVR_KEY
        }
}).
-define(CONF_STOMP_LISTENER_SSL_2, #{
    <<"bind">> => <<"61614">>,
    <<"ssl_options">> =>
        #{
            <<"cacertfile">> => ?SVR_CA,
            <<"certfile">> => ?SVR_CERT2,
            <<"keyfile">> => ?SVR_KEY2
        }
}).
-define(CERTS_PATH(CertName), filename:join(["../../lib/emqx/etc/certs/", CertName])).
-define(CONF_STOMP_LISTENER_SSL_PATH, #{
    <<"bind">> => <<"61614">>,
    <<"ssl_options">> =>
        #{
            <<"cacertfile">> => ?CERTS_PATH("cacert.pem"),
            <<"certfile">> => ?CERTS_PATH("cert.pem"),
            <<"keyfile">> => ?CERTS_PATH("key.pem")
        }
}).
-define(CONF_STOMP_AUTHN_1, #{
    <<"mechanism">> => <<"password_based">>,
    <<"backend">> => <<"built_in_database">>,
    <<"user_id_type">> => <<"clientid">>
}).
-define(CONF_STOMP_AUTHN_2, #{
    <<"mechanism">> => <<"password_based">>,
    <<"backend">> => <<"built_in_database">>,
    <<"user_id_type">> => <<"username">>
}).

t_load_unload_gateway(_) ->
    StompConf1 = compose(
        ?CONF_STOMP_BAISC_1,
        ?CONF_STOMP_AUTHN_1,
        ?CONF_STOMP_LISTENER_1
    ),
    StompConf2 = compose(
        ?CONF_STOMP_BAISC_2,
        ?CONF_STOMP_AUTHN_1,
        ?CONF_STOMP_LISTENER_1
    ),
    {ok, _} = emqx_gateway_conf:load_gateway(stomp, StompConf1),
    ?assertMatch(
        {error, {badres, #{reason := already_exist}}},
        emqx_gateway_conf:load_gateway(stomp, StompConf1)
    ),
    assert_confs(StompConf1, emqx:get_raw_config([gateway, stomp])),

    {ok, _} = emqx_gateway_conf:update_gateway(stomp, StompConf2),
    assert_confs(StompConf2, emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:unload_gateway(stomp),
    ok = emqx_gateway_conf:unload_gateway(stomp),

    ?assertMatch(
        {error, {badres, #{reason := not_found}}},
        emqx_gateway_conf:update_gateway(stomp, StompConf2)
    ),

    ?assertException(
        error,
        {config_not_found, [<<"gateway">>, stomp]},
        emqx:get_raw_config([gateway, stomp])
    ),
    %% test update([gateway], Conf)
    Raw0 = emqx:get_raw_config([gateway]),
    #{<<"listeners">> := StompConfL1} = StompConf1,
    StompConf11 = StompConf1#{
        <<"listeners">> => emqx_gateway_conf:unconvert_listeners(StompConfL1)
    },
    #{<<"listeners">> := StompConfL2} = StompConf2,
    StompConf22 = StompConf2#{
        <<"listeners">> => emqx_gateway_conf:unconvert_listeners(StompConfL2)
    },
    Raw1 = Raw0#{<<"stomp">> => StompConf11},
    Raw2 = Raw0#{<<"stomp">> => StompConf22},
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw1)),
    assert_confs(StompConf1, emqx:get_raw_config([gateway, stomp])),
    ?assertMatch(
        #{
            config := #{
                authentication := #{backend := built_in_database, enable := true},
                listeners := #{tcp := #{default := #{bind := 61613}}},
                mountpoint := <<"t/">>,
                idle_timeout := 10000
            }
        },
        emqx_gateway:lookup('stomp')
    ),
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw2)),
    assert_confs(StompConf2, emqx:get_raw_config([gateway, stomp])),
    ?assertMatch(
        #{
            config :=
                #{
                    authentication := #{backend := built_in_database, enable := true},
                    listeners := #{tcp := #{default := #{bind := 61613}}},
                    idle_timeout := 20000,
                    mountpoint := <<"t2/">>
                }
        },
        emqx_gateway:lookup('stomp')
    ),
    %% reset
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw0)),
    ?assertEqual(undefined, emqx_gateway:lookup('stomp')),
    ok.

t_load_remove_authn(_) ->
    StompConf = compose_listener(?CONF_STOMP_BAISC_1, ?CONF_STOMP_LISTENER_1),

    {ok, _} = emqx_gateway_conf:load_gateway(<<"stomp">>, StompConf),
    assert_confs(StompConf, emqx:get_raw_config([gateway, stomp])),
    ct:sleep(500),

    {ok, _} = emqx_gateway_conf:add_authn(<<"stomp">>, ?CONF_STOMP_AUTHN_1),
    assert_confs(
        maps:put(<<"authentication">>, ?CONF_STOMP_AUTHN_1, StompConf),
        emqx:get_raw_config([gateway, stomp])
    ),

    {ok, _} = emqx_gateway_conf:update_authn(<<"stomp">>, ?CONF_STOMP_AUTHN_2),
    assert_confs(
        maps:put(<<"authentication">>, ?CONF_STOMP_AUTHN_2, StompConf),
        emqx:get_raw_config([gateway, stomp])
    ),

    ok = emqx_gateway_conf:remove_authn(<<"stomp">>),

    ?assertMatch(
        {error, {badres, #{reason := not_found}}},
        emqx_gateway_conf:update_authn(<<"stomp">>, ?CONF_STOMP_AUTHN_2)
    ),

    ?assertException(
        error,
        {config_not_found, [<<"gateway">>, stomp, authentication]},
        emqx:get_raw_config([gateway, stomp, authentication])
    ),
    %% test update([gateway], Conf)
    Raw0 = emqx:get_raw_config([gateway]),
    #{<<"listeners">> := StompConfL} = StompConf,
    StompConf1 = StompConf#{
        <<"listeners">> => emqx_gateway_conf:unconvert_listeners(StompConfL),
        <<"authentication">> => ?CONF_STOMP_AUTHN_1
    },
    Raw1 = maps:put(<<"stomp">>, StompConf1, Raw0),
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw1)),
    assert_confs(StompConf1, emqx:get_raw_config([gateway, stomp])),
    ?assertMatch(
        #{
            stomp :=
                #{
                    authn := <<"password_based:built_in_database">>,
                    listeners := [#{authn := <<"undefined">>, type := tcp}],
                    num_clients := 0
                }
        },
        emqx_gateway:get_basic_usage_info()
    ),
    %% reset(remove authn)
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw0)),
    ?assertMatch(
        #{
            stomp :=
                #{
                    authn := <<"undefined">>,
                    listeners := [#{authn := <<"undefined">>, type := tcp}],
                    num_clients := 0
                }
        },
        emqx_gateway:get_basic_usage_info()
    ),
    ok.

t_load_remove_listeners(_) ->
    StompConf = compose_authn(?CONF_STOMP_BAISC_1, ?CONF_STOMP_AUTHN_1),

    {ok, _} = emqx_gateway_conf:load_gateway(<<"stomp">>, StompConf),
    assert_confs(StompConf, emqx:get_raw_config([gateway, stomp])),
    ct:sleep(500),

    {ok, _} = emqx_gateway_conf:add_listener(
        <<"stomp">>,
        {<<"tcp">>, <<"default">>},
        ?CONF_STOMP_LISTENER_1
    ),

    assert_confs(
        maps:merge(StompConf, listener(?CONF_STOMP_LISTENER_1)),
        emqx:get_raw_config([gateway, stomp])
    ),

    {ok, _} = emqx_gateway_conf:update_listener(
        <<"stomp">>,
        {<<"tcp">>, <<"default">>},
        ?CONF_STOMP_LISTENER_2
    ),
    assert_confs(
        maps:merge(StompConf, listener(?CONF_STOMP_LISTENER_2)),
        emqx:get_raw_config([gateway, stomp])
    ),

    ok = emqx_gateway_conf:remove_listener(
        <<"stomp">>, {<<"tcp">>, <<"default">>}
    ),

    ?assertMatch(
        {error, {badres, #{reason := not_found}}},
        emqx_gateway_conf:update_listener(
            <<"stomp">>, {<<"tcp">>, <<"default">>}, ?CONF_STOMP_LISTENER_2
        )
    ),

    ?assertException(
        error,
        {config_not_found, [<<"gateway">>, stomp, listeners, tcp, default]},
        emqx:get_raw_config([gateway, stomp, listeners, tcp, default])
    ),
    %% test update([gateway], Conf)
    Raw0 = emqx:get_raw_config([gateway]),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"stomp">>, <<"listeners">>, <<"tcp">>, <<"default">>], Raw0, ?CONF_STOMP_LISTENER_1
    ),
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw1)),
    assert_confs(
        maps:merge(StompConf, listener(?CONF_STOMP_LISTENER_1)),
        emqx:get_raw_config([gateway, stomp])
    ),
    ?assertMatch(
        #{
            stomp :=
                #{
                    authn := <<"password_based:built_in_database">>,
                    listeners := [#{authn := <<"undefined">>, type := tcp}],
                    num_clients := 0
                }
        },
        emqx_gateway:get_basic_usage_info()
    ),
    Raw2 = emqx_utils_maps:deep_put(
        [<<"stomp">>, <<"listeners">>, <<"tcp">>, <<"default">>], Raw0, ?CONF_STOMP_LISTENER_2
    ),
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw2)),
    assert_confs(
        maps:merge(StompConf, listener(?CONF_STOMP_LISTENER_2)),
        emqx:get_raw_config([gateway, stomp])
    ),
    ?assertMatch(
        #{
            stomp :=
                #{
                    authn := <<"password_based:built_in_database">>,
                    listeners := [#{authn := <<"undefined">>, type := tcp}],
                    num_clients := 0
                }
        },
        emqx_gateway:get_basic_usage_info()
    ),
    %% reset(remove listener)
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw0)),
    ?assertMatch(
        #{
            stomp :=
                #{
                    authn := <<"password_based:built_in_database">>,
                    listeners := [],
                    num_clients := 0
                }
        },
        emqx_gateway:get_basic_usage_info()
    ),
    ok.

t_load_remove_listener_authn(_) ->
    StompConf = compose_listener(
        ?CONF_STOMP_BAISC_1,
        ?CONF_STOMP_LISTENER_1
    ),
    StompConf1 = compose_listener_authn(
        ?CONF_STOMP_BAISC_1,
        ?CONF_STOMP_LISTENER_1,
        ?CONF_STOMP_AUTHN_1
    ),
    StompConf2 = compose_listener_authn(
        ?CONF_STOMP_BAISC_1,
        ?CONF_STOMP_LISTENER_1,
        ?CONF_STOMP_AUTHN_2
    ),

    {ok, _} = emqx_gateway_conf:load_gateway(<<"stomp">>, StompConf),
    assert_confs(StompConf, emqx:get_raw_config([gateway, stomp])),
    ct:sleep(500),

    {ok, _} = emqx_gateway_conf:add_authn(
        <<"stomp">>, {<<"tcp">>, <<"default">>}, ?CONF_STOMP_AUTHN_1
    ),
    assert_confs(StompConf1, emqx:get_raw_config([gateway, stomp])),

    {ok, _} = emqx_gateway_conf:update_authn(
        <<"stomp">>, {<<"tcp">>, <<"default">>}, ?CONF_STOMP_AUTHN_2
    ),
    assert_confs(StompConf2, emqx:get_raw_config([gateway, stomp])),

    ok = emqx_gateway_conf:remove_authn(
        <<"stomp">>, {<<"tcp">>, <<"default">>}
    ),

    ?assertMatch(
        {error, {badres, #{reason := not_found}}},
        emqx_gateway_conf:update_authn(
            <<"stomp">>, {<<"tcp">>, <<"default">>}, ?CONF_STOMP_AUTHN_2
        )
    ),

    Path = [gateway, stomp, listeners, tcp, default, authentication],
    ?assertException(
        error,
        {config_not_found, [<<"gateway">>, stomp, listeners, tcp, default, authentication]},
        emqx:get_raw_config(Path)
    ),
    ok.

t_load_gateway_with_certs_content(_) ->
    StompConf = compose_ssl_listener(
        ?CONF_STOMP_BAISC_1,
        ?CONF_STOMP_LISTENER_SSL
    ),
    {ok, _} = emqx_gateway_conf:load_gateway(<<"stomp">>, StompConf),
    assert_confs(StompConf, emqx:get_raw_config([gateway, stomp])),
    SslConf = emqx_utils_maps:deep_get(
        [<<"listeners">>, <<"ssl">>, <<"default">>, <<"ssl_options">>],
        emqx:get_raw_config([gateway, stomp])
    ),
    assert_ssl_confs_files_exist(SslConf),
    ok = emqx_gateway_conf:unload_gateway(<<"stomp">>),
    assert_ssl_confs_files_deleted(SslConf),
    ?assertException(
        error,
        {config_not_found, [<<"gateway">>, stomp]},
        emqx:get_raw_config([gateway, stomp])
    ),
    %% test update([gateway], Conf)
    Raw0 = emqx:get_raw_config([gateway]),
    #{<<"listeners">> := StompConfL} = StompConf,
    StompConf1 = StompConf#{
        <<"listeners">> => emqx_gateway_conf:unconvert_listeners(StompConfL)
    },
    Raw1 = emqx_utils_maps:deep_put([<<"stomp">>], Raw0, StompConf1),
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw1)),
    assert_ssl_confs_files_exist(SslConf),
    ?assertEqual(
        SslConf,
        emqx_utils_maps:deep_get(
            [<<"listeners">>, <<"ssl">>, <<"default">>, <<"ssl_options">>],
            emqx:get_raw_config([gateway, stomp])
        )
    ),
    %% reset
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw0)),
    assert_ssl_confs_files_deleted(SslConf),
    ok.

%% TODO: Comment out this test case for now, because emqx_tls_lib
%% will delete the configured certificate file.

%t_load_gateway_with_certs_path(_) ->
%    StompConf = compose_ssl_listener(
%                  ?CONF_STOMP_BAISC_1,
%                  ?CONF_STOMP_LISTENER_SSL_PATH
%                 ),
%    {ok, _} = emqx_gateway_conf:load_gateway(<<"stomp">>, StompConf),
%    assert_confs(StompConf, emqx:get_raw_config([gateway, stomp])),
%    SslConf = emqx_utils_maps:deep_get(
%                [<<"listeners">>, <<"ssl">>, <<"default">>, <<"ssl_options">>],
%                emqx:get_raw_config([gateway, stomp])
%               ),
%    ok = emqx_gateway_conf:unload_gateway(<<"stomp">>),
%    assert_ssl_confs_files_deleted(SslConf),
%    ?assertException(error, {config_not_found, [gateway, stomp]},
%                     emqx:get_raw_config([gateway, stomp])),
%    ok.

t_add_listener_with_certs_content(_) ->
    StompConf = ?CONF_STOMP_BAISC_1,
    {ok, _} = emqx_gateway_conf:load_gateway(<<"stomp">>, StompConf),
    assert_confs(StompConf, emqx:get_raw_config([gateway, stomp])),

    {ok, _} = emqx_gateway_conf:add_listener(
        <<"stomp">>,
        {<<"ssl">>, <<"default">>},
        ?CONF_STOMP_LISTENER_SSL
    ),
    assert_confs(
        maps:merge(StompConf, ssl_listener(?CONF_STOMP_LISTENER_SSL)),
        emqx:get_raw_config([gateway, stomp])
    ),

    {ok, _} = emqx_gateway_conf:update_listener(
        <<"stomp">>,
        {<<"ssl">>, <<"default">>},
        ?CONF_STOMP_LISTENER_SSL_2
    ),
    assert_confs(
        maps:merge(StompConf, ssl_listener(?CONF_STOMP_LISTENER_SSL_2)),
        emqx:get_raw_config([gateway, stomp])
    ),

    SslConf = emqx_utils_maps:deep_get(
        [<<"listeners">>, <<"ssl">>, <<"default">>, <<"ssl_options">>],
        emqx:get_raw_config([gateway, stomp])
    ),
    assert_ssl_confs_files_exist(SslConf),
    ok = emqx_gateway_conf:remove_listener(
        <<"stomp">>, {<<"ssl">>, <<"default">>}
    ),
    assert_ssl_confs_files_deleted(SslConf),

    ?assertMatch(
        {error, {badres, #{reason := not_found}}},
        emqx_gateway_conf:update_listener(
            <<"stomp">>, {<<"ssl">>, <<"default">>}, ?CONF_STOMP_LISTENER_SSL_2
        )
    ),

    ?assertException(
        error,
        {config_not_found, [<<"gateway">>, stomp, listeners, ssl, default]},
        emqx:get_raw_config([gateway, stomp, listeners, ssl, default])
    ),

    %% test update([gateway], Conf)
    Raw0 = emqx:get_raw_config([gateway]),
    Raw1 = emqx_utils_maps:deep_put(
        [<<"stomp">>, <<"listeners">>, <<"ssl">>, <<"default">>], Raw0, ?CONF_STOMP_LISTENER_SSL
    ),
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw1)),
    SslConf1 = emqx_utils_maps:deep_get(
        [<<"listeners">>, <<"ssl">>, <<"default">>, <<"ssl_options">>],
        emqx:get_raw_config([gateway, stomp])
    ),
    assert_ssl_confs_files_exist(SslConf1),
    %% update
    Raw2 = emqx_utils_maps:deep_put(
        [<<"stomp">>, <<"listeners">>, <<"ssl">>, <<"default">>], Raw0, ?CONF_STOMP_LISTENER_SSL_2
    ),
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw2)),
    SslConf2 =
        emqx_utils_maps:deep_get(
            [<<"listeners">>, <<"ssl">>, <<"default">>, <<"ssl_options">>],
            emqx:get_raw_config([gateway, stomp])
        ),
    assert_ssl_confs_files_exist(SslConf2),
    %% reset
    ?assertMatch({ok, _}, emqx:update_config([gateway], Raw0)),
    assert_ssl_confs_files_deleted(SslConf),
    assert_ssl_confs_files_deleted(SslConf1),
    assert_ssl_confs_files_deleted(SslConf2),
    ok.

assert_ssl_confs_files_deleted(SslConf) when is_map(SslConf) ->
    {ok, _} = emqx_tls_certfile_gc:force(),
    Ks = [<<"cacertfile">>, <<"certfile">>, <<"keyfile">>],
    lists:foreach(
        fun(K) ->
            Path = maps:get(K, SslConf),
            {error, enoent} = file:read_file(Path)
        end,
        Ks
    ).
assert_ssl_confs_files_exist(SslConf) when is_map(SslConf) ->
    Ks = [<<"cacertfile">>, <<"certfile">>, <<"keyfile">>],
    lists:foreach(
        fun(K) ->
            Path = maps:get(K, SslConf),
            {ok, _} = file:read_file(Path)
        end,
        Ks
    ).

%%--------------------------------------------------------------------
%% Utils

compose(Basic, Authn, Listener) ->
    maps:merge(
        maps:merge(Basic, #{<<"authentication">> => Authn}),
        listener(Listener)
    ).

compose_listener(Basic, Listener) ->
    maps:merge(Basic, listener(Listener)).

compose_ssl_listener(Basic, Listener) ->
    maps:merge(Basic, ssl_listener(Listener)).

compose_authn(Basic, Authn) ->
    maps:merge(Basic, #{<<"authentication">> => Authn}).

compose_listener_authn(Basic, Listener, Authn) ->
    maps:merge(
        Basic,
        listener(maps:put(<<"authentication">>, Authn, Listener))
    ).

listener(L) ->
    #{
        <<"listeners">> => [
            L#{
                <<"type">> => <<"tcp">>,
                <<"name">> => <<"default">>
            }
        ]
    }.

ssl_listener(L) ->
    #{
        <<"listeners">> => [
            L#{
                <<"type">> => <<"ssl">>,
                <<"name">> => <<"default">>
            }
        ]
    }.
