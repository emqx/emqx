%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_client_region_SUITE).

-moduledoc """
Signing region selection of the S3 client.

The cases do not use the network. Regular requests go to a mocked HTTP
client, and the region is read from the credential scope of the
`Authorization` header. Presigned URLs carry the region in
`X-Amz-Credential`.
""".

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-define(POOL, ?MODULE).
-define(VPCE_HOST,
    "bucket.vpce-0123456789abcdef0-example-us-west-2a.s3.us-west-2.vpce.amazonaws.com"
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    ok = meck:new(emqx_s3_client_http, [passthrough]),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = meck:unload(emqx_s3_client_http).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "An explicit region is set on the aws_config.".
t_aws_config_region(_Config) ->
    AWSConfig = emqx_s3_client:aws_config(client_config(#{<<"region">> => <<"eu-central-1">>})),
    ?assertEqual("eu-central-1", AWSConfig#aws_config.aws_region).

-doc "A config without a region produces an aws_config without a region.".
t_aws_config_no_region(_Config) ->
    Config = client_config(#{}),
    ?assertEqual(undefined, maps:get(region, Config)),
    AWSConfig = emqx_s3_client:aws_config(Config),
    ?assertEqual(undefined, AWSConfig#aws_config.aws_region),
    %% Apart from the region, the aws_config is the same as with an explicit region.
    AWSConfigWithRegion = emqx_s3_client:aws_config(Config#{region => <<"eu-central-1">>}),
    ?assertEqual(AWSConfig, AWSConfigWithRegion#aws_config{aws_region = undefined}).

-doc "An empty region is the same as no region: the region is inferred from the host.".
t_empty_region_infers(_Config) ->
    Config = client_config(#{<<"region">> => <<>>}),
    ?assertEqual(undefined, (emqx_s3_client:aws_config(Config))#aws_config.aws_region),
    ?assertEqual({"us-west-2", "us-west-2"}, signed_regions(Config)).

-doc "Without a region, the region is inferred from the host on both signing paths.".
t_inferred_region(_Config) ->
    Config = client_config(#{<<"host">> => <<"s3.us-west-2.amazonaws.com">>}),
    ?assertEqual({"us-west-2", "us-west-2"}, signed_regions(Config)).

-doc "An explicit region takes precedence over the region in the host on both signing paths.".
t_explicit_region_wins(_Config) ->
    Config = client_config(#{
        <<"host">> => <<"s3.us-west-2.amazonaws.com">>,
        <<"region">> => <<"eu-central-1">>
    }),
    ?assertEqual({"eu-central-1", "eu-central-1"}, signed_regions(Config)).

-doc "Without a region, an AWS PrivateLink endpoint host selects its region on both signing paths.".
t_inferred_region_vpce(_Config) ->
    Config = client_config(#{<<"host">> => list_to_binary(?VPCE_HOST)}),
    ?assertEqual({"us-west-2", "us-west-2"}, signed_regions(Config)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_config(Overrides) ->
    RawConfig = maps:merge(
        #{
            <<"bucket">> => <<"bucket">>,
            <<"host">> => <<"s3.us-west-2.amazonaws.com">>,
            <<"port">> => 443,
            <<"access_key_id">> => <<"access_key_id">>,
            <<"secret_access_key">> => <<"secret_access_key">>,
            <<"transport_options">> => #{<<"ssl">> => #{<<"enable">> => false}}
        },
        Overrides
    ),
    ProfileConfig = emqx_s3_schema:translate(RawConfig),
    emqx_s3_profile_conf:client_config(ProfileConfig, ?POOL).

%% Returns `{RequestRegion, PresignedUrlRegion}'.
signed_regions(Config) ->
    Self = self(),
    ok = meck:expect(
        emqx_s3_client_http,
        request,
        fun(_Url, _Method, Headers, _Body, _Timeout, _HttpClient) ->
            Self ! {authorization, proplists:get_value("Authorization", Headers)},
            {ok, {{200, "OK"}, [], <<>>}}
        end
    ),
    Client = emqx_s3_client:create("bucket", Config),
    ok = emqx_s3_client:put_object(Client, "key", <<"data">>),
    Authorization =
        receive
            {authorization, Auth} -> Auth
        after 1000 -> ct:fail(no_request)
        end,
    {match, [RequestRegion]} =
        re:run(Authorization, "Credential=[^/]+/[0-9]+/([^/]+)/s3/", [
            {capture, all_but_first, list}
        ]),
    Url = lists:flatten(emqx_s3_client:uri(Client, "key")),
    {match, [UrlRegion]} =
        re:run(Url, "X-Amz-Credential=[^%]+%2F[0-9]+%2F([^%]+)%2Fs3%2F", [
            {capture, all_but_first, list}
        ]),
    {RequestRegion, UrlRegion}.
