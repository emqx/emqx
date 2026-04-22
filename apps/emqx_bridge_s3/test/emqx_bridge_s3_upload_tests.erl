%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3_upload_tests).

-include_lib("eunit/include/eunit.hrl").

accept_legacy_datetime_test() ->
    Template = <<"${action}/${node}/${datetime.rfc3339utc}/${sequence}">>,
    ?assertEqual(ok, emqx_bridge_s3_upload:validate_key_template(Template)).

accept_datetime_parts_test() ->
    Template =
        <<
            "year=${datetime.YYYY}/month=${datetime.MM}/day=${datetime.DD}/"
            "hour=${datetime.hh}/${action}_${node}_N${sequence}.json"
        >>,
    ?assertEqual(ok, emqx_bridge_s3_upload:validate_key_template(Template)).

accept_datetime_until_parts_test() ->
    Template = <<"${datetime_until.YYYY}-${datetime_until.MM}/${action}/${node}/${sequence}">>,
    ?assertEqual(ok, emqx_bridge_s3_upload:validate_key_template(Template)).

reject_unknown_datetime_token_test() ->
    Template = <<"${datetime.bogus}/${action}/${node}/${sequence}">>,
    ?assertMatch({error, _}, emqx_bridge_s3_upload:validate_key_template(Template)).

reject_unknown_binding_test() ->
    Template = <<"${nope}/${action}/${node}/${sequence}">>,
    ?assertMatch({error, _}, emqx_bridge_s3_upload:validate_key_template(Template)).

accept_datetime_utc_zone_test() ->
    Template = <<"${datetime.utc.YYYY}/${datetime.utc.MM}/${action}/${node}/${sequence}">>,
    ?assertEqual(ok, emqx_bridge_s3_upload:validate_key_template(Template)).

accept_datetime_local_zone_test() ->
    Template =
        <<"${datetime.local.YYYY}/${datetime.local.MM}/${action}/${node}/${sequence}">>,
    ?assertEqual(ok, emqx_bridge_s3_upload:validate_key_template(Template)).

accept_datetime_until_zoned_test() ->
    Template = <<"${datetime_until.local.YYYY}/${action}/${node}/${sequence}">>,
    ?assertEqual(ok, emqx_bridge_s3_upload:validate_key_template(Template)).

reject_unknown_zone_test() ->
    Template = <<"${datetime.berlin.YYYY}/${action}/${node}/${sequence}">>,
    ?assertMatch({error, _}, emqx_bridge_s3_upload:validate_key_template(Template)).

reject_zoned_legacy_format_test() ->
    Template = <<"${datetime.utc.rfc3339}/${action}/${node}/${sequence}">>,
    ?assertMatch({error, _}, emqx_bridge_s3_upload:validate_key_template(Template)).
