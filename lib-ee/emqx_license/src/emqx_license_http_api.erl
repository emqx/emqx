%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_http_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    namespace/0,
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    '/license'/2,
    '/license/key'/2,
    '/license/file'/2
]).

-define(BAD_REQUEST, 'BAD_REQUEST').

namespace() -> "license_http_api".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    [
        "/license",
        "/license/key",
        "/license/file"
    ].

schema("/license") ->
    #{
        'operationId' => '/license',
        get => #{
            tags => [<<"license">>],
            summary => <<"Get license info">>,
            description => ?DESC("desc_license_info_api"),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    map(),
                    #{
                        sample_license_info => #{
                            value => sample_license_info_response()
                        }
                    }
                )
            }
        }
    };
schema("/license/file") ->
    #{
        'operationId' => '/license/file',
        post => #{
            tags => [<<"license">>],
            summary => <<"Upload license file">>,
            description => ?DESC("desc_license_file_api"),
            'requestBody' => emqx_dashboard_swagger:file_schema(filename),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    map(),
                    #{
                        sample_license_info => #{
                            value => sample_license_info_response()
                        }
                    }
                ),
                400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad license file">>)
            }
        }
    };
schema("/license/key") ->
    #{
        'operationId' => '/license/key',
        post => #{
            tags => [<<"license">>],
            summary => <<"Update license key">>,
            description => ?DESC("desc_license_key_api"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_license_schema:key_license(),
                #{
                    license_key => #{
                        summary => <<"License key string">>,
                        value => #{
                            <<"key">> => <<"xxx">>,
                            <<"connection_low_watermark">> => "75%",
                            <<"connection_high_watermark">> => "80%"
                        }
                    }
                }
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    map(),
                    #{
                        sample_license_info => #{
                            value => sample_license_info_response()
                        }
                    }
                ),
                400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad license file">>)
            }
        }
    }.

sample_license_info_response() ->
    #{
        customer => "Foo",
        customer_type => 10,
        deployment => "bar-deployment",
        email => "contact@foo.com",
        expiry => false,
        expiry_at => "2295-10-27",
        max_connections => 10,
        start_at => "2022-01-11",
        type => "trial"
    }.

error_msg(Code, Msg) ->
    #{code => Code, message => emqx_misc:readable_error_msg(Msg)}.

'/license'(get, _Params) ->
    License = maps:from_list(emqx_license_checker:dump()),
    {200, License}.

'/license/file'(post, #{body := #{<<"filename">> := #{type := _} = File}}) ->
    [{_Filename, Contents}] = maps:to_list(maps:without([type], File)),
    case emqx_license:update_file_contents(Contents) of
        {error, Error} ->
            ?SLOG(error, #{
                msg => "bad_license_file",
                reason => Error
            }),
            {400, error_msg(?BAD_REQUEST, <<"Bad license file">>)};
        {ok, _} ->
            ?SLOG(info, #{
                msg => "updated_license_file"
            }),
            License = maps:from_list(emqx_license_checker:dump()),
            {200, License}
    end;
'/license/file'(post, _Params) ->
    {400, error_msg(?BAD_REQUEST, <<"Invalid request params">>)}.

'/license/key'(post, #{body := #{<<"key">> := Key}}) ->
    case emqx_license:update_key(Key) of
        {error, Error} ->
            ?SLOG(error, #{
                msg => "bad_license_key",
                reason => Error
            }),
            {400, error_msg(?BAD_REQUEST, <<"Bad license key">>)};
        {ok, _} ->
            ?SLOG(info, #{msg => "updated_license_key"}),
            License = maps:from_list(emqx_license_checker:dump()),
            {200, License}
    end;
'/license/key'(post, _Params) ->
    {400, error_msg(?BAD_REQUEST, <<"Invalid request params">>)}.
