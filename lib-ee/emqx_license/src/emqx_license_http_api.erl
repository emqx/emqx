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
    '/license/upload'/2
]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').

namespace() -> "license_http_api".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    [
        "/license",
        "/license/upload"
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
                            value => #{
                                customer => "Foo",
                                customer_type => 10,
                                deployment => "bar-deployment",
                                email => "contact@foo.com",
                                expiry => false,
                                expiry_at => "2295-10-27",
                                max_connections => 10,
                                start_at => "2022-01-11",
                                type => "trial"
                            }
                        }
                    }
                )
            }
        }
    };
schema("/license/upload") ->
    #{
        'operationId' => '/license/upload',
        post => #{
            tags => [<<"license">>],
            summary => <<"Upload license">>,
            description => ?DESC("desc_license_upload_api"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_license_schema:license_type(),
                #{
                    license_key => #{
                        summary => <<"License key string">>,
                        value => #{
                            <<"key">> => <<"xxx">>,
                            <<"connection_low_watermark">> => "75%",
                            <<"connection_high_watermark">> => "80%"
                        }
                    },
                    license_file => #{
                        summary => <<"Path to a license file">>,
                        value => #{
                            <<"file">> => <<"/path/to/license">>,
                            <<"connection_low_watermark">> => "75%",
                            <<"connection_high_watermark">> => "80%"
                        }
                    }
                }
            ),
            responses => #{
                200 => <<"ok">>,
                400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"bad request">>),
                404 => emqx_dashboard_swagger:error_codes([?NOT_FOUND], <<"file not found">>)
            }
        }
    }.

'/license'(get, _Params) ->
    License = maps:from_list(emqx_license_checker:dump()),
    {200, License}.

'/license/upload'(post, #{body := #{<<"file">> := Filepath}}) ->
    case emqx_license:update_file(Filepath) of
        {error, enoent} ->
            ?SLOG(error, #{
                msg => "license_file_not_found",
                path => Filepath
            }),
            {404, <<"file not found">>};
        {error, Error} ->
            ?SLOG(error, #{
                msg => "bad_license_file",
                reason => Error,
                path => Filepath
            }),
            {400, <<"bad request">>};
        {ok, _} ->
            ?SLOG(info, #{
                msg => "updated_license_file",
                path => Filepath
            }),
            {200, <<"ok">>}
    end;
'/license/upload'(post, #{body := #{<<"key">> := Key}}) ->
    case emqx_license:update_key(Key) of
        {error, Error} ->
            ?SLOG(error, #{
                msg => "bad_license_key",
                reason => Error
            }),
            {400, <<"bad request">>};
        {ok, _} ->
            ?SLOG(info, #{msg => "updated_license_key"}),
            {200, <<"ok">>}
    end;
'/license/upload'(post, _Params) ->
    {400, <<"bad request">>}.
