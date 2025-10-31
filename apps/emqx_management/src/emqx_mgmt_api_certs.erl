%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_certs).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx/include/emqx_managed_certs.hrl").

%% `minirest' and `minirest_trails' API
-export([
    namespace/0,
    api_spec/0,
    fields/1,
    paths/0,
    schema/1
]).

%% `minirest' handlers
-export([
    '/certs/global/list'/2,
    '/certs/global/name/:name'/2,
    '/certs/ns/:namespace/list'/2,
    '/certs/ns/:namespace/name/:name'/2
]).

%%-------------------------------------------------------------------------------------------------
%% Type definitions
%%-------------------------------------------------------------------------------------------------

-define(TAGS, [<<"TLS Management">>]).

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        check_schema => true, translate_body => {true, atom_keys}
    }).

paths() ->
    [
        "/certs/global/list",
        "/certs/global/name/:name",
        "/certs/ns/:namespace/list",
        "/certs/ns/:namespace/name/:name"
    ].

schema("/certs/global/list") ->
    #{
        'operationId' => '/certs/global/list',
        get => #{
            tags => ?TAGS,
            description => ?DESC("global_bundle_list"),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            array(ref(bundle_out)),
                            example_bundle_list()
                        ),
                    500 => internal_error(?DESC("internal_error"))
                }
        }
    };
schema("/certs/global/name/:name") ->
    #{
        'operationId' => '/certs/global/name/:name',
        get => #{
            tags => ?TAGS,
            description => ?DESC("global_file_list"),
            parameters => [param_path_bundle_name()],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            hoconsc:map(file_kind, ref(file_out)),
                            example_file_list()
                        ),
                    404 => not_found(?DESC("bundle_not_found")),
                    500 => internal_error(?DESC("internal_error"))
                }
        },
        post => #{
            tags => ?TAGS,
            description => ?DESC("global_file_upload"),
            parameters => [param_path_bundle_name()],
            'requestBody' => upload_files_request_body(),
            responses =>
                #{
                    204 => <<"">>,
                    400 => bad_request(?DESC("bad_request")),
                    500 => internal_error(?DESC("internal_error"))
                }
        },
        delete => #{
            tags => ?TAGS,
            description => ?DESC("global_bundle_delete"),
            parameters => [param_path_bundle_name()],
            responses =>
                #{
                    204 => <<"">>,
                    500 => internal_error(?DESC("internal_error"))
                }
        }
    };
schema("/certs/ns/:namespace/list") ->
    #{
        'operationId' => '/certs/ns/:namespace/list',
        get => #{
            tags => ?TAGS,
            description => ?DESC("ns_bundle_list"),
            parameters => [param_path_ns()],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            array(ref(bundle_out)),
                            example_bundle_list()
                        ),
                    500 => internal_error(?DESC("internal_error"))
                }
        }
    };
schema("/certs/ns/:namespace/name/:name") ->
    #{
        'operationId' => '/certs/ns/:namespace/name/:name',
        get => #{
            tags => ?TAGS,
            description => ?DESC("ns_file_list"),
            parameters => [param_path_ns(), param_path_bundle_name()],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            hoconsc:map(file_kind, ref(file_out)),
                            example_file_list()
                        ),
                    404 => not_found(?DESC("bundle_not_found")),
                    500 => internal_error(?DESC("internal_error"))
                }
        },
        post => #{
            tags => ?TAGS,
            description => ?DESC("ns_file_upload"),
            parameters => [param_path_ns(), param_path_bundle_name()],
            'requestBody' => upload_files_request_body(),
            responses =>
                #{
                    204 => <<"">>,
                    400 => bad_request(?DESC("bad_request")),
                    500 => internal_error(?DESC("internal_error"))
                }
        },
        delete => #{
            tags => ?TAGS,
            description => ?DESC("ns_bundle_delete"),
            parameters => [param_path_ns(), param_path_bundle_name()],
            responses =>
                #{
                    204 => <<"">>,
                    500 => internal_error(?DESC("internal_error"))
                }
        }
    }.

fields(files_in) ->
    Kinds = [
        ?FILE_KIND_KEY,
        ?FILE_KIND_CHAIN,
        ?FILE_KIND_CA,
        ?FILE_KIND_ACC_KEY,
        ?FILE_KIND_KEY_PASSWORD
    ],
    [
        {Kind, mk(binary(), #{required => false})}
     || Kind <- Kinds
    ];
fields(bundle_out) ->
    [{name, mk(binary(), #{})}];
fields(file_out) ->
    [{path, mk(binary(), #{})}].

param_path_ns() ->
    {namespace,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"ns1">>,
                desc => ?DESC("param_path_ns")
            }
        )}.

param_path_bundle_name() ->
    {name,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"bundle1">>,
                validator => fun emqx_resource:validate_name/1,
                desc => ?DESC("param_path_bundle_name")
            }
        )}.

upload_files_request_body() ->
    hoconsc:mk(ref(files_in), #{
        converter => fun upload_files_request_body_converter/2,
        validator => fun upload_files_request_body_validator/1
    }).

upload_files_request_body_converter(#{} = Input, _HoconOpts) ->
    case maps:to_list(Input) of
        [{_Type, #{type := _}} | _] = InputList ->
            %% multipart/form-data
            lists:foldl(
                fun({Type, Data0}, Acc) ->
                    [{_Filename, Contents}] = maps:to_list(maps:remove(type, Data0)),
                    Acc#{Type => Contents}
                end,
                #{},
                InputList
            );
        _ ->
            Input
    end;
upload_files_request_body_converter(Input, _HoconOpts) ->
    Input.

upload_files_request_body_validator(#{} = Input) when map_size(Input) == 0 ->
    {error, <<"must include at least one file kind">>};
upload_files_request_body_validator(#{} = _Input) ->
    ok;
upload_files_request_body_validator(_Input) ->
    {error, <<"invalid input">>}.

not_found(Desc) -> emqx_dashboard_swagger:error_codes([?NOT_FOUND], Desc).
bad_request(Desc) -> emqx_dashboard_swagger:error_codes([?BAD_REQUEST], Desc).
internal_error(Desc) -> emqx_dashboard_swagger:error_codes([?INTERNAL_ERROR], Desc).

%%-------------------------------------------------------------------------------------------------
%% `minirest' handlers
%%-------------------------------------------------------------------------------------------------

'/certs/global/list'(get, _Req) ->
    handle_list_bundles(?global_ns).

'/certs/global/name/:name'(get, #{bindings := #{name := BundleName}} = _Req) ->
    handle_list_files(?global_ns, BundleName);
'/certs/global/name/:name'(post, #{bindings := #{name := BundleName}} = Req) ->
    #{body := Files} = Req,
    handle_upload_files(?global_ns, BundleName, Files);
'/certs/global/name/:name'(delete, #{bindings := #{name := BundleName}} = _Req) ->
    handle_delete_bundle(?global_ns, BundleName).

'/certs/ns/:namespace/list'(get, #{bindings := #{namespace := Namespace}} = _Req) ->
    handle_list_bundles(Namespace).

'/certs/ns/:namespace/name/:name'(get, Req) ->
    #{bindings := #{namespace := Namespace, name := BundleName}} = Req,
    handle_list_files(Namespace, BundleName);
'/certs/ns/:namespace/name/:name'(post, Req) ->
    #{bindings := #{namespace := Namespace, name := BundleName}} = Req,
    #{body := Files} = Req,
    handle_upload_files(Namespace, BundleName, Files);
'/certs/ns/:namespace/name/:name'(delete, Req) ->
    #{bindings := #{namespace := Namespace, name := BundleName}} = Req,
    handle_delete_bundle(Namespace, BundleName).

%%-------------------------------------------------------------------------------------------------
%% Examples
%%-------------------------------------------------------------------------------------------------

example_bundle_list() ->
    #{
        <<"list">> =>
            #{
                summary => <<"List">>,
                value => [#{name => <<"bundle1">>}, #{name => <<"bundle2">>}]
            }
    }.

example_file_list() ->
    #{
        <<"list">> =>
            #{
                summary => <<"List">>,
                value => #{
                    ca => #{path => <<"/path/to/bundle/ca.pem">>},
                    chain => #{path => <<"/path/to/bundle/chain.pem">>},
                    key => #{path => <<"/path/to/bundle/key.pem">>},
                    key_password => #{path => <<"/path/to/bundle/key-password.pem">>},
                    acc_key => #{path => <<"/path/to/bundle/acc-key.pem">>}
                }
            }
    }.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

handle_list_bundles(Namespace) ->
    case emqx_managed_certs:list_bundles(Namespace) of
        {ok, Bundles} ->
            Res = bundles_out(Bundles),
            ?OK(Res);
        {error, enoent} ->
            ?OK([]);
        {error, Reason} ->
            ?INTERNAL_ERROR(emqx_utils:explain_posix(Reason))
    end.

handle_list_files(Namespace, BundleName) ->
    case emqx_managed_certs:list_managed_files(Namespace, BundleName) of
        {ok, Files} ->
            ?OK(Files);
        {error, enoent} ->
            ?NOT_FOUND(<<"Bundle not found">>);
        {error, Reason} ->
            ?INTERNAL_ERROR(emqx_utils:explain_posix(Reason))
    end.

handle_delete_bundle(Namespace, BundleName) ->
    case emqx_managed_certs:delete_bundle(Namespace, BundleName) of
        ok ->
            ?NO_CONTENT;
        {error, Errors} ->
            ?INTERNAL_ERROR(Errors)
    end.

handle_upload_files(Namespace, BundleName, Files) ->
    %% Special case: if an ACME account key exists, we forbid uploading key and chain
    %% files as it's probably an user error, since ACME client will generate these other
    %% kinds.  Updating CA and the account key itself is fine.
    IsUpdatingChainOrKey =
        is_map_key(?FILE_KIND_CHAIN, Files) orelse
            is_map_key(?FILE_KIND_KEY, Files),
    DoesAccKeyExist = does_acc_key_exist(Namespace, BundleName),
    case DoesAccKeyExist andalso IsUpdatingChainOrKey of
        true ->
            ?BAD_REQUEST(<<"Account key exists; chain and key will be managed by ACME client.">>);
        false ->
            case emqx_managed_certs:add_managed_files(Namespace, BundleName, Files) of
                ok ->
                    ?NO_CONTENT;
                {error, Errors} ->
                    ?INTERNAL_ERROR(Errors)
            end
    end.

does_acc_key_exist(Namespace, BundleName) ->
    case emqx_managed_certs:list_managed_files(Namespace, BundleName) of
        {ok, #{?FILE_KIND_ACC_KEY := _}} ->
            true;
        _ ->
            false
    end.

bundles_out(Bundles) ->
    lists:map(fun(Bundle) -> #{<<"name">> => bin(Bundle)} end, Bundles).

mk(Type, Props) -> hoconsc:mk(Type, Props).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).
array(Type) -> hoconsc:array(Type).
bin(X) -> emqx_utils_conv:bin(X).
