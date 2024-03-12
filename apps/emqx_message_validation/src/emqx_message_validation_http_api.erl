%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_message_validation_http_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").

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
    '/message_validations'/2,
    '/message_validations/:name'/2,
    '/message_validations/:name/move'/2
]).

%%-------------------------------------------------------------------------------------------------
%% Type definitions
%%-------------------------------------------------------------------------------------------------

-define(TAGS, [<<"Message Validation">>]).

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

namespace() -> "message_validation_http_api".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/message_validations",
        "/message_validations/:name",
        "/message_validations/:name/move"
    ].

schema("/message_validations") ->
    #{
        'operationId' => '/message_validations',
        get => #{
            tags => ?TAGS,
            summary => <<"List validations">>,
            description => ?DESC("list_validations"),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            hoconsc:array(
                                emqx_message_validation_schema:api_schema(list)
                            ),
                            #{
                                sample =>
                                    #{value => example_return_list()}
                            }
                        )
                }
        },
        post => #{
            tags => ?TAGS,
            summary => <<"Append a new validation">>,
            description => ?DESC("append_validation"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_message_validation_schema:api_schema(post),
                example_input_create()
            ),
            responses =>
                #{
                    201 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_message_validation_schema:api_schema(post),
                            example_return_create()
                        ),
                    400 => error_schema('ALREADY_EXISTS', "Validation already exists")
                }
        },
        put => #{
            tags => ?TAGS,
            summary => <<"Update a validation">>,
            description => ?DESC("update_validation"),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_message_validation_schema:api_schema(put),
                example_input_update()
            ),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            emqx_message_validation_schema:api_schema(put),
                            example_return_update()
                        ),
                    404 => error_schema('NOT_FOUND', "Validation not found"),
                    400 => error_schema('BAD_REQUEST', "Bad params")
                }
        }
    };
schema("/message_validations/:name") ->
    #{
        'operationId' => '/message_validations/:name',
        get => #{
            tags => ?TAGS,
            summary => <<"Lookup a validation">>,
            description => ?DESC("lookup_validation"),
            parameters => [param_path_name()],
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            hoconsc:array(
                                emqx_message_validation_schema:api_schema(lookup)
                            ),
                            #{
                                sample =>
                                    #{value => example_return_lookup()}
                            }
                        ),
                    404 => error_schema('NOT_FOUND', "Validation not found")
                }
        },
        delete => #{
            tags => ?TAGS,
            summary => <<"Delete a validation">>,
            description => ?DESC("delete_validation"),
            parameters => [param_path_name()],
            responses =>
                #{
                    204 => <<"Validation deleted">>,
                    404 => error_schema('NOT_FOUND', "Validation not found")
                }
        }
    };
schema("/message_validations/:name/move") ->
    #{
        'operationId' => '/message_validations/:name/move',
        post => #{
            tags => ?TAGS,
            summary => <<"Change the order of a validation">>,
            description => ?DESC("move_validation"),
            parameters => [param_path_name()],
            'requestBody' =>
                emqx_dashboard_swagger:schema_with_examples(
                    hoconsc:union(fun position_union_member_selector/1),
                    example_position()
                ),
            responses =>
                #{
                    204 => <<"No Content">>,
                    400 => error_schema('BAD_REQUEST', <<"Bad request">>),
                    404 => error_schema('NOT_FOUND', "Validation not found")
                }
        }
    }.

param_path_name() ->
    {name,
        mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"my_validation">>,
                desc => ?DESC("param_path_name")
            }
        )}.

fields(front) ->
    [{position, mk(front, #{default => front, required => true, in => body})}];
fields(rear) ->
    [{position, mk(rear, #{default => rear, required => true, in => body})}];
fields('after') ->
    [
        {position, mk('after', #{default => 'after', required => true, in => body})},
        {validation, mk(binary(), #{required => true, in => body})}
    ];
fields(before) ->
    [
        {position, mk(before, #{default => before, required => true, in => body})},
        {validation, mk(binary(), #{required => true, in => body})}
    ].

%%-------------------------------------------------------------------------------------------------
%% `minirest' handlers
%%-------------------------------------------------------------------------------------------------

'/message_validations'(get, _Params) ->
    ?OK(emqx_message_validation:list());
'/message_validations'(post, #{body := Params = #{<<"name">> := Name}}) ->
    with_validation(
        Name,
        return(?BAD_REQUEST('ALREADY_EXISTS', <<"Validation already exists">>)),
        fun() ->
            case emqx_message_validation:insert(Params) of
                {ok, _} ->
                    {ok, Res} = emqx_message_validation:lookup(Name),
                    {201, Res};
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
        end
    );
'/message_validations'(put, #{body := Params = #{<<"name">> := Name}}) ->
    with_validation(
        Name,
        fun() ->
            case emqx_message_validation:update(Params) of
                {ok, _} ->
                    {ok, Res} = emqx_message_validation:lookup(Name),
                    {200, Res};
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
        end,
        not_found()
    ).

'/message_validations/:name'(get, #{bindings := #{name := Name}}) ->
    with_validation(
        Name,
        fun(Validation) -> ?OK(Validation) end,
        not_found()
    );
'/message_validations/:name'(delete, #{bindings := #{name := Name}}) ->
    with_validation(
        Name,
        fun() ->
            case emqx_message_validation:delete(Name) of
                {ok, _} ->
                    ?NO_CONTENT;
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
        end,
        not_found()
    ).

'/message_validations/:name/move'(post, #{bindings := #{name := Name}, body := Body}) ->
    with_validation(
        Name,
        fun() ->
            do_move(Name, parse_position(Body))
        end,
        not_found(Name)
    ).

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

ref(Struct) -> hoconsc:ref(?MODULE, Struct).
mk(Type, Opts) -> hoconsc:mk(Type, Opts).

example_input_create() ->
    %% TODO
    #{}.

example_input_update() ->
    %% TODO
    #{}.

example_return_list() ->
    %% TODO
    [].

example_return_create() ->
    %% TODO
    #{}.

example_return_update() ->
    %% TODO
    #{}.

example_return_lookup() ->
    %% TODO
    #{}.

example_position() ->
    %% TODO
    #{}.

error_schema(Code, Message) when is_atom(Code) ->
    error_schema([Code], Message);
error_schema(Codes, Message) when is_list(Message) ->
    error_schema(Codes, list_to_binary(Message));
error_schema(Codes, Message) when is_list(Codes) andalso is_binary(Message) ->
    emqx_dashboard_swagger:error_codes(Codes, Message).

position_union_member_selector(all_union_members) ->
    position_refs();
position_union_member_selector({value, V}) ->
    position_refs(V).

position_refs() ->
    [].

position_types() ->
    [
        front,
        rear,
        'after',
        before
    ].

position_refs(#{<<"position">> := <<"front">>}) ->
    [ref(front)];
position_refs(#{<<"position">> := <<"rear">>}) ->
    [ref(rear)];
position_refs(#{<<"position">> := <<"after">>}) ->
    [ref('after')];
position_refs(#{<<"position">> := <<"before">>}) ->
    [ref(before)];
position_refs(_) ->
    Expected = lists:join(" | ", [atom_to_list(T) || T <- position_types()]),
    throw(#{
        field_name => position,
        expected => iolist_to_binary(Expected)
    }).

%% Schema is already checked, so we don't need to do further validation.
parse_position(#{<<"position">> := <<"front">>}) ->
    front;
parse_position(#{<<"position">> := <<"rear">>}) ->
    rear;
parse_position(#{<<"position">> := <<"after">>, <<"validation">> := OtherValidationName}) ->
    {'after', OtherValidationName};
parse_position(#{<<"position">> := <<"before">>, <<"validation">> := OtherValidationName}) ->
    {before, OtherValidationName}.

do_move(ValidationName, {_, OtherValidationName} = Position) ->
    with_validation(
        OtherValidationName,
        fun() ->
            case emqx_message_validation:move(ValidationName, Position) of
                {ok, _} ->
                    ?NO_CONTENT;
                {error, Error} ->
                    ?BAD_REQUEST(Error)
            end
        end,
        bad_request_not_found(OtherValidationName)
    );
do_move(ValidationName, Position) ->
    case emqx_message_validation:move(ValidationName, Position) of
        {ok, _} ->
            ?NO_CONTENT;
        {error, Error} ->
            ?BAD_REQUEST(Error)
    end.

with_validation(Name, FoundFn, NotFoundFn) ->
    case emqx_message_validation:lookup(Name) of
        {ok, Validation} ->
            {arity, Arity} = erlang:fun_info(FoundFn, arity),
            case Arity of
                1 -> FoundFn(Validation);
                0 -> FoundFn()
            end;
        {error, not_found} ->
            NotFoundFn()
    end.

return(Response) ->
    fun() -> Response end.

not_found() ->
    return(?NOT_FOUND(<<"Validation not found">>)).

not_found(Name) ->
    return(?NOT_FOUND(<<"Validation not found: ", Name/binary>>)).

%% After we found the base validation, but not the other one being referenced in a move.
bad_request_not_found(Name) ->
    return(?BAD_REQUEST(<<"Validation not found: ", Name/binary>>)).
