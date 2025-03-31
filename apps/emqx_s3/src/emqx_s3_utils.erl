%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_s3_utils).

-include_lib("xmerl/include/xmerl.hrl").

%% API
-export([
    map_error_details/1
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

map_error_details({s3_error, Code, Message}) ->
    emqx_utils:format("S3 error: ~s ~s", [Code, Message]);
map_error_details({aws_error, Error}) ->
    map_error_details(Error);
map_error_details({socket_error, Reason}) ->
    emqx_utils:format("Socket error: ~s", [emqx_utils:readable_error_msg(Reason)]);
map_error_details({http_error, _, _, _} = Error) ->
    emqx_utils:format("AWS error: ~s", [map_aws_error_details(Error)]);
map_error_details({failed_to_obtain_credentials, Error}) ->
    emqx_utils:format("Unable to obtain AWS credentials: ~s", [map_error_details(Error)]);
map_error_details({upload_failed, Error}) ->
    map_error_details(Error);
map_error_details(Error) ->
    Error.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec map_aws_error_details(_AWSError) ->
    unicode:chardata().
map_aws_error_details({http_error, _Status, _, Body}) ->
    try xmerl_scan:string(unicode:characters_to_list(Body), [{quiet, true}]) of
        {Error = #xmlElement{name = 'Error'}, _} ->
            map_aws_error_details(Error);
        _ ->
            Body
    catch
        exit:_ ->
            Body
    end;
map_aws_error_details(#xmlElement{content = Content}) ->
    Code = extract_xml_text(lists:keyfind('Code', #xmlElement.name, Content)),
    Message = extract_xml_text(lists:keyfind('Message', #xmlElement.name, Content)),
    [Code, $:, $\s | Message].

extract_xml_text(#xmlElement{content = Content}) ->
    [Fragment || #xmlText{value = Fragment} <- Content];
extract_xml_text(false) ->
    [].
