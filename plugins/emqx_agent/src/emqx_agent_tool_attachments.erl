%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_attachments).

-moduledoc """
Extracts images from JSON/binary data.

OpenAI API does not allow images in tool responses; multimodal inputs must be
sent separately.

`process/2` takes a decoded data value and options describing how images should
be extracted:

```erlang
Opts = #{
    autodiscover_images => boolean(),
    images => [binary()],
    content_type => undefined | binary()
}
```

- `autodiscover_images` scans the payload for image-looking strings of the form
  `data:image/...;base64,...`.
- `images` contains explicit
  paths to try as images, for example `<<".image_url">>` or `<<".">>` for the
  root value.
- `content_type` is used for binary payload responses, e.g.`<<"image/png">>`.

When an image is extracted, the value in the data structure is replaced with an
`Image <id>` binary, and the image is returned separately:

```erlang
Data = #{
    <<"image_url">> => <<"data:image/png;base64,AAA">>
},
{ok, Sanitized, Attachments} = process(Data, #{
    autodiscover_images => true,
    images => [],
    content_type => undefined
}),
Sanitized = #{<<"image_url">> => <<"Image .image_url">>},
Attachments = [#{
    <<"id">> => <<".image_url">>,
    <<"type">> => <<"image">>,
    <<"mime_type">> => <<"image/png">>,
    <<"data">> => <<"AAA">>
}].
```

For binary image responses, callers can either pass an image content type:

```erlang
{ok, <<"Image .">>, [Attachment]} = process(PngBytes, #{
    autodiscover_images => true,
    images => [],
    content_type => <<"image/png">>
}).
```
""".

-export([process/2]).

-spec process(term(), map()) -> {ok, term(), [map()]}.
process(
    Data,
    #{
        autodiscover_images := AutodiscoverImages,
        images := Images,
        content_type := ContentType
    }
) when
    is_boolean(AutodiscoverImages),
    is_list(Images),
    ContentType =:= undefined orelse is_binary(ContentType)
->
    {Data1, Attachments} = extract(Data, AutodiscoverImages, Images, ContentType),
    {ok, Data1, Attachments}.

extract(Payload, AutodiscoverImages, Images, ContentType0) ->
    {Payload1, Attachments0} = extract_explicit_paths(Images, Payload, #{}),
    {Payload2, Attachments1} =
        case AutodiscoverImages of
            true -> autodiscover(Payload1, Attachments0);
            false -> {Payload1, Attachments0}
        end,
    ContentType = normalize_content_type(ContentType0),
    {Payload3, Attachments2} = maybe_extract_binary_image_root(Payload2, ContentType, Attachments1),
    {Payload3, attachments_to_list(Attachments2)}.

normalize_content_type(undefined) ->
    undefined;
normalize_content_type(ContentType) when is_binary(ContentType) ->
    [Type | _] = binary:split(ContentType, <<";">>),
    string:lowercase(trim(Type)).

extract_explicit_paths([], Payload, Attachments) ->
    {Payload, Attachments};
extract_explicit_paths([Path | Rest], Payload0, Attachments0) ->
    {Payload1, Attachments1} = extract_path(Path, Payload0, Attachments0),
    extract_explicit_paths(Rest, Payload1, Attachments1).

extract_path(<<".">> = Path, Payload, Attachments) ->
    maybe_extract_explicit_value(Path, Payload, Attachments);
extract_path(<<".", _/binary>> = Path, Payload, Attachments) ->
    Segments = path_segments(Path),
    case replace_at_path(Segments, Path, Payload, Attachments) of
        {ok, Payload1, Attachments1} -> {Payload1, Attachments1};
        error -> {Payload, Attachments}
    end;
extract_path(_Path, Payload, Attachments) ->
    {Payload, Attachments}.

path_segments(<<".">>) ->
    [];
path_segments(<<".", Rest/binary>>) ->
    binary:split(Rest, <<".">>, [global]);
path_segments(_) ->
    [].

replace_at_path([], Path, Value, Attachments) ->
    {Value1, Attachments1} = maybe_extract_explicit_value(Path, Value, Attachments),
    case Attachments1 =:= Attachments of
        true -> error;
        false -> {ok, Value1, Attachments1}
    end;
replace_at_path([Seg | Rest], Path, Map, Attachments) when is_map(Map) ->
    case Map of
        #{Seg := Value} ->
            case replace_at_path(Rest, Path, Value, Attachments) of
                {ok, Value1, Attachments1} -> {ok, Map#{Seg => Value1}, Attachments1};
                error -> error
            end;
        #{} ->
            error
    end;
replace_at_path([Seg | Rest], Path, List, Attachments) when is_list(List) ->
    case parse_index(Seg) of
        {ok, Index} when Index >= 0, Index < length(List) ->
            Value = lists:nth(Index + 1, List),
            case replace_at_path(Rest, Path, Value, Attachments) of
                {ok, Value1, Attachments1} -> {ok, replace_nth(Index, Value1, List), Attachments1};
                error -> error
            end;
        _ ->
            error
    end;
replace_at_path(_, _, _, _) ->
    error.

autodiscover(Payload, Attachments) ->
    walk(Payload, [], Attachments).

walk(Value, PathRev, Attachments) when is_binary(Value) ->
    maybe_extract_value(PathRev, Value, fun(NewValue) -> NewValue end, Attachments);
walk(Map, PathRev, Attachments0) when is_map(Map) ->
    maps:fold(
        fun(Key, Value, {MapAcc, AttachmentsAcc}) ->
            ChildPathRev = [Key | PathRev],
            {Value1, Attachments1} = walk(Value, ChildPathRev, AttachmentsAcc),
            {MapAcc#{Key => Value1}, Attachments1}
        end,
        {Map, Attachments0},
        Map
    );
walk(List, PathRev, Attachments0) when is_list(List) ->
    {Rev, Attachments1, _Index} = lists:foldl(
        fun(Value, {Acc, AttachmentsAcc, Index}) ->
            ChildPathRev = [integer_to_binary(Index) | PathRev],
            {Value1, Attachments1} = walk(Value, ChildPathRev, AttachmentsAcc),
            {[Value1 | Acc], Attachments1, Index + 1}
        end,
        {[], Attachments0, 0},
        List
    ),
    {lists:reverse(Rev), Attachments1};
walk(Value, _Path, Attachments) ->
    {Value, Attachments}.

maybe_extract_binary_image_root(Payload, ContentType, Attachments) when is_binary(Payload) ->
    case is_image_content_type(ContentType) andalso not is_data_image_url(Payload) of
        true ->
            Attachment = image_attachment(<<".">>, ContentType, base64:encode(Payload)),
            {attachment_ref(<<".">>), maps:put(<<".">>, Attachment, Attachments)};
        false ->
            {Payload, Attachments}
    end;
maybe_extract_binary_image_root(Payload, _ContentType, Attachments) ->
    {Payload, Attachments}.

maybe_extract_value(PathRev, Value, Replace, Attachments) when is_binary(Value) ->
    case parse_data_image_url(Value) of
        {ok, MimeType, Data} ->
            Path = path_id(PathRev),
            Attachment = image_attachment(Path, MimeType, Data),
            {Replace(attachment_ref(Path)), maps:put(Path, Attachment, Attachments)};
        error ->
            {Value, Attachments}
    end;
maybe_extract_value(_Path, Value, _Replace, Attachments) ->
    {Value, Attachments}.

maybe_extract_explicit_value(Path, Value, Attachments) when is_binary(Value) ->
    case parse_data_image_url(Value) of
        {ok, MimeType, Data} ->
            Attachment = image_attachment(Path, MimeType, Data),
            {attachment_ref(Path), maps:put(Path, Attachment, Attachments)};
        error ->
            case sniff_image_binary(Value) of
                {ok, MimeType} ->
                    Attachment = image_attachment(Path, MimeType, base64:encode(Value)),
                    {attachment_ref(Path), maps:put(Path, Attachment, Attachments)};
                error ->
                    {Value, Attachments}
            end
    end;
maybe_extract_explicit_value(_Path, Value, Attachments) ->
    {Value, Attachments}.

parse_data_image_url(<<"data:", Rest/binary>>) ->
    maybe
        [Meta, Data] ?= binary:split(Rest, <<",">>),
        [MimeType, <<"base64">>] ?= binary:split(string:lowercase(Meta), <<";">>, [global]),
        true ?= is_image_content_type(MimeType),
        {ok, MimeType, Data}
    else
        _ -> error
    end;
parse_data_image_url(_) ->
    error.

is_data_image_url(Value) ->
    parse_data_image_url(Value) =/= error.

image_attachment(Id, MimeType, Data) ->
    #{
        <<"id">> => Id,
        <<"type">> => <<"image">>,
        <<"mime_type">> => MimeType,
        <<"data">> => Data
    }.

attachment_ref(Id) ->
    <<"Image ", Id/binary>>.

attachments_to_list(Attachments) ->
    [maps:get(Id, Attachments) || Id <- lists:sort(maps:keys(Attachments))].

is_image_content_type(undefined) -> false;
is_image_content_type(<<"image/", _/binary>>) -> true;
is_image_content_type(_) -> false.

sniff_image_binary(<<137, 80, 78, 71, 13, 10, 26, 10, _/binary>>) -> {ok, <<"image/png">>};
sniff_image_binary(<<255, 216, 255, _/binary>>) -> {ok, <<"image/jpeg">>};
sniff_image_binary(<<"GIF87a", _/binary>>) -> {ok, <<"image/gif">>};
sniff_image_binary(<<"GIF89a", _/binary>>) -> {ok, <<"image/gif">>};
sniff_image_binary(<<"RIFF", _Size:32/little, "WEBP", _/binary>>) -> {ok, <<"image/webp">>};
sniff_image_binary(_) -> error.

path_id([]) ->
    <<".">>;
path_id(PathRev) ->
    iolist_to_binary([<<".">>, lists:join(<<".">>, lists:reverse(PathRev))]).

parse_index(Seg) ->
    try
        {ok, binary_to_integer(Seg)}
    catch
        _:_ -> error
    end.

replace_nth(Index, Value, List) ->
    {Left, [_Old | Right]} = lists:split(Index, List),
    Left ++ [Value | Right].

trim(Bin) ->
    iolist_to_binary(string:trim(Bin)).
