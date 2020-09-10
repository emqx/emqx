-module('rebar.config.mod').

-export([get_vsn/1, render/1]).

get_vsn(_Conf) ->
  ComparingFun = fun
    _Fun([C1|R1], [C2|R2]) when is_list(C1), is_list(C2);
                                is_integer(C1), is_integer(C2) -> C1 < C2 orelse _Fun(R1, R2);
    _Fun([C1|R1], [C2|R2]) when is_integer(C1), is_list(C2)    -> _Fun(R1, R2);
    _Fun([C1|R1], [C2|R2]) when is_list(C1), is_integer(C2)    -> true;
    _Fun(_, _) -> false
  end,
  SortFun = fun(T1, T2) ->
    C = fun(T) ->
          [case catch list_to_integer(E) of
              I when is_integer(I) -> I;
              _ -> E
            end || E <- re:split(string:sub_string(T, 2), "[.-]", [{return, list}])]
        end,
    ComparingFun(C(T1), C(T2))
  end,
  Tag = os:cmd("git describe --abbrev=0 --tags") -- "\n",
  LatestTagCommitId = os:cmd(io_lib:format("git rev-parse ~s", [Tag])) -- "\n",
  Tags = string:tokens(os:cmd(io_lib:format("git tag -l \"v*\" --points-at ~s", [LatestTagCommitId])), "\n"),
  LatestTag = lists:last(lists:sort(SortFun, Tags)),
  Branch = case os:getenv("GITHUB_RUN_ID") of
                  false -> os:cmd("git branch | grep -e '^*' | cut -d' ' -f 2") -- "\n";
                  _ -> re:replace(os:getenv("GITHUB_REF"), "^refs/heads/|^refs/tags/", "", [global, {return ,list}])
          end,
  GitRef =  case re:run(Branch, "master|^dev/|^hotfix/", [{capture, none}]) of
                match -> {branch, Branch};
                _ -> {tag, LatestTag}
            end,
  DefaultDepRef =
      case os:getenv("EMQX_DEPS_DEFAULT_VSN") of
          false -> GitRef; %% not set
          "" -> GitRef; %% set empty
          [] -> GitRef; %% set empty
          MaybeTag ->
              case re:run(MaybeTag, "^[ev0-9\]+\.\[0-9\]+\.*") of
                  nomatch -> {branch, MaybeTag};
                  _ -> {tag, MaybeTag}
              end
      end,
  T = case DefaultDepRef of
          {tag, EnvTag} -> EnvTag;
          _Else -> LatestTag
      end,
  re:replace(T, "v", "", [{return ,list}]).

render(Config) ->
    PlcHdlrs = proplists:get_value(rebar_place_holders, Config),
    RealConfig = proplists:delete(rebar_place_holders, Config),
    [render_entry(Entry, PlcHdlrs, RealConfig) || Entry <- RealConfig].

render_entry(Entry, PlcHdlrs, Config) when is_tuple(Entry) ->
    list_to_tuple(render_entry(tuple_to_list(Entry), PlcHdlrs, Config));
render_entry(Entry, PlcHdlrs, Config) when is_list(Entry) ->
    io:format("render_entry: ~p~n", [Entry]),
    lists:foldl(fun(Item, Acc) ->
            case render_item(Item, PlcHdlrs, Config) of
                {var, Fun} when is_function(Fun) ->
                    Acc ++ [Fun(Config)];
                {var, Var} ->
                    Acc ++ [Var];
                {elems, Elems} ->
                    Acc ++ render_entry(Elems, PlcHdlrs, Config)
            end
        end, [], Entry);
render_entry(Entry, _PlcHdlrs, _Config) ->
    Entry.

render_item("${"++Key0 = Entry0, PlcHdlrs, Config) ->
    Key = string:trim(Key0, trailing, "}"),
    case lists:keyfind(Key, 1, PlcHdlrs) of
        false -> {var, Entry0};
        {_, Type, Entry} ->
            {Type, render_entry(Entry, PlcHdlrs, Config)}
    end;
render_item(Entry, PlcHdlrs, Config) when is_tuple(Entry); is_list(Entry) ->
    {var, render_entry(Entry, PlcHdlrs, Config)};
render_item(Entry, _PlcHdlrs, _Config) ->
    {var, Entry}.
