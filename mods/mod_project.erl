-module(mod_project).

-export([ get_vsn/1
        , coveralls_configs/1
        ]).

get_vsn(_Conf) ->
  ComparingFun = fun
    _Fun([C1|R1], [C2|R2]) when is_list(C1), is_list(C2);
                                is_integer(C1), is_integer(C2) -> C1 < C2 orelse _Fun(R1, R2);
    _Fun([C1|R1], [C2|R2]) when is_integer(C1), is_list(C2)    -> _Fun(R1, R2);
    _Fun([C1|_], [C2|_]) when is_list(C1), is_integer(C2)    -> true;
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

coveralls_configs(_Config) ->
    case {os:getenv("GITHUB_ACTIONS"), os:getenv("GITHUB_TOKEN")} of
    {"true", Token} when is_list(Token) ->
        CONFIG1 = [
            {coveralls_repo_token, Token},
            {coveralls_service_job_id, os:getenv("GITHUB_RUN_ID")},
            {coveralls_commit_sha, os:getenv("GITHUB_SHA")},
            {coveralls_service_number, os:getenv("GITHUB_RUN_NUMBER")},
            {coveralls_coverdata, "_build/test/cover/*.coverdata"},
            {coveralls_service_name, "github"}
        ],
        case os:getenv("GITHUB_EVENT_NAME") =:= "pull_request"
            andalso string:tokens(os:getenv("GITHUB_REF"), "/") of
            [_, "pull", PRNO, _] ->
                [{coveralls_service_pull_request, PRNO} | CONFIG1];
            _ ->
                CONFIG1
        end;
    _ ->
        []
    end.
