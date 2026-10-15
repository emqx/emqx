#!/usr/bin/env elixir

# Checks that every RPC target a released node can call is still exported by
# this tree.
#
# A proto module (`*_proto_vN.erl`) is a local wrapper: it calls a target
# function on the remote node by name. A released node keeps calling those
# targets for as long as it can be a peer during a rolling upgrade, and some of
# its call sites name a proto module directly instead of negotiating a version.
# So a proto module may be deleted from this tree (see `?FORCE_DELETED_APIS` in
# `emqx_bpapi_static_checks`), but its targets may not.
#
# The targets are read from two sources:
#
#   * the committed BPAPI baselines, as `file:consult/1` reads them;
#   * the proto modules at every GA tag of each line that has a baseline. A
#     baseline is frozen from a branch tip, so it does not hold a proto module
#     the tip had already deleted, while the released tags still call it.
#
# The exports of this tree are read from the `-export` attributes of the
# source files, so nothing is built. Exports inside `-ifdef` blocks count.
# A module that is not in this tree is looked up in OTP.
#
# Usage: check-bpapi-targets.exs [--data-dir DIR] [--no-tags]

defmodule CheckBpapiTargets do
  @data_dir "apps/emqx_bpapi/test/emqx_static_checks_data"
  @src_glob "{apps,plugins}/*/src/**/*.erl"
  @proto_path ~r{/proto/([a-z_0-9]+)_proto_v(\d+)\.erl$}
  @rpc_modules [:rpc, :erpc, :emqx_rpc, :gen_rpc, :emqx_cluster_rpc]

  # {API, Version} whose targets may be missing from this tree. Every entry
  # states why no released node can call the missing targets.
  @exempt_apis %{
    # EMQX 6.0 removed bridges V1 (#15864) and the `emqx_bridge` API. 5.x nodes
    # call these targets only from their REST handlers. During a rolling
    # upgrade such a request fails on the 5.x node. No background task calls
    # them.
    {:emqx_bridge, 1} => :bridge_v1_removed_in_60,
    {:emqx_bridge, 2} => :bridge_v1_removed_in_60,
    {:emqx_bridge, 3} => :bridge_v1_removed_in_60,
    {:emqx_bridge, 4} => :bridge_v1_removed_in_60,
    {:emqx_bridge, 5} => :bridge_v1_removed_in_60,
    {:emqx_bridge, 6} => :bridge_v1_removed_in_60,
    {:emqx_bridge, 7} => :bridge_v1_removed_in_60,
    # A cluster that ran durable sessions on 5.x must be recreated to upgrade
    # to 6.0 (see the 6.0.0 release notes). It cannot roll, so no 5.x node
    # calls a 6.x node through these APIs.
    {:emqx_ds, 1} => :durable_storage_no_rolling_upgrade_from_5x,
    {:emqx_ds, 2} => :durable_storage_no_rolling_upgrade_from_5x,
    {:emqx_ds, 3} => :durable_storage_no_rolling_upgrade_from_5x,
    {:emqx_ds, 4} => :durable_storage_no_rolling_upgrade_from_5x,
    {:emqx_ds, 5} => :durable_storage_no_rolling_upgrade_from_5x,
    {:emqx_ds_otx, 1} => :durable_storage_no_rolling_upgrade_from_5x,
    {:emqx_ds_beamsplitter, 2} => :durable_storage_no_rolling_upgrade_from_5x,
    {:emqx_persistent_session_ds, 1} => :durable_storage_no_rolling_upgrade_from_5x,
    {:emqx_ds_fdb_backend, 1} => :durable_storage_no_rolling_upgrade_from_5x,
    # The `emqx_event_history` application shipped with 5.8 only. Only its
    # REST handler calls this API, so a request to that handler on a 5.8 node
    # fails during a rolling upgrade. No background task calls it.
    {:emqx_event_history, 1} => :event_history_removed_after_58,
    # The relup REST API was removed (#17244). Hot upgrade runs through
    # `emqx ctl relup` on each node. 5.8, 5.9 and 6.0.0 to 6.2.0 call these
    # targets only from that REST handler, so a request to it on such a node
    # fails during a rolling upgrade. No background task calls them.
    {:emqx_mgmt_api_relup, 1} => :relup_rest_api_removed,
    # `start/1` and `stop/1` of these proto modules have no caller on any
    # released line, so `emqx_prometheus:do_start/0` and `do_stop/0` are not
    # reachable (see `?DIVERGED_APIS` in `emqx_bpapi_static_checks`).
    {:emqx_prometheus, 1} => :start_stop_never_called,
    {:emqx_prometheus, 2} => :start_stop_never_called
  }

  def main(args) do
    {opts, _, _} = OptionParser.parse(args, strict: [data_dir: :string, tags: :boolean])
    data_dir = Keyword.get(opts, :data_dir, @data_dir)

    files =
      Path.wildcard("#{data_dir}/*.bpapi*")
      |> Enum.reject(&(Path.basename(&1) |> String.starts_with?("master.")))
      |> Enum.sort()

    if files == [], do: die("No baselines found in #{data_dir}")

    with_tags = Keyword.get(opts, :tags, true)

    edges =
      Enum.flat_map(files, &baseline_edges/1) ++
        if(with_tags, do: tag_edges(files), else: [])

    sources = sources()

    misses =
      edges
      |> Enum.reject(fn {_where, key, _mfa} -> Map.has_key?(@exempt_apis, key) end)
      |> Enum.reject(fn {_where, _key, mfa} -> exported?(mfa, sources) end)
      |> Enum.group_by(fn {_where, key, mfa} -> {key, mfa} end, &elem(&1, 0))
      |> Enum.sort()

    scope =
      "#{length(files)} baselines" <>
        if(with_tags, do: " and the proto modules at their GA tags", else: "")

    report(scope, misses)
  end

  # {Where, {API, Version}, Target MFA} for every call and cast of a baseline.
  defp baseline_edges(file) do
    case :file.consult(String.to_charlist(file)) do
      {:ok, [%{api: api}]} ->
        for {key, %{calls: calls, casts: casts}} <- api,
            {_from, {m, f, args}} <- calls ++ casts,
            do: {Path.basename(file), key, {m, f, length(args)}}

      other ->
        die("Cannot read #{file}: #{inspect(other)}")
    end
  end

  ## Released tags

  defp tag_edges(files) do
    tags = Enum.flat_map(files, &ga_tags/1)

    if tags == [] do
      die("No release tags found. Fetch tags first (a shallow clone has none).")
    end

    # {blob, path, tag} for every proto module at every tag. A proto module
    # does not change after release, so most blobs repeat across tags.
    protos =
      Enum.flat_map(tags, fn tag ->
        git!(["ls-tree", "-r", tag])
        |> String.split("\n", trim: true)
        |> Enum.flat_map(fn line ->
          [meta, path] = String.split(line, "\t", parts: 2)

          case Regex.match?(@proto_path, path) do
            true -> [{meta |> String.split() |> List.last(), path, tag}]
            false -> []
          end
        end)
      end)

    blobs = protos |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> read_blobs()

    targets = Map.new(blobs, fn {blob, src} -> {blob, rpc_targets(src)} end)

    for {blob, path, tag} <- protos,
        [_, api, vsn] = Regex.run(@proto_path, path),
        mfa <- Map.fetch!(targets, blob),
        do: {tag, {String.to_atom(api), String.to_integer(vsn)}, mfa}
  end

  # GA tags of the release line a baseline is named for.
  defp ga_tags(file) do
    line = Path.basename(file) |> String.replace(~r/\.bpapi\d*$/, "")
    prefix = if String.starts_with?(line, "5."), do: "e", else: ""

    git!(["tag", "-l", "#{prefix}#{line}.*"])
    |> String.split("\n", trim: true)
    |> Enum.filter(&String.match?(&1, ~r/^e?\d+\.\d+\.\d+$/))
  end

  defp read_blobs(shas) do
    input = Path.join(System.tmp_dir!(), "bpapi-targets-#{System.os_time()}")
    File.write!(input, Enum.join(shas, "\n") <> "\n")

    try do
      {out, 0} = System.cmd("sh", ["-c", "git cat-file --batch < \"$1\"", "sh", input])
      parse_batch(out, %{})
    after
      File.rm(input)
    end
  end

  # `git cat-file --batch` output: "<sha> blob <size>\n<content>\n" per object.
  defp parse_batch("", acc), do: acc

  defp parse_batch(out, acc) do
    [header, rest] = String.split(out, "\n", parts: 2)
    [sha, "blob", size] = String.split(header, " ")
    size = String.to_integer(size)
    <<content::binary-size(size), "\n", rest::binary>> = rest
    parse_batch(rest, Map.put(acc, sha, content))
  end

  # Target MFAs of the RPC calls in a proto module source. Every RPC function
  # takes the module, the function and the argument list as three adjacent
  # arguments; a call whose arguments are not literals there is skipped.
  defp rpc_targets(src) do
    {:ok, io} = StringIO.open(expand_atom_macros(src))
    {:ok, forms} = :epp_dodger.quick_parse(io)
    StringIO.close(io)

    forms
    |> Enum.flat_map(fn form ->
      :erl_syntax_lib.fold(fn node, acc -> rpc_target(node) ++ acc end, [], form)
    end)
    |> Enum.uniq()
  end

  # Replaces `?NAME` with the atom of a `-define(NAME, atom).` in the same
  # source, so a call through such a macro names its target module.
  defp expand_atom_macros(src) do
    Regex.scan(~r/^-define\((\w+),\s*([a-z][a-zA-Z0-9_]*)\)\./m, src)
    |> Enum.reduce(src, fn [_, name, atom], acc ->
      String.replace(acc, ~r/\?#{name}\b/, atom)
    end)
  end

  defp rpc_target(node) do
    with :application <- :erl_syntax.type(node),
         op = :erl_syntax.application_operator(node),
         :module_qualifier <- :erl_syntax.type(op),
         mod = :erl_syntax.module_qualifier_argument(op),
         :atom <- :erl_syntax.type(mod),
         true <- :erl_syntax.atom_value(mod) in @rpc_modules do
      node |> :erl_syntax.application_arguments() |> mfa_args()
    else
      _ -> []
    end
  end

  defp mfa_args([m, f, a | rest]) do
    with :atom <- :erl_syntax.type(m),
         :atom <- :erl_syntax.type(f),
         true <- :erl_syntax.type(a) in [:list, nil],
         true <- :erl_syntax.is_proper_list(a) do
      [{:erl_syntax.atom_value(m), :erl_syntax.atom_value(f), :erl_syntax.list_length(a)}]
    else
      _ -> mfa_args([f, a | rest])
    end
  end

  defp mfa_args(_), do: []

  ## This tree

  # Module name -> source path, for every Erlang module in this tree.
  defp sources do
    Path.wildcard(@src_glob)
    |> Map.new(fn path -> {String.to_atom(Path.basename(path, ".erl")), path} end)
  end

  defp exported?({m, _, _} = mfa, sources) do
    key = {__MODULE__, :exports, m}

    exports =
      case Process.get(key) do
        nil ->
          exports = exports(m, Map.get(sources, m))
          Process.put(key, exports)
          exports

        exports ->
          exports
      end

    MapSet.member?(exports, mfa)
  end

  defp exports(m, nil) do
    case :code.ensure_loaded(m) do
      {:module, ^m} -> MapSet.new(m.module_info(:exports), fn {f, a} -> {m, f, a} end)
      _ -> MapSet.new()
    end
  end

  defp exports(m, path) do
    case :epp_dodger.quick_parse_file(String.to_charlist(path)) do
      {:ok, forms} ->
        for form <- forms,
            :attribute == :erl_syntax.type(form),
            :export == :erl_syntax.atom_value(:erl_syntax.attribute_name(form)),
            [list] = :erl_syntax.attribute_arguments(form),
            fa <- :erl_syntax.list_elements(list),
            into: MapSet.new() do
          {:erl_syntax.atom_value(:erl_syntax.arity_qualifier_body(fa)),
           :erl_syntax.integer_value(:erl_syntax.arity_qualifier_argument(fa))}
          |> then(fn {f, a} -> {m, f, a} end)
        end

      {:error, reason} ->
        die("Cannot parse #{path}: #{inspect(reason)}")
    end
  end

  ## Report

  defp report(scope, []) do
    IO.puts("BPAPI targets ok: checked #{scope}")
  end

  defp report(_scope, misses) do
    Enum.each(misses, fn {{{api, vsn}, {m, f, a}}, wheres} ->
      IO.puts("  #{api} v#{vsn} calls #{m}:#{f}/#{a}, which this tree does not export")
      IO.puts("      called by: #{wheres |> Enum.uniq() |> Enum.sort() |> Enum.join(" ")}")
    end)

    IO.puts("""

    #{length(misses)} RPC target(s) that a released node calls are not exported.

    During a rolling upgrade such a node calls the target by name and gets
    `undef`. Restore the export. When no released node can reach the target,
    add the API to @exempt_apis in #{Path.relative_to_cwd(__ENV__.file)} and
    state the reason.
    """)

    System.halt(1)
  end

  defp git!(args) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {out, 0} -> out
      {out, _} -> die("git #{Enum.join(args, " ")} failed:\n#{out}")
    end
  end

  defp die(msg) do
    IO.puts(:stderr, msg)
    System.halt(1)
  end
end

CheckBpapiTargets.main(System.argv())
