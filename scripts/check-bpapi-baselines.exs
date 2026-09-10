#!/usr/bin/env elixir

# Checks that every committed BPAPI baseline describes the release line it is
# named for.
#
# A baseline is frozen from a branch tip, so it may hold more than the newest
# released tag on its line. It may not hold less: proto versions only
# accumulate, so every one present at that tag has to be in the file. A baseline
# that is short of them was taken from the wrong tree -- which has happened three
# times, each time going unnoticed for months, because nothing read the file
# closely enough to tell.
#
# Two absences are legitimate. A proto module that makes no RPC call is never in
# a dump: those exist only to be announced in `bpapi.versions` so callers can
# gate on the version, and `dump_api/1` records calls. And a module the tag has
# but the working tree does not has been deleted since, so a baseline frozen
# after that deletion is right not to hold it.
#
# That second rule is why this compares against the working tree rather than
# reading `?FORCE_DELETED_APIS`: a deletion is visible in the tree, and the tree
# cannot fall out of step with itself the way a copied list can. It also keeps
# the check to git and `file:consult/1`, with nothing to build and no Erlang
# source to parse.

defmodule CheckBpapiBaselines do
  @data_dir "apps/emqx_bpapi/test/emqx_static_checks_data"
  @proto_glob "{apps,plugins}/*/src/**/*_proto_v*.erl"
  @rpc_call ~r/\b(rpc|erpc|emqx_rpc|emqx_cluster_rpc):/

  def main do
    case baselines() do
      [] ->
        die("No baselines found in #{@data_dir}")

      files ->
        present = protos_in_tree()
        results = Enum.map(files, &check(&1, present))
        report(results)
    end
  end

  defp baselines do
    Path.wildcard("#{@data_dir}/*.bpapi*")
    |> Enum.reject(&(Path.basename(&1) |> String.starts_with?("master.")))
    |> Enum.sort()
  end

  # "5.10.bpapi2" -> "5.10"
  defp line_of(file), do: Path.basename(file) |> String.replace(~r/\.bpapi\d*$/, "")

  # 5.x release tags carry an `e` prefix; 6.0 onwards do not.
  defp tag_prefix(line) do
    case String.split(line, ".") do
      ["5" | _] -> "e"
      _ -> ""
    end
  end

  defp newest_ga_tag(line) do
    pattern = "#{tag_prefix(line)}#{line}.*"

    case git(["tag", "-l", pattern]) do
      {:ok, out} ->
        out
        |> String.split("\n", trim: true)
        |> Enum.reject(&String.match?(&1, ~r/-(rc|alpha|beta|patch)/))
        |> Enum.sort_by(&version_key/1)
        |> List.last()

      :error ->
        nil
    end
  end

  defp version_key(tag) do
    tag
    |> String.replace(~r/^e/, "")
    |> String.split(".")
    |> Enum.map(&(Integer.parse(&1) |> elem(0)))
  end

  # {api, version} for every proto module present at a tag, with its path.
  #
  # A tag object can be listed by `git tag -l` while its commit is absent, which
  # is what a shallow clone looks like. `ls-tree` then fails, and returning an
  # empty list here would report the baseline as ok having compared it against
  # nothing. Every real tag has proto modules, so treat both as errors.
  defp protos_at(tag) do
    case git(["ls-tree", "-r", "--name-only", tag]) do
      {:ok, out} ->
        out
        |> String.split("\n", trim: true)
        |> Enum.flat_map(fn path ->
          case proto_key(path) do
            nil -> []
            key -> [{key, path}]
          end
        end)
        |> case do
          [] -> die("No proto modules at #{tag}; is the tag's commit present?")
          protos -> protos
        end

      :error ->
        die("Cannot read #{tag}; the tag is listed but its commit is missing")
    end
  end

  defp proto_key(path) do
    case Regex.run(~r{/proto/([a-z_0-9]+)_proto_v(\d+)\.erl$}, path) do
      [_, api, vsn] -> {String.to_atom(api), String.to_integer(vsn)}
      nil -> nil
    end
  end

  defp makes_rpc_call?(tag, path) do
    case git(["show", "#{tag}:#{path}"]) do
      {:ok, src} -> String.match?(src, @rpc_call)
      :error -> true
    end
  end

  defp baseline_keys(file) do
    case :file.consult(String.to_charlist(file)) do
      {:ok, [dump]} -> dump |> Map.fetch!(:api) |> Map.keys() |> MapSet.new()
      other -> die("Cannot read #{file}: #{inspect(other)}")
    end
  end

  # Proto modules that still exist. A tag may name one this tree has deleted;
  # that absence from a baseline frozen later is correct.
  defp protos_in_tree do
    case Path.wildcard(@proto_glob) do
      [] -> die("Found no proto modules under #{@proto_glob}; the layout must have changed")
      paths -> paths |> Enum.map(&proto_key/1) |> Enum.reject(&is_nil/1) |> MapSet.new()
    end
  end

  defp check(file, present) do
    line = line_of(file)

    case newest_ga_tag(line) do
      nil ->
        {:skipped, line, "no released tag yet"}

      tag ->
        keys = baseline_keys(file)

        missing =
          protos_at(tag)
          |> Enum.reject(fn {key, _path} -> MapSet.member?(keys, key) end)
          |> Enum.filter(fn {key, _path} -> MapSet.member?(present, key) end)
          |> Enum.filter(fn {_key, path} -> makes_rpc_call?(tag, path) end)
          |> Enum.map(&elem(&1, 0))
          |> Enum.sort()

        checked = protos_at(tag) |> length()

        case missing do
          [] -> {:ok, line, tag, checked}
          _ -> {:error, line, tag, missing}
        end
    end
  end

  defp report(results) do
    if Enum.all?(results, &match?({:skipped, _, _}, &1)) do
      die(
        "No release tags found, so no baseline could be checked.\n" <>
          "Fetch tags first (a shallow clone has none)."
      )
    end

    Enum.each(results, fn
      {:ok, line, tag, checked} ->
        IO.puts("  #{line}: ok against #{tag}, #{checked} versions compared")

      {:skipped, line, why} ->
        IO.puts("  #{line}: skipped, #{why}")

      {:error, line, tag, missing} ->
        IO.puts("  #{line}: FAILED against #{tag}")

        Enum.each(missing, fn {api, vsn} ->
          IO.puts("      #{api} v#{vsn} is at the tag but not in the baseline")
        end)
    end)

    case Enum.filter(results, &match?({:error, _, _, _}, &1)) do
      [] ->
        IO.puts("BPAPI baselines ok")

      errors ->
        IO.puts("""

        #{length(errors)} baseline(s) do not describe their release line.

        A baseline may hold more than its newest released tag, never less. The
        versions above are at the tag and still in this tree, so the baseline was
        generated from the wrong one; rebuild it from that line and commit the
        result. See apps/emqx_bpapi/README.md.
        """)

        System.halt(1)
    end
  end

  defp git(args) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {out, 0} -> {:ok, out}
      _ -> :error
    end
  end

  defp die(msg) do
    IO.puts(:stderr, msg)
    System.halt(1)
  end
end

CheckBpapiBaselines.main()
