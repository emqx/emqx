#!/usr/bin/env elixir

defmodule CheckMixExsDiscrepancies do
  @moduledoc """
  This compares the build info contained in rebar3 format with the new mix build info.

  After we switch to mix-only builds, this script can be dropped.
  """

  def this_file() do
    root = __DIR__ |> Path.join("..")

    __ENV__.file
    |> Path.relative_to(root)
  end

  def apps() do
    Path.wildcard("apps/*")
    |> MapSet.new(fn path ->
      [_, app] = Path.split(path)
      String.to_atom(app)
    end)
  end

  def rebar_configs() do
    Path.wildcard("apps/*/rebar.config")
    |> Map.new(fn path ->
      [_, app, _] = Path.split(path)
      app = String.to_atom(app)
      {app, path}
    end)
  end

  def mix_infos() do
    Path.wildcard("apps/*/mix.exs")
    |> Map.new(fn path ->
      [_, app, _] = Path.split(path)
      app = String.to_atom(app)
      {app, read_mix_exs_info(path)}
    end)
  end

  def read_mix_exs_info(mix_exs_path) do
    mod =
      case Code.require_file(mix_exs_path) do
        [{mod, _}] ->
          :persistent_term.put(mix_exs_path, mod)
          mod

        nil ->
          ## useful when evaluating this in a repl
          :persistent_term.get(mix_exs_path)
      end

    %{
      mix_exs_path: mix_exs_path,
      project: mod.project(),
      application: mod.application(),
      deps: mod.deps()
    }
  end

  def app_srcs() do
    Path.wildcard("apps/*/src/*.app.src")
    |> Map.new(fn path ->
      [_, app | _] = Path.split(path)
      app = String.to_atom(app)
      {app, path}
    end)
  end

  def rebar_infos() do
    rebar_configs = rebar_configs()
    app_srcs = app_srcs()

    Enum.reduce(app_srcs, %{}, fn {app, app_src_path}, acc ->
      rebar_config_path = Map.fetch!(rebar_configs, app)
      {:ok, [{:application, ^app, app_src}]} = :file.consult(app_src_path)
      {:ok, rebar_config} = :file.consult(rebar_config_path)
      Map.put(acc, app, %{app_src: app_src, rebar_config: rebar_config})
    end)
  end

  def check_app_vsn_mismatches(mix_infos, rebar_infos) do
    Enum.reduce(rebar_infos, %{}, fn {app, rebar_info}, acc ->
      rebar_vsn = rebar_info |> get_in([:app_src, :vsn]) |> to_string()
      mix_vsn = get_in(mix_infos, [app, :project, :version])

      if rebar_vsn != mix_vsn do
        mix_exs_path = get_in(mix_infos, [app, :mix_exs_path])
        Map.put(acc, app, %{mix_exs_path: mix_exs_path, rebar_vsn: rebar_vsn, mix_vsn: mix_vsn})
      else
        acc
      end
    end)
  end

  def check_missing_erl_opts(mix_infos, rebar_infos) do
    Enum.reduce(rebar_infos, %{}, fn {app, rebar_info}, acc ->
      custom_erl_opts = get_in(rebar_info, [:rebar_config, :erl_opts])

      with [_ | _] <- custom_erl_opts,
           mix_erl_opts = get_in(mix_infos, [app, :project, :erlc_options]),
           [_ | _] = missing_erl_opts <-
             do_check_missing_erl_opts(custom_erl_opts, mix_erl_opts, app, mix_infos) do
        Map.put(acc, app, missing_erl_opts)
      else
        _ -> acc
      end
    end)
  end

  defp do_check_missing_erl_opts(custom_erl_opts, mix_erl_opts, app, mix_infos) do
    missing_erl_opts = custom_erl_opts -- mix_erl_opts

    {src_dirs, missing_erl_opts} =
      Enum.split_with(missing_erl_opts, fn
        {:src_dirs, _} -> true
        _ -> false
      end)

    if src_dirs == [] do
      missing_erl_opts
    else
      src_dirs = for {:src_dirs, ds} <- src_dirs, d <- ds, do: to_string(d)
      erlc_paths = get_in(mix_infos, [app, :project, :erlc_paths])
      missing_erlc_paths = src_dirs -- erlc_paths

      if missing_erlc_paths == [] do
        missing_erl_opts
      else
        [{:src_dirs, missing_erlc_paths} | missing_erl_opts]
      end
    end
  end

  def check_missing_deps(mix_infos, rebar_infos) do
    umbrella_dep? = fn dep_spec ->
      app = elem(dep_spec, 0)
      emqx? = to_string(app) =~ ~r"^emqx.*"
      rebar_path? = match?({_, {:path, _}}, dep_spec)
      mix_umbrella? = match?({_, [{:in_umbrella, true} | _]}, dep_spec)

      emqx? && (rebar_path? || mix_umbrella?)
    end

    # simple check: only compares tags
    get_rebar3_dep_vsn = fn
      {_, vsn} when is_list(vsn) ->
        to_string(vsn)

      {_, tup} when is_tuple(tup) ->
        Tuple.to_list(tup)
        |> Enum.find(&match?({:tag, _vsn}, &1))
        |> then(fn {:tag, vsn} -> to_string(vsn) end)
    end

    get_mix_dep_vsn = fn
      {_, vsn} when is_binary(vsn) ->
        vsn

      {_, vsn, _} when is_binary(vsn) ->
        vsn

      {_, kw} when is_list(kw) ->
        Keyword.fetch!(kw, :tag)
    end

    Enum.reduce(rebar_infos, %{}, fn {app, rebar_info}, acc ->
      {rebar_umbrella, rebar_ext} =
        rebar_info
        |> get_in([:rebar_config, :deps])
        |> Enum.split_with(umbrella_dep?)

      rebar_umbrella = Enum.map(rebar_umbrella, &elem(&1, 0))

      rebar_ext =
        Map.new(rebar_ext, fn dep_spec ->
          app = elem(dep_spec, 0)
          vsn = get_rebar3_dep_vsn.(dep_spec)
          {app, vsn}
        end)

      {mix_umbrella, mix_ext} =
        mix_infos
        |> get_in([app, :deps])
        |> Enum.split_with(umbrella_dep?)

      mix_umbrella = Enum.map(mix_umbrella, &elem(&1, 0))
      missing_umbrella_deps = rebar_umbrella -- mix_umbrella

      mix_ext =
        Map.new(mix_ext, fn dep_spec ->
          app = elem(dep_spec, 0)
          vsn = get_mix_dep_vsn.(dep_spec)
          {app, vsn}
        end)

      ext_with_different_vsns =
        Enum.reduce(rebar_ext, %{}, fn {app, rebar3_vsn}, acc ->
          mix_vsn = Map.get(mix_ext, app, :missing)

          if mix_vsn == rebar3_vsn do
            acc
          else
            Map.put(acc, app, %{mix_vsn: mix_vsn, rebar3_vsn: rebar3_vsn})
          end
        end)

      problems =
        %{}
        |> put_if_any(:missing_umbrella_deps, missing_umbrella_deps)
        |> put_if_any(:version_mismatches, ext_with_different_vsns)

      if problems == %{} do
        acc
      else
        Map.put(acc, app, problems)
      end
    end)
  end

  def check_start_app_mismatches(mix_infos, rebar_infos) do
    mix_injected_apps = [:kernel, :stdlib]

    Enum.reduce(rebar_infos, %{}, fn {app, rebar_info}, acc ->
      rebar_apps = get_in(rebar_info, [:app_src, :applications]) || []
      rebar_included_apps = get_in(rebar_info, [:app_src, :included_applications]) || []
      rebar_included_apps = MapSet.new(rebar_included_apps)

      mix_deps =
        mix_infos
        |> get_in([app, :deps])
        |> Kernel.||([])
        |> Enum.map(&elem(&1, 0))

      mix_extra_apps = get_in(mix_infos, [app, :application, :extra_applications]) || []
      mix_included_apps = get_in(mix_infos, [app, :application, :included_applications]) || []
      mix_included_apps = MapSet.new(mix_included_apps)

      rebar_apps = MapSet.new(rebar_apps)
      mix_apps = MapSet.new(mix_injected_apps ++ mix_deps ++ mix_extra_apps)

      missing_apps = MapSet.difference(rebar_apps, mix_apps)
      missing_included_apps = MapSet.difference(rebar_included_apps, mix_included_apps)

      problems =
        %{}
        |> put_if_any(:missing_apps, missing_apps)
        |> put_if_any(:missing_included_apps, missing_included_apps)

      if Enum.empty?(problems) do
        acc
      else
        Map.put(acc, app, problems)
      end
    end)
  end

  defp put_if_any(map, k, enum) do
    if Enum.empty?(enum) do
      map
    else
      Map.put(map, k, enum)
    end
  end

  def copy_rebar_app_vsn_to_mix({_app, vsn_mismatch_info}) do
    %{
      mix_exs_path: mix_exs_path,
      rebar_vsn: rebar_vsn,
      mix_vsn: mix_vsn
    } = vsn_mismatch_info

    mix_exs_path
    |> File.read!()
    |> String.replace(~s|version: "#{mix_vsn}"|, ~s|version: "#{rebar_vsn}"|)
    |> then(&File.write!(mix_exs_path, &1))
  end

  def check() do
    Application.ensure_all_started(:mix)
    Code.require_file("mix.exs")

    apps = apps()
    mix_infos = mix_infos()
    rebar_infos = rebar_infos()

    apps_with_mix_exss =
      mix_infos
      |> Map.keys()
      |> MapSet.new()

    apps_without_mix_exs = MapSet.difference(apps, apps_with_mix_exss)

    %{
      apps_without_mix_exs: apps_without_mix_exs,
      version_mismatches: check_app_vsn_mismatches(mix_infos, rebar_infos),
      missing_erl_opts: check_missing_erl_opts(mix_infos, rebar_infos),
      deps_mismatches: check_missing_deps(mix_infos, rebar_infos),
      start_apps_mismatches: check_start_app_mismatches(mix_infos, rebar_infos)
    }
  end

  def puts(fmt) do
    IO.puts(IO.ANSI.format(fmt))
  end

  def report_missing_mix_exss(apps_without_mix_exs) do
    if Enum.empty?(apps_without_mix_exs) do
      _failed? = false
    else
      puts([
        :red,
        "These applications lack a `mix.exs` file:\n",
        Enum.map(apps_without_mix_exs, &"  * #{&1}\n"),
        "\n"
      ])

      _failed? = true
    end
  end

  def report_version_mismatches(version_mismatches) do
    if Enum.empty?(version_mismatches) do
      _failed? = false
    else
      puts([
        :red,
        "The applications below differ in their app versions (`mix.exs` and `*.app.src`):\n",
        Enum.map(version_mismatches, fn {app, vsn_info} ->
          %{rebar_vsn: rebar_vsn, mix_vsn: mix_vsn} = vsn_info

          [
            "  * #{app}\n",
            "    - rebar vsn: #{rebar_vsn}\n",
            "    - mix vsn:   #{mix_vsn}\n"
          ]
        end),
        "\n",
        "Either fix them manually, or run `#{this_file()} --fix-app-vsns`"
      ])

      _failed? = true
    end
  end

  def report_missing_erl_opts(missing_erl_opts) do
    if Enum.empty?(missing_erl_opts) do
      _failed? = false
    else
      puts([
        :red,
        "The applications below differ in their erl_opts (`mix.exs` and `rebar.config`):\n",
        Enum.map(missing_erl_opts, fn {app, missing} ->
          formatted_missing =
            missing
            |> Inspect.Algebra.to_doc(Inspect.Opts.new(pretty: true))
            |> Inspect.Algebra.nest(4, :always)
            |> Inspect.Algebra.format(0)

          [
            "  * #{app} is missing:\n",
            ["    ", formatted_missing, "\n"]
          ]
        end),
        "\n"
      ])

      _failed? = true
    end
  end

  def report_missing_umbrella_deps(missing_umbrella_deps) do
    if Enum.empty?(missing_umbrella_deps) do
      []
    else
      [
        "    - Missing umbrella dependencies:\n",
        Enum.map(missing_umbrella_deps, fn umbrella_dep ->
          "      + #{umbrella_dep}\n"
        end)
      ]
    end
  end

  def report_deps_version_mismatches(ext_version_mismatches) do
    if Enum.empty?(ext_version_mismatches) do
      []
    else
      [
        "    - External dependency version mismatches:\n",
        Enum.map(ext_version_mismatches, fn {ext_dep, info} ->
          %{mix_vsn: mix_vsn, rebar3_vsn: rebar3_vsn} = info
          "      + #{ext_dep} has mix version #{mix_vsn} and rebar3 version #{rebar3_vsn} \n"
        end)
      ]
    end
  end

  def report_deps_mismatches(deps_mismatches) do
    if Enum.empty?(deps_mismatches) do
      _failed? = false
    else
      puts([
        :red,
        "The applications below have dependency mismatches (`mix.exs` and `rebar.config`):\n",
        Enum.map(deps_mismatches, fn {app, problems} ->
          [
            "  * #{app}:\n",
            Map.get(problems, :missing_umbrella_deps, []) |> report_missing_umbrella_deps(),
            Map.get(problems, :version_mismatches, %{}) |> report_deps_version_mismatches()
          ]
        end),
        "\n"
      ])

      _failed? = true
    end
  end

  def report_missing_start_apps(missing_start_apps, label) do
    if Enum.empty?(missing_start_apps) do
      []
    else
      [
        "    - Missing #{label} apps:\n",
        Enum.map(missing_start_apps, fn app ->
          "      + #{app}\n"
        end)
      ]
    end
  end

  def report_start_apps_mismatches(start_apps_mismatches) do
    if Enum.empty?(start_apps_mismatches) do
      _failed? = false
    else
      puts([
        :red,
        "The applications below have start/included application mismatches (`mix.exs` and `app.src`):\n",
        Enum.map(start_apps_mismatches, fn {app, problems} ->
          [
            "  * #{app}:\n",
            Map.get(problems, :missing_apps, [])
            |> report_missing_start_apps("start"),
            Map.get(problems, :missing_included_apps, [])
            |> report_missing_start_apps("included")
          ]
        end),
        "\n"
      ])

      _failed? = true
    end
  end

  def report_problems(results) do
    failed? =
      Enum.reduce(results, false, fn
        {:apps_without_mix_exs, apps_without_mix_exs}, acc ->
          report_missing_mix_exss(apps_without_mix_exs) || acc

        {:version_mismatches, version_mismatches}, acc ->
          report_version_mismatches(version_mismatches) || acc

        {:missing_erl_opts, missing_erl_opts}, acc ->
          report_missing_erl_opts(missing_erl_opts) || acc

        {:deps_mismatches, deps_mismatches}, acc ->
          report_deps_mismatches(deps_mismatches) || acc

        {:start_apps_mismatches, start_apps_mismatches}, acc ->
          report_start_apps_mismatches(start_apps_mismatches) || acc
      end)

    if failed? do
      System.halt(1)
    else
      puts([:green, "Ok!\n"])
    end
  end

  def main(argv) do
    {flags, _} = OptionParser.parse!(argv, strict: [check: :boolean, fix_app_vsns: :boolean])

    results = check()

    cond do
      flags[:fix_app_vsns] ->
        results
        |> Map.fetch!(:version_mismatches)
        |> Enum.each(&CheckMixExsDiscrepancies.copy_rebar_app_vsn_to_mix/1)

      flags[:check] ->
        report_problems(results)

      :else ->
        puts([:yellow, "Usage: #{this_file()} --check, --fix-app-vsns"])
        System.halt(1)
    end
  end
end

System.argv()
|> CheckMixExsDiscrepancies.main()
