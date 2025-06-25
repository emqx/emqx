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
      IO.inspect(%{app: app, custom: custom_erl_opts, mix: get_in(mix_infos, [app])})
      with [_ | _] <- custom_erl_opts,
           mix_erl_opts = get_in(mix_infos, [app, :project, :erlc_options]),
           [_ | _] = missing_erl_opts <- custom_erl_opts -- mix_erl_opts do
        Map.put(acc, app, missing_erl_opts)
      else
        _ -> acc
      end
    end)
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
        "\n",
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
      end)

    if failed? do
      System.halt(1)
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
