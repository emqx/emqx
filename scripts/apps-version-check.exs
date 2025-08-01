#!/usr/bin/env elixir

defmodule AppsVersionCheck do
  @moduledoc """
  This script checks the versions from each of our umbrella applications and ensure they
  follow some conventions between EMQX releases.  It replaces the previous version that
  dealt with rebar3 configs:

  https://github.com/emqx/emqx/blob/abcebb7f76a6f5dfb099d05fea41cdf9489f0277/scripts/apps-version-check.sh

  # Usage

  Simply:

      $ scripts/apps-version-check.exs

  This script can also automatically fix version issues it finds.  For that, simply run:

      $ scripts/apps-version-check.exs --auto-fix

  # Conventions

  Let's say the latest release on the branch being examined is `RMajor.RMinor.RPatch`, or
  `LatestRelease` for short.

  1) We want that all app versions have the form `RMajor.RMinor.APatch`, where `APatch` is
     application-dependent.

  2) For a given application, if its version in `LatestRelease` was
     `RMajor.RMinor.APatch`, and it has production code changes since then, then its
     version at the next release must be `RMajor.RMinor.(APatch + 1)`.
  """

  def latest_release!() do
    {out, 0} =
      System.cmd(
        "bash",
        ["-c", "./scripts/find-prev-rel-tag.sh"],
        env: [{"PREV_TAG_MATCH_PATTERN", "*"}]
      )

    git_ref = String.trim(out)

    vsn =
      git_ref
      |> String.replace(~r/^(e|v)/, "")
      |> Version.parse!()

    %{
      git_ref: git_ref,
      latest_release: vsn
    }
  end

  def mix_exs_at(filepath, git_ref) do
    System.cmd(
      "git",
      ["show", "#{git_ref}:#{filepath}"]
    )
    |> case do
      {out, 0} ->
        {:ok, out}

      {_, _} ->
        :error
    end
  end

  def get_version(mix_exs_contents) do
    %{"vsn" => vsn} = Regex.named_captures(~r/version: "(?<vsn>[^"]+)"/, mix_exs_contents)
    Version.parse!(vsn)
  end

  def app_version_at(filepath, git_ref) do
    with {:ok, mix_exs_contents} <- mix_exs_at(filepath, git_ref) do
      get_version(mix_exs_contents)
    else
      _ -> :error
    end
  end

  def follows_convention?(vsn, context) do
    %{latest_release: latest_release} = context

    vsn.major == latest_release.major && vsn.minor == latest_release.minor
  end

  def has_valid_app_vsn?(app, context) do
    src_file = Path.join(["apps", app, "mix.exs"])

    if File.exists?(src_file) do
      do_has_valid_app_vsn?(app, context)
    else
      log("IGNORE: #{src_file} was deleted")
      true
    end
  end

  def has_changed_files?(app, context) do
    %{git_ref: git_ref} = context
    app_path = Path.join(["apps", app])

    {out, 0} =
      System.cmd(
        "git",
        [
          "diff",
          git_ref,
          "--ignore-blank-lines",
          "-G",
          "(^[^\s?%])",
          "--",
          "#{app_path}/src",
          "--",
          "#{app_path}/include",
          "--",
          ":(exclude)#{app_path}/mix.exs",
          "--",
          "#{app_path}/priv",
          "--",
          "#{app_path}/c_src"
        ]
      )

    out
    |> String.trim()
    |> String.split("\n", trim: true)
    |> Enum.count()
    |> Kernel.>(0)
  end

  def log_err(args) do
    IO.puts(IO.ANSI.format([:red, args]))
  end

  def log(args) do
    IO.puts(IO.ANSI.format(args))
  end

  def fix_vsn(src_file, current_vsn, desired_vsn) do
    src_file
    |> File.read!()
    |> String.replace(
      ~r/version: "#{to_string(current_vsn)}"/,
      ~s/version: "#{to_string(desired_vsn)}"/
    )
    |> then(&File.write!(src_file, &1))
  end

  def do_has_valid_app_vsn?(app, context) do
    %{
      latest_release: latest_release,
      git_ref: git_ref
    } = context

    src_file = Path.join(["apps", app, "mix.exs"])
    current_app_version = src_file |> File.read!() |> get_version()
    old_app_version = app_version_at(src_file, git_ref)
    current_follows_convention? = follows_convention?(current_app_version, context)

    old_follows_convention? =
      old_app_version != :error && follows_convention?(old_app_version, context)

    has_changes? = has_changed_files?(app, context)
    convention = "#{latest_release.major}.#{latest_release.minor}.PATCH_NUM"
    auto_fix? = Map.get(context, :auto_fix, false)

    cond do
      not current_follows_convention? ->
        log_err("#{src_file}: app version must be of form `#{convention}`")

        desired_version =
          latest_release
          |> Map.put(:patch, 0)
          |> Map.put(:pre, [])

        auto_fix? && fix_vsn(src_file, current_app_version, desired_version)
        false

      old_app_version == :error ->
        log("IGNORE: #{src_file} is newly added")
        true

      old_app_version == current_app_version && has_changes? ->
        log_err("ERROR: #{src_file} needs a vsn bump")

        desired_version =
          old_app_version
          |> Map.update!(:patch, &(&1 + 1))

        auto_fix? && fix_vsn(src_file, current_app_version, desired_version)
        false

      not old_follows_convention? ->
        log("IGNORE: #{src_file}: old app version did not follow the convention #{convention}")
        true

      current_app_version.patch != old_app_version.patch + 1 ->
        log_err([
          "#{src_file} non-strict semver version bump from",
          "#{old_app_version} to #{current_app_version}"
        ])

        desired_version =
          old_app_version
          |> Map.update!(:patch, &(&1 + 1))

        auto_fix? && fix_vsn(src_file, current_app_version, desired_version)
        false

      :otherwise ->
        true
    end
  end

  def main(argv) do
    {opts, _rest} = OptionParser.parse!(argv, strict: [auto_fix: :boolean])

    context =
      latest_release!()
      |> Map.put(:auto_fix, !!opts[:auto_fix])

    apps =
      "apps"
      |> File.ls!()
      |> Enum.filter(fn app ->
        ["apps", app]
        |> Path.join()
        |> File.dir?()
      end)

    apps
    |> Enum.reject(&has_valid_app_vsn?(&1, context))
    |> case do
      [] ->
        :ok

      invalid_apps ->
        log_err([
          "Errors were found\n",
          "Invalid apps: \n",
          [inspect(invalid_apps, pretty: true), "\n"],
          "Run this script again with `--auto-fix` to automatically fix issues,",
          " or fix them manually."
        ])

        System.halt(1)
    end
  end
end

System.argv()
|> AppsVersionCheck.main()
