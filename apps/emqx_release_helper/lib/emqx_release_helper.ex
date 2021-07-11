defmodule EmqxReleaseHelper do
  def applications do
    config = profile_vars()

    EmqxReleaseHelper.Applications.__all__()
    |> Enum.filter(fn
      %{enable?: fun} -> fun.(config)
      _ -> true
    end)
    |> Enum.map(fn %{name: name, start_type: start_type} -> {name, start_type} end)
  end

  def run(release) do
    config = Map.merge(profile_vars(), release_vars(release))

    release
    |> EmqxReleaseHelper.Overlay.run(config)
    |> EmqxReleaseHelper.Applications.run(config)
  end

  def profile_vars() do
    "RELEASE_PROFILE"
    |> System.get_env("emqx")
    |> String.to_existing_atom()
    |> case do
      :emqx ->
        %{
          release_type: :cloud,
          package_type: :bin
        }
    end
    |> Map.merge(%{
      project_path: EMQXUmbrella.MixProject.project_path(),
      enable_bcrypt: EMQXUmbrella.MixProject.enable_bcrypt(),
      enable_plugin_emqx_modules: false,
      enable_plugin_emqx_retainer: true,
      apps_paths: Mix.Project.apps_paths(),
      built_on_arch: get_arch()
    })
    |> then(fn %{release_type: release_type} = config ->
      Map.merge(config, profile_vars(:release_type, release_type))
    end)
    |> then(fn %{package_type: package_type} = config ->
      Map.merge(config, profile_vars(:package_type, package_type))
    end)
  end

  defp profile_vars(:release_type, :cloud) do
    %{
      emqx_description: "EMQ X Broker",
      enable_plugin_emqx_rule_engine: true,
      enable_plugin_emqx_bridge_mqtt: false
    }
  end

  defp profile_vars(:release_type, :edge) do
    %{
      emqx_description: "EMQ X Edge",
      enable_plugin_emqx_rule_engine: false,
      enable_plugin_emqx_bridge_mqtt: true
    }
  end

  defp profile_vars(:package_type, :bin) do
    %{
      platform_bin_dir: "bin",
      platform_data_dir: "data",
      platform_etc_dir: "etc",
      platform_lib_dir: "lib",
      platform_log_dir: "log",
      platform_plugins_dir: "etc/plugins",
      runner_root_dir: EMQXUmbrella.MixProject.project_path(),
      runner_bin_dir: "$RUNNER_ROOT_DIR/bin",
      runner_etc_dir: "$RUNNER_ROOT_DIR/etc",
      runner_lib_dir: "$RUNNER_ROOT_DIR/lib",
      runner_log_dir: "$RUNNER_ROOT_DIR/log",
      runner_data_dir: "$RUNNER_ROOT_DIR/data",
      runner_user: ""
    }
  end

  defp profile_vars(:package_type, :pkg) do
    %{
      platform_bin_dir: "",
      platform_data_dir: "/var/lib/emqx",
      platform_etc_dir: "/etc/emqx",
      platform_lib_dir: "",
      platform_log_dir: "/var/log/emqx",
      platform_plugins_dir: "/var/lib/emqx/plugins",
      runner_root_dir: "/usr/lib/emqx",
      runner_bin_dir: "/usr/bin",
      runner_etc_dir: "/etc/emqx",
      runner_lib_dir: "$RUNNER_ROOT_DIR/lib",
      runner_log_dir: "/var/log/emqx",
      runner_data_dir: "/var/lib/emqx",
      runner_user: "emqx"
    }
  end

  defp release_vars(release) do
    %{
      release_version: release.version,
      release_path: release.path,
      release_version_path: release.version_path
    }
  end

  defp get_arch do
    major_version = System.otp_release()

    otp_release =
      [:code.root_dir(), "releases", major_version, "OTP_VERSION"]
      |> Path.join()
      |> File.read()
      |> case do
        {:ok, version} -> String.trim(version)
        {:error, _} -> major_version
      end

    wordsize =
      try do
        :erlang.system_info({:wordsize, :external}) * 8
      rescue
        _ ->
          :erlang.system_info(:wordsize) * 8
      end

    Enum.join([otp_release, :erlang.system_info(:system_architecture), wordsize], "-")
  end
end
