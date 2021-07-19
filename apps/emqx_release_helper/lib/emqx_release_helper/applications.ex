defmodule EmqxReleaseHelper.Applications do
  use EmqxReleaseHelper.DSL.Application

  application :emqx do
    start_type :permanent

    overlay %{release_type: release_type} do
      copy "etc/certs", "etc/certs"

      template "etc/acl.conf", "etc/acl.conf"
      template "etc/emqx.conf", "etc/emqx.conf"
      template "etc/ssl_dist.conf", "etc/ssl_dist.conf"
      template "etc/emqx_#{release_type}/vm.args", "etc/vm.args"
    end
  end

  application :emqx_connector do
    start_type :permanent
  end

  application :emqx_modules do
    start_type :permanent
  end

  application :emqx_resource do
    start_type :permanent
  end

  application :emqx_plugin_libs do
    start_type :load
  end

  application :emqx_authz do
    start_type :permanent
    overlay :plugin
  end

  application :emqx_data_bridge do
    start_type :permanent
    overlay :plugin
  end

  application :emqx_sn do
    start_type :load
    overlay :plugin
  end

  application :emqx_authentication do
    start_type :load
    overlay :plugin
  end

  application :emqx_dashboard do
    start_type :load
    overlay :plugin
  end

  application :emqx_management do
    start_type :load
    overlay :plugin
  end

  application :emqx_statsd do
    start_type :load
    overlay :plugin
  end

  application :emqx_stomp do
    start_type :load
    overlay :plugin
  end

  application :emqx_bridge_mqtt do
    start_type :load
    overlay :plugin
  end

  application :emqx_retainer do
    start_type :load
    overlay :plugin
  end

  application :emqx_telemetry do
    start_type :permanent
    overlay :plugin
  end

  application :emqx_coap do
    start_type :load
    overlay :plugin
  end

  application :emqx_rule_engine do
    start_type :load
    overlay :plugin
  end

  application :emqx_web_hook do
    start_type :load
    overlay :plugin
  end

  application :emqx_config_helper do
    start_type :load
  end

  application :emqx_exhook, %{release_type: :cloud} do
    start_type :load
    overlay :plugin
  end

  application :emqx_exproto, %{release_type: :cloud} do
    start_type :load
    overlay :plugin
  end

  application :emqx_prometheus, %{release_type: :cloud} do
    start_type :load
    overlay :plugin
  end

  application :emqx_lwm2m, %{release_type: :cloud} do
    start_type :load
    overlay :plugin

    overlay do
      copy "lwm2m_xml", "etc/lwm2m_xml"
    end
  end

  application :emqx_psk_file, %{release_type: :cloud} do
    start_type :load
    overlay :plugin

    overlay do
      copy "etc/psk.txt", "etc/psk.txt"
    end
  end

  application :bcrypt, %{enable_bcrypt: true, release_type: :cloud} do
    start_type :permanent
  end

  application :xmerl, %{release_type: :cloud} do
    start_type :permanent
  end

  def run(release, config) do
    %{project_path: project_path, apps_paths: apps_paths} = config

    __all__()
    |> Enum.filter(fn %{name: name} -> Map.has_key?(apps_paths, name) end)
    |> Enum.filter(fn
      %{enable?: fun} -> fun.(config)
      _ -> true
    end)
    |> Enum.each(fn %{name: name, overlays: overlays} ->
      app_path = Map.get(apps_paths, name)
      config = Map.put(config, :app_source_path, Path.join(project_path, app_path))
      Enum.each(overlays, fn overlay -> overlay.(config) end)
    end)

    release
  end
end
