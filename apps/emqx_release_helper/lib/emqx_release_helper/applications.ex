defmodule EmqxReleaseHelper.Applications do
  use EmqxReleaseHelper.DSL.Application

  application :emqx do
    start_type :load

    overlay %{release_type: release_type} do
      copy "etc/certs", "etc/certs"

      template "etc/ssl_dist.conf", "etc/ssl_dist.conf"
      template "etc/emqx_#{release_type}/vm.args", "etc/vm.args"
    end
  end

  application :emqx_conf do
    start_type :load

    overlay do
      template "etc/emqx.conf.all", "etc/emqx.conf"
    end
  end

  application :lc do
    start_type :load
  end

  application :esasl do
    start_type :load
  end

  application :mria do
    start_type :load
  end

  application :mnesia do
    start_type :load
  end

  application :ekka do
    start_type :load
  end

  application :emqx_plugin_libs do
    start_type :load
  end

  application :emqx_gateway do
    start_type :load

    overlay do
      copy "src/lwm2m/lwm2m_xml", "etc/lwm2m_xml"
    end
  end

  application :emqx_resource do
    start_type :load
  end

  application :emqx_connector do
    start_type :load
  end

  application :emqx_bridge do
    start_type :load
  end

  application :emqx_authn do
    start_type :load
  end

  application :emqx_authz do
    start_type :load

    overlay do
      template "etc/acl.conf", "etc/acl.conf"
    end
  end

  application :emqx_machine do
    start_type :permanent
  end

  application :emqx_auto_subscribe do
    start_type :permanent
  end

  application :emqx_exhook do
    start_type :permanent
  end

  application :emqx_modules do
    start_type :permanent
  end

  application :emqx_dashboard do
    start_type :permanent
  end

  application :emqx_management do
    start_type :permanent
  end

  application :emqx_statsd do
    start_type :permanent
  end

  application :emqx_retainer do
    start_type :permanent
  end

  application :emqx_rule_engine do
    start_type :permanent
  end

  application :emqx_psk do
    start_type :permanent
  end

  application :emqx_limiter do
    start_type :permanent
  end

  application :emqx_prometheus do
    start_type :permanent
  end

  application :bcrypt, %{enable_bcrypt: true, release_type: :cloud} do
    start_type :permanent
  end

  application :xmerl, %{release_type: :cloud} do
    start_type :permanent
  end

  application :observer, %{release_type: :cloud} do
    start_type :load
  end
end
