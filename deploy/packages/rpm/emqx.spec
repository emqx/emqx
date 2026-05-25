%define debug_package %{nil}
%define _user %{_name}
%define _group %{_name}
%define _conf_dir %{_sysconfdir}/%{_name}
%define _log_dir %{_var}/log/%{_name}
%define _lib_home /usr/lib/%{_name}
%define _var_home %{_sharedstatedir}/%{_name}
%define _build_name_fmt %{_arch}/%{_name}-%{_version}.%{_arch}.rpm
%define _build_id_links none

Name: %{_package_name}
Version: %{_version}
Release: 1%{?dist}
Summary: emqx
Group: System Environment/Daemons
License: Business Source License 1.1
URL: https://www.emqx.com
BuildRoot: %{_tmppath}/%{_name}-%{_version}-root
Provides: %{_name}
AutoReq: 0

Requires: libatomic procps which findutils ncurses

%if 0%{?rhel} == 7 && "%{_arch}" == "x86_64"
Requires: openssl11
%else
%if 0%{?rhel} == 9 && "%{?dist}" >= ".el9_7"
Requires: openssl >= 1:3.5.1
%else
%if 0%{?rhel} == 9
Requires: openssl >= 1:3.0.7
%else
Requires: openssl
%endif
%endif
%endif

%if "%{?dist}" == ".amzn2023"
Requires: util-linux shadow-utils
%endif

%description
EMQX, a distributed, massively scalable, highly extensible MQTT message broker.

%prep

%build

%install
mkdir -p %{buildroot}%{_lib_home}
mkdir -p %{buildroot}%{_lib_home}/plugins
mkdir -p %{buildroot}%{_log_dir}
mkdir -p %{buildroot}%{_unitdir}
mkdir -p %{buildroot}%{_conf_dir}
mkdir -p %{buildroot}%{_bindir}
mkdir -p %{buildroot}%{_var_home}

cp -R %{_reldir}/lib %{buildroot}%{_lib_home}/
touch %{buildroot}%{_lib_home}/plugins/.keep
cp -R %{_reldir}/erts-* %{buildroot}%{_lib_home}/
cp -R %{_reldir}/releases %{buildroot}%{_lib_home}/
cp -R %{_reldir}/bin %{buildroot}%{_lib_home}/
cp -R %{_reldir}/etc/* %{buildroot}%{_conf_dir}/
cp -R %{_reldir}/data/* %{buildroot}%{_var_home}/
install -m644 %{_service_src} %{buildroot}%{_service_dst}

%pre
if [ $1 = 1 ]; then
  # Initial installation
  /usr/bin/getent group %{_group} >/dev/null || /usr/sbin/groupadd -r %{_group}
  if ! /usr/bin/getent passwd %{_user} >/dev/null ; then
      /usr/sbin/useradd -r -g %{_group} -m -d %{_sharedstatedir}/%{_name} -c "%{_name}" %{_user}
  fi
fi

%post
if [ $1 = 1 ]; then
    ln -s %{_lib_home}/bin/emqx %{_bindir}/emqx
    ln -s %{_lib_home}/bin/emqx_ctl %{_bindir}/emqx_ctl
fi
%{_post_addition}
systemctl enable %{_name}.service
chown -R %{_user}:%{_group} %{_lib_home}

# Expose the scattered FHS layout under /opt/%{_name} so paths match the docker image.
mkdir -p /opt/%{_name}
ln -sfn %{_lib_home}/bin      /opt/%{_name}/bin
ln -sfn %{_var_home}          /opt/%{_name}/data
ln -sfn %{_conf_dir}          /opt/%{_name}/etc
ln -sfn %{_lib_home}/lib      /opt/%{_name}/lib
ln -sfn %{_log_dir}           /opt/%{_name}/log
ln -sfn %{_lib_home}/plugins  /opt/%{_name}/plugins
ln -sfn %{_lib_home}/releases /opt/%{_name}/releases
for erts in %{_lib_home}/erts-*; do
    [ -d "$erts" ] || continue
    ln -sfn "$erts" "/opt/%{_name}/$(basename "$erts")"
done

%preun
%{_preun_addition}
# Only on uninstall, not upgrades
if [ $1 = 0 ]; then
    systemctl disable %{_name}.service
    rm -f %{_bindir}/emqx
    rm -f %{_bindir}/emqx_ctl
fi
exit 0

%postun
if [ $1 = 0 ]; then
   rm -rf %{_lib_home}
   if [ -d /opt/%{_name} ]; then
       for entry in /opt/%{_name}/bin /opt/%{_name}/data /opt/%{_name}/etc \
                    /opt/%{_name}/lib /opt/%{_name}/log /opt/%{_name}/plugins \
                    /opt/%{_name}/releases /opt/%{_name}/erts-*; do
           if [ -L "$entry" ]; then
               rm -f "$entry"
           fi
       done
       rmdir /opt/%{_name} 2>/dev/null || true
   fi
fi
exit 0

%files
%defattr(-,root,root)
%{_service_dst}
%attr(-,%{_user},%{_group}) %{_lib_home}/*
%attr(750,%{_user},%{_group}) %dir %{_var_home}
%attr(-,%{_user},%{_group}) %config(noreplace) %{_var_home}/*
%attr(-,%{_user},%{_group}) %dir %{_log_dir}
%attr(-,%{_user},%{_group}) %config(noreplace) %{_conf_dir}/*

%clean
rm -rf %{buildroot}

%changelog
