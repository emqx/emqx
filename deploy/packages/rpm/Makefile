# Keep this short to avoid bloating beam files with long file path info
TOPDIR := /tmp/emqx
SRCDIR := $(TOPDIR)/$(PKG_VSN)
BUILT := $(SRCDIR)/BUILT
dash := -
none :=
space := $(none) $(none)
## RPM does not allow '-' in version nubmer and release string, replace with '_'
RPM_VSN := $(subst -,_,$(PKG_VSN))
RPM_REL := otp$(subst -,_,$(OTP_VSN))

ARCH ?= amd64
ifeq ($(ARCH),mips64)
ARCH:=mips64el
endif

EMQX_NAME=$(subst -pkg,,$(EMQX_BUILD))

ifeq ($(IS_ELIXIR), yes)
  ELIXIR_PKG_VSN := -elixir$(ELIXIR_VSN)
else
  ELIXIR_PKG_VSN :=
endif

TAR_PKG_DIR ?= _build/$(EMQX_BUILD)/rel/emqx
TAR_PKG     := $(EMQX_REL)/$(TAR_PKG_DIR)/emqx-$(PKG_VSN).tar.gz
SOURCE_PKG  := emqx-$(RPM_VSN)-$(RPM_REL).$(shell uname -m)
TARGET_PKG  := $(EMQX_NAME)-$(PKG_VSN)-otp$(OTP_VSN)$(ELIXIR_PKG_VSN)-$(SYSTEM)-$(ARCH)

SYSTEMD := $(shell if command -v systemctl >/dev/null 2>&1; then echo yes; fi)
# Not $(PWD) as it does not work for make -C
CURDIR := $(shell pwd)

SERVICE_SRC := $(if $(SYSTEMD),$(CURDIR)/emqx.service,$(CURDIR)/init.script)
SERVICE_DST := $(if $(SYSTEMD),%{_unitdir}/emqx.service,%{_initddir}/emqx)

POST_ADDITION := $(if $(SYSTEMD),%systemd_post %{_name}.service,echo post)
PREUN_ADDITION := $(if $(SYSTEMD),%systemd_preun %{_name}.service,echo preun)

.PHONY: all
all: | $(BUILT)
	cd $(SRCDIR)
	rpmbuild -v -bb \
		--define "_package_name $(EMQX_NAME)" \
		--define "_name emqx" \
		--define "_topdir $(TOPDIR)" \
		--define "_version $(RPM_VSN)" \
		--define "_reldir $(SRCDIR)" \
		--define "_release $(RPM_REL)" \
		--define "_service_src $(SERVICE_SRC)" \
		--define "_service_dst $(SERVICE_DST)" \
		--define "_post_addition $(POST_ADDITION)" \
		--define "_preun_addition $(PREUN_ADDITION)" \
		--define "_sharedstatedir /var/lib" \
		emqx.spec
	mkdir -p $(EMQX_REL)/_packages/$(EMQX_NAME)
	cp $(TOPDIR)/RPMS/$(shell uname -m)/$(SOURCE_PKG).rpm $(EMQX_REL)/_packages/$(EMQX_NAME)/$(TARGET_PKG).rpm

$(BUILT):
	mkdir -p $(TOPDIR) $(SRCDIR) $(SRCDIR)/BUILT
	tar zxf $(TAR_PKG) -C $(SRCDIR)

clean:
	rm -rf $(SRCDIR)
