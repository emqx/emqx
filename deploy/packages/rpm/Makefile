# Keep this short to avoid bloating beam files with long file path info
TOPDIR := /tmp/emqx
SRCDIR := $(TOPDIR)/$(PKG_VSN)
BUILT := $(SRCDIR)/BUILT
dash := -
none :=
space := $(none) $(none)
RPM_VSN ?= $(shell echo $(PKG_VSN) | grep -oE "[0-9]+\.[0-9]+(\.[0-9]+)?")
RPM_REL ?= $(shell echo $(PKG_VSN) | grep -oE "(alpha|beta|rc)\.[0-9]")

ARCH:=$(shell uname -m)
ifeq ($(ARCH),mips64)
ARCH:=mips64el
endif

EMQX_NAME=$(subst -pkg,,$(EMQX_BUILD))

TAR_PKG := $(EMQX_REL)/_build/$(EMQX_BUILD)/rel/emqx/emqx-$(PKG_VSN).tar.gz
TARGET_PKG := $(EMQX_NAME)-$(SYSTEM)-$(PKG_VSN)-$(ARCH)
ifeq ($(RPM_REL),)
	# no tail
	RPM_REL := 1
endif
SOURCE_PKG := emqx-$(SYSTEM)-$(RPM_VSN)-$(RPM_REL).$(ARCH)

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
		--define "_ostype -$(SYSTEM)" \
		--define "_sharedstatedir /var/lib" \
		emqx.spec
	mkdir -p $(EMQX_REL)/_packages/$(EMQX_NAME)
	cp $(TOPDIR)/RPMS/$(ARCH)/$(SOURCE_PKG).rpm $(EMQX_REL)/_packages/$(EMQX_NAME)/$(TARGET_PKG).rpm

$(BUILT):
	mkdir -p $(TOPDIR) $(SRCDIR) $(SRCDIR)/BUILT
	tar zxf $(TAR_PKG) -C $(SRCDIR)

clean:
	rm -rf $(SRCDIR)

