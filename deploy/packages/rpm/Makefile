# Keep this short to avoid bloating beam files with long file path info
TOPDIR := /tmp/emqx
SRCDIR := $(TOPDIR)/$(PKG_VSN)
BUILT := $(SRCDIR)/BUILT
dash := -
none :=
space := $(none) $(none)
## RPM does not allow '-' in version number and release string, replace with '_'
RPM_VSN := $(subst -,_,$(PKG_VSN))
RPM_REL := otp$(subst -,_,$(OTP_VSN))

EMQX_NAME=$(subst -pkg,,$(EMQX_BUILD))

TAR_PKG_DIR ?= _build/$(EMQX_BUILD)/rel/emqx
TAR_PKG     := $(EMQX_REL)/$(TAR_PKG_DIR)/emqx-$(PKG_VSN).tar.gz
SOURCE_PKG  := emqx-$(RPM_VSN)-$(RPM_REL).$(shell uname -m)
TARGET_PKG  := $(EMQX_NAME)-$(shell $(EMQX_REL)/pkg-vsn.sh $(EMQX_NAME) --long)

# Not $(PWD) as it does not work for make -C
CURDIR := $(shell pwd)

POST_ADDITION := %systemd_post %{_name}.service
PREUN_ADDITION := %systemd_preun %{_name}.service
SERVICE_SRC := $(CURDIR)/emqx.service
SERVICE_DST := %{_unitdir}/emqx.service

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
		--define "_post_addition $(POST_ADDITION)" \
		--define "_preun_addition $(PREUN_ADDITION)" \
		--define "_sharedstatedir /var/lib" \
		--define "_service_src $(SERVICE_SRC)" \
		--define "_service_dst $(SERVICE_DST)" \
		emqx.spec
	mkdir -p $(EMQX_REL)/_packages/$(EMQX_NAME)
	cp $(TOPDIR)/RPMS/$(shell uname -m)/$(SOURCE_PKG).rpm $(EMQX_REL)/_packages/$(EMQX_NAME)/$(TARGET_PKG).rpm
	sha256sum $(EMQX_REL)/_packages/$(EMQX_NAME)/$(TARGET_PKG).rpm | head -c 64 > \
		$(EMQX_REL)/_packages/$(EMQX_NAME)/$(TARGET_PKG).rpm.sha256

$(BUILT):
	mkdir -p $(TOPDIR) $(SRCDIR) $(SRCDIR)/BUILT
	tar zxf $(TAR_PKG) -C $(SRCDIR)

clean:
	rm -rf $(SRCDIR)
