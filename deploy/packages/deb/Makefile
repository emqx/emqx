ARCH ?= amd64
TOPDIR := /tmp/emqx
# Keep this short to avoid bloating beam files with long file path info
SRCDIR := $(TOPDIR)/$(PKG_VSN)
BUILT := $(SRCDIR)/BUILT

EMQX_NAME=$(subst -pkg,,$(EMQX_BUILD))

ifeq ($(IS_ELIXIR), yes)
  ELIXIR_PKG_VSN := -elixir$(ELIXIR_VSN)
else
  ELIXIR_PKG_VSN :=
endif

TAR_PKG_DIR ?= _build/$(EMQX_BUILD)/rel/emqx
TAR_PKG     := $(EMQX_REL)/$(TAR_PKG_DIR)/emqx-$(PKG_VSN).tar.gz
SOURCE_PKG  := $(EMQX_NAME)_$(PKG_VSN)_$(shell dpkg --print-architecture)
TARGET_PKG  := $(EMQX_NAME)-$(PKG_VSN)-otp$(OTP_VSN)$(ELIXIR_PKG_VSN)-$(SYSTEM)-$(ARCH)

.PHONY: all
all: | $(BUILT)
	cp -r debian $(SRCDIR)/
	cp emqx.service $(SRCDIR)/debian/${EMQX_NAME}.service
	sed -i "s#<DATETIME>#$(shell date -u '+%a, %d %b %Y %T %z')#g" $(SRCDIR)/debian/changelog
	sed -i "s#<VERSION>#$(PKG_VSN)#g" $(SRCDIR)/debian/changelog
	sed -i "s/emqx-pkg/$(EMQX_NAME)-pkg/g" $(SRCDIR)/debian/rules; \
	sed -i "s debian/emqx debian/$(EMQX_NAME) g" $(SRCDIR)/debian/rules; \
	sed -i "s/Package: emqx/Package: $(EMQX_NAME)/1" $(SRCDIR)/debian/control; \
	cd $(SRCDIR) && dpkg-buildpackage -us -uc
	mkdir -p $(EMQX_REL)/_packages/$(EMQX_NAME)
	cp $(SRCDIR)/../$(SOURCE_PKG).deb $(EMQX_REL)/_packages/$(EMQX_NAME)/$(TARGET_PKG).deb

$(BUILT):
	mkdir -p $(TOPDIR) $(SRCDIR)
	tar zxf $(TAR_PKG) -C $(SRCDIR)

clean:
	rm -rf $(SRCDIR)
