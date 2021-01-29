# Keep this short to avoid bloating beam files with long file path info
TOPDIR := /tmp/emqx
SRCDIR := $(TOPDIR)/$(PKG_VSN)
BUILT := $(SRCDIR)/BUILT

EMQX_NAME=$(subst -pkg,,$(EMQX_BUILD))

TAR_PKG := $(EMQX_REL)/_build/$(EMQX_BUILD)/rel/emqx/emqx-$(PKG_VSN).tar.gz
SOURCE_PKG := $(EMQX_NAME)_$(PKG_VSN)_$(shell dpkg --print-architecture)
TARGET_PKG := $(EMQX_NAME)-$(SYSTEM)-$(PKG_VSN)-$(shell uname -m)

.PHONY: all
all: | $(BUILT)
	cp -r debian $(SRCDIR)/
	sed -i "s#<DATETIME>#$(shell date -u '+%a, %d %b %Y %T %z')#g" $(SRCDIR)/debian/changelog
	sed -i "s#<VERSION>#$(PKG_VSN)#g" $(SRCDIR)/debian/changelog
	if [ ! -z $(shell echo $(EMQX_NAME) |grep edge) ]; then \
		sed -i "s/emqx-pkg/emqx-edge-pkg/g" $(SRCDIR)/debian/rules; \
		sed -i "s debian/emqx debian/emqx-edge g" $(SRCDIR)/debian/rules; \
		sed -i "s/Package: emqx/Package: emqx-edge/1" $(SRCDIR)/debian/control; \
	fi
	cd $(SRCDIR) && dpkg-buildpackage -us -uc
	mkdir -p $(EMQX_REL)/_packages/$(EMQX_NAME)
	cp $(SRCDIR)/../$(SOURCE_PKG).deb $(EMQX_REL)/_packages/$(EMQX_NAME)/$(TARGET_PKG).deb

$(BUILT):
	mkdir -p $(TOPDIR) $(SRCDIR)
	tar zxf $(TAR_PKG) -C $(SRCDIR)

clean:
	rm -rf $(SRCDIR)
