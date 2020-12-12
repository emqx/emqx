#!/usr/bin/make -f
# -*- makefile -*-

PROFILES := emqx emqx-edge
PKG_PROFILES := emqx-pkg emqx-edge-pkg

ifeq ($(shell uname -s),Darwin)
	SYSTEM := macos
else ifeq ($(shell uname -s),Linux)
    ifneq ($(shell cat /etc/*-release |grep -o -i centos),)
    	ID := centos
    	VERSION_ID := $(shell rpm --eval '%{centos_ver}')
    else
    	ID := $(shell sed -n '/^ID=/p' /etc/os-release | sed -r 's/ID=(.*)/\1/g' | sed 's/"//g' )
    	VERSION_ID := $(shell sed -n '/^VERSION_ID=/p' /etc/os-release | sed -r 's/VERSION_ID=(.*)/\1/g' | sed 's/"//g')
    endif
    SYSTEM := $(shell echo $(ID)$(VERSION_ID) | sed -r "s/([a-zA-Z]*)-.*/\1/g")
    ##
    ## Support RPM and Debian based linux systems
    ##
    ifeq ($(ID),ubuntu)
    	PKGERDIR := deb
    else ifeq ($(ID),debian)
    	PKGERDIR := deb
    else ifeq ($(ID),raspbian)
    	PKGERDIR := deb
    else
    	PKGERDIR := rpm
    endif
endif

.PHONY: $(PROFILES:%=relup-%)
$(PROFILES:%=relup-%): $(REBAR)
ifneq ($(RELUP_BASE_VERSIONS),)
ifneq ($(OS),Windows_NT)
	@if [ ! -z $$(ls | grep -E "$(@:relup-%=%)-$(SYSTEM)-(.*)-$$(uname -m).zip" | head -1 ) ]; then \
		mkdir -p tmp/relup_packages/$(@:relup-%=%); \
		cp $(@:relup-%=%)-$(SYSTEM)-*-$$(uname -m).zip tmp/relup_packages/$(@:relup-%=%); \
	fi
	$(REBAR) as $(@:relup-%=%) relup --relname emqx --relvsn $(PKG_VSN) --upfrom $(RELUP_BASE_VERSIONS)
endif
endif

.PHONY: $(PROFILES:%=%-tar) $(PKG_PROFILES:%=%-tar)
$(PROFILES:%=%-tar) $(PKG_PROFILES:%=%-tar): $(REBAR)
ifneq ($(shell echo $(@) |grep edge),)
	export EMQX_DESC="EMQ X Edge"
else
	export EMQX_DESC="EMQ X Broker"
endif
	$(REBAR) as $(subst -tar,,$(@)) tar

.PHONY: $(PROFILES:%=%-zip)
$(PROFILES:%=%-zip): $(REBAR)
ifneq ($(shell echo $(PKG_VSN) | grep -oE "^[0-9]+\.[0-9]+\.[1-9]+?"),)
	make relup-$(subst -zip,,$(@))
endif
	make $(subst -zip,,$(@))-tar

	@tard="/tmp/emqx_untar_$(PKG_VSN)" \
	&& rm -rf "$${tard}" && mkdir -p "$${tard}/emqx" \
	&& prof="$(subst -zip,,$(@))" \
	&& relpath="$$(pwd)/_build/$${prof}/rel/emqx" \
	&& pkgpath="$$(pwd)/_packages/$${prof}" \
	&& mkdir -p $${pkgpath} \
	&& tarball="$${relpath}/emqx-$(PKG_VSN).tar.gz" \
	&& zipball="$${pkgpath}/$${prof}-$(SYSTEM)-$(PKG_VSN)-$$(uname -m).zip" \
	&& tar zxf "$${tarball}" -C "$${tard}/emqx" \
	&& cd "$${tard}" && zip -q -r "$${zipball}" ./emqx && cd -

.PHONY: $(PKG_PROFILES)
$(PKG_PROFILES:%=%): $(REBAR)
ifneq ($(PKGERDIR),)
	make $(subst -pkg,,$(@))-zip
	make $(@)-tar
	make -C deploy/packages/$(PKGERDIR) clean
	EMQX_REL=$$(pwd) EMQX_BUILD=$(@) PKG_VSN=$(PKG_VSN) SYSTEM=$(SYSTEM) make -C deploy/packages/$(PKGERDIR)
else
	make $(subst -pkg,,$(@))-zip
endif

