MODULE=user
SRCDIR=oar

MANDIR_FILES = $(SRCDIR)/../docs/man/man1/oardel.1 \
	       $(SRCDIR)/../docs/man/man1/oarnodes.1 \
	       $(SRCDIR)/../docs/man/man1/oarresume.1 \
	       $(SRCDIR)/../docs/man/man1/oarstat.1 \
	       $(SRCDIR)/../docs/man/man1/oarsub.1 \
		   $(SRCDIR)/../docs/man/man1/oarwalltime.1 \
	       $(SRCDIR)/../docs/man/man1/oarhold.1 \
	       $(SRCDIR)/../docs/man/man1/oarmonitor_graph_gen.1

include Makefiles/shared/shared.mk

clean: clean_shared
	$(MAKE) -f Makefiles/man.mk clean
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/.oarnodes CMD_TARGET=$(DESTDIR)$(BINDIR)/oarnodes
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/.oardel CMD_TARGET=$(DESTDIR)$(BINDIR)/oardel
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/.oarstat CMD_TARGET=$(DESTDIR)$(BINDIR)/oarstat
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/.oarcluster CMD_TARGET=$(DESTDIR)$(BINDIR)/oarcluster
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/.oarsub CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsub
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/.oarconnect CMD_TARGET=$(DESTDIR)$(BINDIR)/oarconnect
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/.oarwalltime CMD_TARGET=$(DESTDIR)$(BINDIR)/oarwalltime
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/.oarhold CMD_TARGET=$(DESTDIR)$(BINDIR)/oarhold
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/.oarresume CMD_TARGET=$(DESTDIR)$(BINDIR)/oarresume

build: build_shared
	$(MAKE) -f Makefiles/man.mk build
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/.oarnodes CMD_TARGET=$(DESTDIR)$(BINDIR)/oarnodes
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/.oardel CMD_TARGET=$(DESTDIR)$(BINDIR)/oardel
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/.oarstat CMD_TARGET=$(DESTDIR)$(BINDIR)/oarstat
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/.oarcluster CMD_TARGET=$(DESTDIR)$(BINDIR)/oarcluster
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/.oarsub CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsub
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/.oarconnect CMD_TARGET=$(DESTDIR)$(BINDIR)/oarconnect
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/.oarwalltime CMD_TARGET=$(DESTDIR)$(BINDIR)/oarwalltime
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/.oarhold CMD_TARGET=$(DESTDIR)$(BINDIR)/oarhold
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/.oarresume CMD_TARGET=$(DESTDIR)$(BINDIR)/oarresume

install: install_shared
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 $(SRCDIR)/tools/oarmonitor_graph_gen.pl $(DESTDIR)$(BINDIR)/oarmonitor_graph_gen
	install -m 0755 $(SRCDIR)/tools/oarprint $(DESTDIR)$(BINDIR)/oarprint
	for file in oar-almighty oar-appendice-proxy oar-bipbip-commander \
		oar-sarko oar-finaud oar-leon oar-bipbip oar-node-change-state \
		.oarproperty oar-hulot kao kamelot kamelot-fifo \
		.oarremoveresource .oaraccounting .oarconnect \
		.oarnodes .oardel .oarstat .oarsub .oarhold .oarresume .oarwalltime .oarcluster;\
	do \
		if [ -f  $(DESTDIR)$(BINDIR)/$$file ]; then \
			mv $(DESTDIR)$(BINDIR)/$$file $(OARDIR)/$$file; \
		fi \
	done
	# Install wrappers
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/.oarnodes CMD_TARGET=$(DESTDIR)$(BINDIR)/oarnodes
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/.oardel CMD_TARGET=$(DESTDIR)$(BINDIR)/oardel
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/.oarstat CMD_TARGET=$(DESTDIR)$(BINDIR)/oarstat
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/.oarcluster CMD_TARGET=$(DESTDIR)$(BINDIR)/oarcluster
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/.oarsub CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsub
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/.oarconnect CMD_TARGET=$(DESTDIR)$(BINDIR)/oarconnect
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/.oarwalltime CMD_TARGET=$(DESTDIR)$(BINDIR)/oarwalltime
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/.oarhold CMD_TARGET=$(DESTDIR)$(BINDIR)/oarhold
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/.oarresume CMD_TARGET=$(DESTDIR)$(BINDIR)/oarresume

uninstall: uninstall_shared
	rm -f $(DESTDIR)$(BINDIR)/oarmonitor_graph_gen
	rm -f $(DESTDIR)$(BINDIR)/oarprint

	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/.oarnodes CMD_TARGET=$(DESTDIR)$(BINDIR)/oarnodes
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/.oardel CMD_TARGET=$(DESTDIR)$(BINDIR)/oardel
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/.oarstat CMD_TARGET=$(DESTDIR)$(BINDIR)/oarstat
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/.oarsub CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsub
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/.oarconnect CMD_TARGET=$(DESTDIR)$(BINDIR)/oarconnect
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/.oarwalltime CMD_TARGET=$(DESTDIR)$(BINDIR)/oarwalltime
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/.oarhold CMD_TARGET=$(DESTDIR)$(BINDIR)/oarhold
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/.oarresume CMD_TARGET=$(DESTDIR)$(BINDIR)/oarresume


.PHONY: install setup uninstall build clean
