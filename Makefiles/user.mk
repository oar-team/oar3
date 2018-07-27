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
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/oarnodes3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarnodes
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/oardel3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oardel
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/oarstat3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarstat
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/oarsub3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsub
	# $(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/oarwalltime3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarwalltime
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/oarhold3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarhold
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/oarresume3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarresume

build: build_shared
	$(MAKE) -f Makefiles/man.mk build
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/oarnodes3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarnodes
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/oardel3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oardel
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/oarstat3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarstat
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/oarsub3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsub
	#$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/oarwalltime3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarwalltime
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/oarhold3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarhold
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/oarresume3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarresume

install: install_shared
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 $(SRCDIR)/tools/oarmonitor_graph_gen.pl $(DESTDIR)$(BINDIR)/oarmonitor_graph_gen
	for file in oar3-almighty oar3-appendice-proxy oar3-bipbip-commander \
		oar3-sarko oar3-finaud oar3-leon oar3-bipbip oar3-node-change-state \
		oarproperty3 oar3-hulot kao kamelot kamelot-fifo \
		oarremoveresource3 oaraccounting3 \
		oarnodes3 oardel3 oarstat3 oarsub3 oarhold3 oarresume3;\
	do \
		if [ -f  $(DESTDIR)$(BINDIR)/$$file ]; then \
			mv $(DESTDIR)$(BINDIR)/$$file $(OARDIR)/$$file; \
		fi \
	done
	# Install wrappers
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/oarnodes3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarnodes
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/oardel3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oardel
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/oarstat3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarstat
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/oarsub3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsub
	# $(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/oarwalltime3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarwalltime
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/oarhold3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarhold
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/oarresume3 CMD_TARGET=$(DESTDIR)$(BINDIR)/oarresume

uninstall: uninstall_shared
	rm -f $(DESTDIR)$(BINDIR)/oarmonitor_graph_gen	

	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/oarnodes CMD_TARGET=$(DESTDIR)$(BINDIR)/oarnodes
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/oardel CMD_TARGET=$(DESTDIR)$(BINDIR)/oardel
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/oarstat CMD_TARGET=$(DESTDIR)$(BINDIR)/oarstat
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/oarsub CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsub
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/oarwalltime CMD_TARGET=$(DESTDIR)$(BINDIR)/oarwalltime
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/oarhold CMD_TARGET=$(DESTDIR)$(BINDIR)/oarhold
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/oarresume CMD_TARGET=$(DESTDIR)$(BINDIR)/oarresume


.PHONY: install setup uninstall build clean
