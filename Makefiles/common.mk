MODULE=common
SRCDIR=oar

OARDIR_BINFILES = $(SRCDIR)/tools/oarsh/oarsh_shell.in \
	              $(SRCDIR)/tools/oarsh/oarsh.in \
		          $(SRCDIR)/tools/sentinelle.pl

MANDIR_FILES = $(SRCDIR)/../docs/man/man1/oarsh.1 \
	       $(SRCDIR)/../docs/man/man1/oarprint.1 \
	       $(SRCDIR)/../docs/man/man1/oarnodesetting.1

SHAREDIR_FILES = $(SRCDIR)/tools/oar.conf.in \
                   $(SRCDIR)/tools/oarnodesetting_ssh.in \
		   $(SRCDIR)/tools/update_cpuset_id.sh.in

LOGROTATEDIR_FILES = setup/logrotate.d/oar-common.in

PROCESS_TEMPLATE_FILES = $(SRCDIR)/tools/oarsh/oarcp.in \
			 $(SRCDIR)/tools/oardodo.c.in \
			 $(SRCDIR)/tools/oardo.c.in

include Makefiles/shared/shared.mk

clean: clean_shared
	$(MAKE) -f Makefiles/man.mk clean
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/oarsh CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsh
	$(OARDO_CLEAN) CMD_WRAPPER=$(OARDIR)/.oarnodesetting CMD_TARGET=$(DESTDIR)$(SBINDIR)/oarnodesetting
	-rm -f $(SRCDIR)/tools/oardodo

build: build_shared
	$(MAKE) -f Makefiles/man.mk build
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/oarsh CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsh
	$(OARDO_BUILD) CMD_WRAPPER=$(OARDIR)/.oarnodesetting CMD_TARGET=$(DESTDIR)$(SBINDIR)/oarnodesetting
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) -o $(SRCDIR)/tools/oardodo $(SRCDIR)/tools/oardodo.c

install: build install_shared
	for file in .oarnodesetting;\
	do \
		if [ -f  $(DESTDIR)$(BINDIR)/$$file ]; then\
			mv $(DESTDIR)$(BINDIR)/$$file $(DESTDIR)$(OARDIR)/$$file; \
		fi \
	done

	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/oarsh CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsh
	$(OARDO_INSTALL) CMD_WRAPPER=$(OARDIR)/.oarnodesetting CMD_TARGET=$(DESTDIR)$(SBINDIR)/oarnodesetting

	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 $(SRCDIR)/tools/oarsh/oarcp $(DESTDIR)$(BINDIR)/
    # install -m 0755 $(SRCDIR)/qfunctions/oarprint $(DESTDIR)$(BINDIR)

	install -d $(DESTDIR)$(OARDIR)/oardodo
	install -m 0754 $(SRCDIR)/tools/oardodo $(DESTDIR)$(OARDIR)/oardodo

uninstall: uninstall_shared
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/oarsh CMD_TARGET=$(DESTDIR)$(BINDIR)/oarsh
	$(OARDO_UNINSTALL) CMD_WRAPPER=$(OARDIR)/.oarnodesetting CMD_TARGET=$(DESTDIR)$(SBINDIR)/oarnodesetting
	rm -rf $(DESTDIR)$(OARDIR)/oardodo
	rm -rf $(DESTDIR)$(EXAMPLEDIR)

.PHONY: install setup uninstall build clean
