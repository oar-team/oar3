ARG DEBIAN_IMAGE
FROM $DEBIAN_IMAGE

ENV DEBIAN_FRONTEND noninteractive
ENV DEBIAN_PRIORITY critical
ENV DEBCONF_NOWARNINGS yes

RUN sed -i -e's/ main/ main contrib non-free non-free-firmware/g' /etc/apt/sources.list.d/debian.sources
RUN apt-get update
RUN cat <<'EOF' | debconf-set-selections
kexec-tools     kexec-tools/load_kexec  boolean true
kexec-tools     kexec-tools/use_grub_config     boolean true
EOF

RUN apt-get -y install \
    dpkg-dev fakeroot lintian python3-sphinx \
    debhelper po-debconf dh-make python3-all python3-setuptools \
    dh-python python3-sphinx-rtd-theme libbpf-dev clang \
    bash-completion kexec-tools bpftool \
    nvidia-kernel-dkms libcuda1

ADD create_deb.sh /create_deb.sh

ENTRYPOINT ["/create_deb.sh"]
