ARG DEBIAN_IMAGE
FROM $DEBIAN_IMAGE

ENV DEBIAN_FRONTEND noninteractive
ENV DEBIAN_PRIORITY critical
ENV DEBCONF_NOWARNINGS yes

RUN apt-get update
RUN apt-get -y install \
    dpkg-dev fakeroot lintian python3-sphinx \
    debhelper po-debconf dh-make python3-all python3-setuptools \
    dh-python python3-sphinx-rtd-theme

ADD create_deb.sh /create_deb.sh

ENTRYPOINT ["/create_deb.sh"]
