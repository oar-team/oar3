# Created by pyp2rpm-2.0.0
%global pypi_name oar-lib

Name:           python-%{pypi_name}
Version:        0.3.0
Release:        1%{?dist}
Summary:        OAR common lib

License:        GPLv2+
URL:            https://github.com/oar-team/python-oar-lib
Source0:        https://pypi.python.org/packages/source/o/%{pypi_name}/%{pypi_name}-%{version}.tar.gz
BuildArch:      noarch

BuildRequires:  python2-devel
BuildRequires:  python-setuptools
%description
Python OAR Common Library

Requires:       python-sqlalchemy
Requires:       python-alembic

%prep
%autosetup -n %{pypi_name}-%{version}
# Remove bundled egg-info
rm -rf %{pypi_name}.egg-info

%build
python setup.py build

%install
python setup.py install --prefix=%{_prefix} --root=%{buildroot}

%clean
rm -rf %{buildroot}

%files -n python-%{pypi_name}
%doc README.rst
%{python_sitelib}/oar
%{python_sitelib}/oar_lib
%{python_sitelib}/oar_lib-%{version}-py?.?.egg-info


%changelog
* Tue Sep 29 2015 John Doe <john@doe.com> - 0.3.0-1
- Initial package.
