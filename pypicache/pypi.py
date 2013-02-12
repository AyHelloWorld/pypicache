"""Handles requests to the real PyPI package index

"""

import json
import logging

# Forward compatible with python 3
try:
    import xmlrpc.client as xmlrpclib
    xmlrpclib  # shut pyflakes up
except ImportError:
    import xmlrpclib

import requests

from pypicache import exceptions

def get_uri(uri):
    """Request the given URI and return the response

    Checks for 200 response and raises appropriate exceptions otherwise.

    """
    response = requests.get(uri)
    if response.status_code == 404:
        raise exceptions.NotFound("Can't locate {a}: {b}".format(a = uri, b = response))
    elif response.status_code != 200:
        raise exceptions.RemoteError("Unexpected response from {a}: {b}".format(a = uri, b = response))
    return response

class PyPI(object):
    """Handles requests to the real PyPI servers

    """
    def __init__(self, pypi_server="http://pypi.python.org/"):
        self.log = logging.getLogger("pypi")
        if not pypi_server.endswith("/"):
            pypi_server = pypi_server + "/"
        self.pypi_server = pypi_server
        # Certain operations aren't available via the JSON api (well, at least obviously)
        self.xmlrpc_client = xmlrpclib.ServerProxy("{a}pypi".format(a = self.pypi_server))

    def get_versions(self, package, show_hidden=False):
        """Returns a list of available versions for a package

        This calls the PyPI XML-RPC API. See
        http://wiki.python.org/moin/PyPiXmlRpc

        :param package: Name of the package
        :param show_hidden: Show versions marked as hidden, can be very slow.
        :returns: A list of version strings

        """
        versions = self.xmlrpc_client.package_releases(package, show_hidden)
        self.log.debug("Got versions {a} for package {b!r}".format(a = versions, b = package))
        return versions

    def get_urls(self, package, version):
        """Get a list of URLS for the given package

        This takes the url info returned from the PyPI JSON API and only
        returns urls dicts.

        See http://wiki.python.org/moin/PyPiJson

        """
        uri = "{server}pypi/{package}/{version}/json".format(
            server=self.pypi_server,
            package=package,
            version=version,
        )
        self.log.info("Fetching JSON info from {a}".format(a = uri))
        r = get_uri(uri)
        for url in json.loads(r.content)["urls"]:
            yield url

    def get_simple_package_info(self, package):
        uri = "{a}simple/{b}/".format(a = self.pypi_server, b = package)
        r = get_uri(uri)
        return r.content
        # TODO WIP in progress, trying to reproduce a simple page with links only
        # Better still to rewrite using local urls
        # self.log.info("Checking PyPI for links to {}".format(package))
        # links = []
        # for version in self.pypi_get_versions(package):
        #     self.log.debug("Found package {} version {}".format(package, version))
        #     for url in self.pypi_get_sdist_urls(package, version):
        #         self.log.debug("Found link {!r}".format(url))
        #         links.append("""<a href="{url}#md5={md5_digest}">{filename}</a>""".format(**url))

        # self.log.debug("Found links {}".format(links))
        # return SIMPLE_PACKAGE_PAGE_TEMPLATE.format(package=package, links="\n".join(links))

    def get_file(self, package, filename, python_version=None):
        """

        :returns: package data

        """
        if python_version is not None:
            uri = "{a}packages/{b}/{c}/{d}/{e}".format(a = self.pypi_server, b = python_version, c = package[0], d = package, e = filename)
        else:
            uri = "{a}packages/source/{b}/{c}/{d}".format(a = self.pypi_server, b = package[0], c = package, d = filename)
        self.log.debug("Fetching from {a}".format(a = uri))
        r = get_uri(uri)
        return r.content
