# Makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
PAPER         =
BUILDDIR      = _build
APIDOCDIR     = _api
APIDOCDIR_EXT = _api_extensions
# User-friendly check for sphinx-build
ifeq ($(shell which $(SPHINXBUILD) >/dev/null 2>&1; echo $$?), 1)
$(error The '$(SPHINXBUILD)' command was not found. Make sure you have Sphinx installed, then set the SPHINXBUILD environment variable to point to the full path of the '$(SPHINXBUILD)' executable. Alternatively you can add the directory with the executable to your PATH. If you don't have Sphinx installed, grab it from http://sphinx-doc.org/)
endif

# Internal variables.
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

.PHONY: help clean html dirhtml singlehtml pickle json htmlhelp qthelp devhelp epub latex latexpdf text man changes linkcheck doctest gettext

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  publish    to put up to date documenation into gh-pages"
	@echo "  html       to make standalone HTML files"

clean:
	rm -rf $(BUILDDIR)
	rm -f README.rst
	rm -rf $(APIDOCDIR)
	rm -rf $(APIDOCDIR_EXT)

publish: html
	cp -r $(BUILDDIR)/html/* gh-pages/

html: export PYTHONPATH=..
html: apidoc apidoc_extensions README.rst
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

apidoc:
	sphinx-apidoc -o $(APIDOCDIR) ../pyschema
	@echo
	@echo "Generated API documentation from docstrings."

apidoc_extensions:
	sphinx-apidoc -o $(APIDOCDIR_EXT) ../pyschema_extensions
	@echo
	@echo "Generated extension API documentation from docstrings."


README.rst: README.ipynb
	ipython nbconvert --to rst README.ipynb
	@echo
	@echo "Converted notebook doc into rst."
