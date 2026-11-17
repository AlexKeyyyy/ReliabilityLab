PYTHON ?= python3
VENV_PYTHON := .venv/bin/python
CODEX_SKILLS ?= $(HOME)/.codex/skills

.PHONY: setup validate explore
setup:
	$(PYTHON) -m venv .venv
	$(VENV_PYTHON) -m pip install -r requirements-dev.txt

validate:
	$(VENV_PYTHON) "$(CODEX_SKILLS)/xbsl-validate/scripts/validate.py" alexkey --format text

explore:
	$(VENV_PYTHON) "$(CODEX_SKILLS)/xbsl-explore/scripts/explore.py" --type Документ --name Order --root alexkey

VERSION ?= 1.0-5
.PHONY: build test-live
build:
	$(VENV_PYTHON) "$(CODEX_SKILLS)/xbsl-deploy/scripts/build.py" --project-dir alexkey/ReliabilityLab --output build --version "$(VERSION)"

test-live:
	@test -n "$$RELIABILITYLAB_API_BASE_URL" || (echo "Set RELIABILITYLAB_API_BASE_URL to an explicit test application"; exit 2)
	$(VENV_PYTHON) -m unittest discover -s tests -v
