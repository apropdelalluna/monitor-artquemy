#!/usr/bin/env bash
set -e
pip install -r requirements_artquemy.txt
playwright install chromium
playwright install-deps chromium
