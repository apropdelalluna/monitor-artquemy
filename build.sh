#!/usr/bin/env bash
set -e
pip install -r requirements_artquemy.txt
playwright install chromium --with-deps 2>/dev/null || playwright install chromium
