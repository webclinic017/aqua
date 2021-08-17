#!/usr/bin/env bash
set -e
isort src tests
black src tests
pylint src tests