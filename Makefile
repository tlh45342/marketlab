# makefile for tcore

.PHONY: all uninstall build clean install reinstall dev

WHEEL := $(wildcard tcore-*.whl)

ifeq ($(OS),Windows_NT)
	RM_FILE = del /q
	RM_DIR  = rmdir /s /q
else
	RM_FILE = rm -f
	RM_DIR  = rm -rf
endif

all: clean build

uninstall:
	pip uninstall tcore -y

build:
	pip wheel .

clean:
	$(RM_FILE) *.whl 2>nul || true
	$(RM_DIR) tcore.egg-info 2>nul || true
	$(RM_DIR) build 2>nul || true

install:
	pip install $(WHEEL)

dev:
	pip install -e .

reinstall: uninstall clean build install
