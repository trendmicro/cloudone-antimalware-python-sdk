# ROOT_DIR := $(abspath ../../)
# include $(ROOT_DIR)/build-scripts/variables.mk

PIPY_URL ?= https://upload.pypi.org/legacy/
TOKEN ?=

build: clean
	pipenv sync --dev
	pipenv run python -m grpc_tools.protoc -Iamaas/grpc/protos=./amaas/protos \
		--python_out=. \
		--pyi_out=. \
		--grpc_python_out=. \
		./amaas/protos/scan.proto
	pipenv run pipenv-setup sync
	pipenv run python setup.py sdist bdist_wheel

upload:
	#pipenv run twine upload ./dist/*.whl
	pipenv run twine upload --repository-url $(PIPY_URL) -u __token__ -p $(TOKEN) ./dist/*.whl --skip-existing

clean:
	@rm -rf dist build amaas/*.egg-info amaas/grpc/protos/
