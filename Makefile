DBS_NAME  ?= dbname
XMA_NAME  ?= skeduler
CNX_NAME  ?= cxnname
ACC_NAME  ?= orgname-accountname
IMG_NAME  ?= skeduler_worker
TMS_EXEC  := $(shell /bin/date "+%Y%m%d_%H%M%S")
REG_PATH  := $(shell echo $(ACC_NAME).registry.snowflakecomputing.com/$(DBS_NAME)/$(XMA_NAME)/images | tr '[:upper:]' '[:lower:]')

.PHONY: all setup deploy reset img_build img_push reg_auth app_pack snow_setup snow_deploy snow_reset spec_render help

help:
	@echo "SKEDULER - Deployment Makefile"
	@echo ""
	@echo "Configuration (override with make VAR=value):"
	@echo "  DBS_NAME  = $(DBS_NAME)    (target database)"
	@echo "  XMA_NAME  = $(XMA_NAME)    (target schema)"
	@echo "  CNX_NAME  = $(CNX_NAME)    (snow CLI connection name)"
	@echo "  ACC_NAME  = $(ACC_NAME)    (Snowflake account identifier)"
	@echo "  IMG_NAME  = $(IMG_NAME)    (Docker image name)"
	@echo ""
	@echo "Targets:"
	@echo "  make setup      - Create all Snowflake objects (schema, tables, SPs, task, pool)"
	@echo "  make deploy     - Build image, package app, upload artifacts, deploy"
	@echo "  make reset      - Tear down all Snowflake objects"
	@echo "  make img_build  - Build Docker image locally"
	@echo "  make img_push   - Build, tag, and push image to Snowflake registry"
	@echo "  make app_pack   - Package app/ as app.tar.gz"
	@echo "  make all        - Full deployment (setup + build + deploy)"

all: setup img_push deploy

reset:
	@mkdir -p log
	snow sql -c $(CNX_NAME) -f dbs/reset.sql \
		-D "dbsname=$(DBS_NAME)" \
		-D "xmaname=$(XMA_NAME)" \
		> log/reset_$(TMS_EXEC).log 2>&1
	@echo "Reset complete. See log/reset_$(TMS_EXEC).log"

snow_setup:
	@mkdir -p log
	snow sql -c $(CNX_NAME) -f dbs/setup.sql \
		-D "dbsname=$(DBS_NAME)" \
		-D "xmaname=$(XMA_NAME)" \
		-D "accname=$(ACC_NAME)" \
		> log/setup_$(TMS_EXEC).log 2>&1
	@echo "Setup complete. See log/setup_$(TMS_EXEC).log"

setup: snow_setup

spec_render:
	@mkdir -p .build
	sed 's|&{ accname }|$(ACC_NAME)|g; s|&{ dbsname }|$(DBS_NAME)|g; s|&{ xmaname }|$(XMA_NAME)|g' \
		img/worker_spec.yaml > .build/worker_spec.yaml

app_pack:
	@mkdir -p .build
	rm -f .build/app.tar.gz
	tar -zcvf .build/app.tar.gz app

snow_deploy: app_pack spec_render
	@mkdir -p log
	snow sql -c $(CNX_NAME) -f dbs/deploy.sql \
		-D "dbsname=$(DBS_NAME)" \
		-D "xmaname=$(XMA_NAME)" \
		> log/deploy_$(TMS_EXEC).log 2>&1
	@echo "Deploy complete. See log/deploy_$(TMS_EXEC).log"

deploy: img_push snow_deploy

reg_auth:
	snow spcs image-registry token --connection $(CNX_NAME) --format=JSON | \
		docker login $(ACC_NAME).registry.snowflakecomputing.com \
		--username 0sessiontoken --password-stdin

img_build:
	docker build --platform=linux/amd64 -t $(IMG_NAME):latest ./img

img_push: img_build reg_auth
	docker tag $(IMG_NAME):latest \
		$(REG_PATH)/$(IMG_NAME):latest
	docker push \
		$(REG_PATH)/$(IMG_NAME):latest
	@echo "Image pushed to $(REG_PATH)/$(IMG_NAME):latest"
