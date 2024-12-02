#!/bin/bash

################################################################################################
# Project: SmartTraffic_Lakehouse_for_HCMC
# Author: Nguyen Trung Nghia (ren294)
# Contact: trungnghia294@gmail.com
# GitHub: Ren294
################################################################################################

superset fab create-admin --username "$ADMIN_USERNAME" --firstname Superset --lastname Admin --email "$ADMIN_EMAIL" --password "$ADMIN_PASSWORD"

superset db upgrade
superset superset init
/bin/sh -c /usr/bin/run-server.sh