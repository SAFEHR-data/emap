#!/usr/bin/env bash
set -euo pipefail

rabbitmqadmin -H rabbitmq declare exchange name=waveform type=fanout durable=true -u "$RABBITMQ_DEFAULT_USER" -p "$RABBITMQ_DEFAULT_PASS"
rabbitmqadmin -H rabbitmq declare queue name=waveform_emap durable=true -u "$RABBITMQ_DEFAULT_USER" -p "$RABBITMQ_DEFAULT_PASS"
rabbitmqadmin -H rabbitmq declare queue name=waveform_atriumdb durable=true -u "$RABBITMQ_DEFAULT_USER" -p "$RABBITMQ_DEFAULT_PASS"
rabbitmqadmin -H rabbitmq declare binding source=waveform destination=waveform_emap -u "$RABBITMQ_DEFAULT_USER" -p "$RABBITMQ_DEFAULT_PASS"
rabbitmqadmin -H rabbitmq declare binding source=waveform destination=waveform_atriumdb -u "$RABBITMQ_DEFAULT_USER" -p "$RABBITMQ_DEFAULT_PASS"