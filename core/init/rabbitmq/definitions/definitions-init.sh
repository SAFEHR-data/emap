#!/usr/bin/env bash
set -euo pipefail

mq_wrapper () {
  rabbitmqadmin -H rabbitmq -u "$RABBITMQ_DEFAULT_USER" -p "$RABBITMQ_DEFAULT_PASS" "$@"
}

mq_wrapper declare exchange name=waveform type=fanout durable=true
mq_wrapper declare queue name=waveform_emap durable=true
mq_wrapper declare queue name=waveform_atriumdb durable=true
mq_wrapper declare binding source=waveform destination=waveform_emap
mq_wrapper declare binding source=waveform destination=waveform_atriumdb