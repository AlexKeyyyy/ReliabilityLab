"""Drain an existing run queue without republishing its input."""
import argparse
import json
import os
from pathlib import Path

import pika

from .adapter import IDENTIFIER, SCENARIOS, Recovery
from .element import ElementClient
from .runner import Journal, broker_parameters, handle_delivery


def drain(amqp_url, client, manifest, journal):
    config = manifest['config']
    run_id = config['run_id']
    if not isinstance(run_id, str) or not IDENTIFIER.fullmatch(run_id) or config['scenario'] not in SCENARIOS:
        raise ValueError('Invalid run configuration')
    amount_unit = manifest['amount_unit']
    if amount_unit not in ('kopecks', 'rubles'):
        raise ValueError('Invalid generator amount unit')
    queue = 'reliabilitylab.' + run_id
    handled = rejected = 0
    with pika.BlockingConnection(broker_parameters(amqp_url)) as connection:
        channel = connection.channel()
        channel.queue_declare(queue + '.unresolved', passive=True)
        channel.queue_declare(queue, passive=True)
        policy = Recovery(client, config['strategy'], config['config']['max_attempts'], journal, connection.sleep)
        while True:
            delivery, props, body = channel.basic_get(queue, auto_ack=False)
            if delivery is None:
                break
            _, outcome = handle_delivery(channel, delivery, props, body, policy, config, amount_unit, journal)
            handled += 1
            rejected += outcome.state != 'COMPLETED'
            connection.process_data_events(time_limit=0)
        channel.close()
    journal('worker_drained', handled=handled, rejected=rejected)
    return handled, rejected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--journal', type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    timeout = manifest['config']['config']['http_timeout_ms']
    if type(timeout) is not int or not 1 <= timeout <= 60000:
        parser.error('Manifest HTTP timeout is outside the Element contract')
    client = ElementClient.from_environment(timeout / 1000)
    try:
        with args.journal.open('x') as stream:
            journal = Journal(stream, manifest['config']['run_id'])
            handled, rejected = drain(os.environ['RELIABILITYLAB_AMQP_URL'], client, manifest, journal)
    finally:
        client.session.close()
    print(f'Handled {handled} deliveries, rejected {rejected}')
    return int(bool(rejected))


if __name__ == '__main__':
    raise SystemExit(main())
