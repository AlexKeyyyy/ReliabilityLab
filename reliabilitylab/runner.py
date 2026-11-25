"""Run one isolated experiment against RabbitMQ and the published Element API."""
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import sys
import time
from uuid import uuid4

import pika

from .adapter import SCENARIOS, STRATEGIES, Outcome, Recovery, matches_order, normalize
from .element import ElementClient


class Journal:
    def __init__(self, stream, run_id):
        self.stream = stream
        self.run_id = run_id
        self.events = []

    def __call__(self, event, **data):
        item = {'sequence': len(self.events) + 1, 'timestamp': datetime.now(timezone.utc).isoformat(),
                'run_id': self.run_id, 'event': event, **data}
        self.stream.write(json.dumps(item, ensure_ascii=False) + '\n')
        self.stream.flush()
        self.events.append(item)


def checked(client, method, path, status, payload=None):
    code, response = client.request(method, path, payload)
    if code != status:
        raise RuntimeError(f'{method} {path}: HTTP {code}, expected {status}')
    return response


def settle(channel, delivery, outcome, journal, operation_id):
    if outcome.state == 'COMPLETED' and outcome.order_ref:
        channel.basic_ack(delivery)
        journal('consumer_ack', operation_id=operation_id, order_ref=outcome.order_ref)
    else:
        journal('unresolved', operation_id=operation_id, state=outcome.state, reason=outcome.reason)
        channel.basic_reject(delivery, requeue=False)
        journal('consumer_reject', operation_id=operation_id)


def run_broker(amqp_url, client, config, inputs, options, journal):
    run_id = config['run_id']
    queue = 'reliabilitylab.' + run_id
    dead = queue + '.unresolved'
    params = pika.URLParameters(amqp_url)
    params.heartbeat = 120
    params.socket_timeout = 10
    params.blocked_connection_timeout = 15
    with pika.BlockingConnection(params) as connection:
        channel = connection.channel()
        channel.queue_declare(queue=dead, durable=True, arguments={'x-queue-type': 'quorum'})
        channel.queue_declare(queue=queue, durable=True, arguments={
            'x-queue-type': 'quorum', 'x-overflow': 'reject-publish',
            'x-dead-letter-strategy': 'at-least-once', 'x-dead-letter-exchange': '',
            'x-dead-letter-routing-key': dead, 'x-delivery-limit': options.max_attempts,
        })
        channel.confirm_delivery()
        checked(client, 'POST', '/runs', 201, config)
        replay = options.scenario == 'duplicate_delivery'
        for source, message in inputs:
            operation_id = message['operation_id']
            for copy in range(2 if replay else 1):
                channel.basic_publish('', queue, json.dumps(source).encode(), mandatory=True,
                                      properties=pika.BasicProperties(delivery_mode=2,
                                                                     message_id=f'{operation_id}.{copy}',
                                                                     headers={'experiment_copy': copy}))
                journal('publisher_confirmed', operation_id=operation_id, copy=copy)

        worker = connection.channel()
        policy = Recovery(client, options.strategy, options.max_attempts, journal, connection.sleep)
        outcomes = {}
        rejected = 0
        for _ in range(len(inputs) * (2 if replay else 1)):
            frame, props, body = worker.basic_get(queue=queue, auto_ack=False)
            if frame is None:
                raise RuntimeError('Broker returned no message for a confirmed publish')
            operation_id = None
            try:
                message = normalize(json.loads(body), run_id, options.amount_unit)
            except (ValueError, TypeError):
                outcome = Outcome('REJECTED', reason='INVALID_MESSAGE')
            else:
                operation_id = message['operation_id']
                repeated = frame.redelivered or bool((props.headers or {}).get('experiment_copy', 0))
                journal('broker_delivery', operation_id=operation_id, redelivered=frame.redelivered,
                        experiment_copy=(props.headers or {}).get('experiment_copy', 0))
                outcome = policy.run(message, recover=repeated,
                                     drop_response=options.scenario == 'drop_response_after_commit' and not repeated)
                outcomes[operation_id] = outcome
            settle(worker, frame.delivery_tag, outcome, journal, operation_id)
            rejected += outcome.state != 'COMPLETED'
            connection.process_data_events(time_limit=0)
        worker.close()
        if channel.queue_declare(queue=queue, passive=True).method.message_count:
            raise RuntimeError('Input queue still contains messages after settlement')
        deadline = time.monotonic() + 10
        parked = channel.queue_declare(queue=dead, passive=True).method.message_count
        while parked < rejected and time.monotonic() < deadline:
            connection.sleep(0.1)
            parked = channel.queue_declare(queue=dead, passive=True).method.message_count
        if parked != rejected:
            raise RuntimeError(f'Rejected {rejected} deliveries but found {parked} in the unresolved queue')
        journal('broker_settled', rejected=rejected, parked=parked)
        return outcomes, {'queue': queue, 'unresolved_queue': dead, 'parked': parked, 'version': options.broker_version}


def audit(client, inputs, outcomes):
    rows = []
    for _, message in inputs:
        identifier = message['operation_id']
        outcome = outcomes.get(identifier, Outcome('UNKNOWN', reason='MISSING_OBSERVATION'))
        status, body = client.request('GET', '/operations/' + identifier)
        error = body.get('error')
        if status == 404 and isinstance(error, dict) and error.get('code') == 'OPERATION_NOT_FOUND':
            orders = []
        elif (status == 200 and body.get('operation_id') == identifier and body.get('run_id') == message['run_id']
              and isinstance(body.get('orders'), list) and body.get('business_effect_count') == len(body['orders'])
              and all(matches_order(order, message) for order in body['orders'])):
            orders = [order['order_ref'] for order in body['orders']]
        else:
            raise RuntimeError(f'Ground-truth query unavailable for {identifier} (HTTP {status})')
        rows.append({'operation_id': identifier, 'client_state': outcome.state, 'client_reason': outcome.reason,
                     'acknowledged_order_ref': outcome.order_ref, 'order_refs': orders,
                     'business_effect_count': len(orders)})
    return rows


def summarize(rows, journal, metrics):
    count = Counter(event['event'] for event in journal.events)
    known_unknown = {event['operation_id'] for event in journal.events if event['event'] == 'outcome_unknown'}
    completed = sum(row['business_effect_count'] > 0 for row in rows)
    duplicates = sum(max(0, row['business_effect_count'] - 1) for row in rows)
    if (completed, duplicates) != (metrics['completed'], metrics['duplicate_business_effects']):
        raise RuntimeError('Server aggregates disagree with independent order audit')
    confirmed = [event for event in journal.events if event['event'] == 'order_confirmed']
    http_calls = count['http_exchange']
    return {
        'sent_operations': len(rows), 'published_deliveries': count['publisher_confirmed'],
        'received_operations': metrics['received'], 'completed_operations': completed,
        'correctly_completed_operations': sum(row['business_effect_count'] == 1 for row in rows),
        'duplicate_business_effects': duplicates, 'unknown_observed_operations': len(known_unknown),
        'unknown_final_operations': sum(row['client_state'] == 'UNKNOWN' for row in rows),
        'confirmed_order_mismatches': sum(row['client_state'] == 'COMPLETED' and
                                          row['acknowledged_order_ref'] not in row['order_refs'] for row in rows),
        'acks': count['consumer_ack'], 'rejects': count['consumer_reject'], 'http_calls': http_calls,
        'extra_http_calls_over_receive_and_process_per_delivery': max(0, http_calls - 2 * count['broker_delivery']),
        'client_processing_ms': [event['elapsed_ms'] for event in confirmed],
        'detection_ms': [event['detection_ms'] for event in journal.events if event['event'] == 'outcome_unknown'],
        'recovery_ms': [event['recovery_ms'] for event in confirmed if event['recovery_ms'] is not None],
    }


def run_experiment(options, client, amqp_url):
    sys.path.insert(0, str(options.generator_root.resolve()))
    from generator.order_generator import OrderGenerator

    run_id = f'exp_{options.strategy.lower()}_{uuid4().hex[:16]}'
    config = {'run_id': run_id, 'strategy': options.strategy, 'scenario': options.scenario, 'seed': options.seed,
              'dataset_id': f'lina_{options.count}_{options.seed}_{options.amount_unit}', 'fault_plan_id': options.scenario,
              'expected_count': options.count, 'baseline_enabled': options.strategy in ('A', 'B'),
              'config': {'http_timeout_ms': options.timeout_ms, 'max_attempts': options.max_attempts}}
    generated = OrderGenerator(count=options.count, seed=options.seed, run_id=run_id).generate()
    inputs = [(source, normalize(source, run_id, options.amount_unit)) for source in generated]
    if len(inputs) != options.count or len({message['operation_id'] for _, message in inputs}) != options.count:
        raise ValueError('Generator did not produce the expected number of distinct operation IDs')
    target = options.output / run_id
    target.mkdir(parents=True)
    (target / 'manifest.json').write_text(json.dumps({
        'config': config, 'element_version': options.element_version, 'generator_commit': options.generator_commit,
        'runner_commit': options.runner_commit, 'python': platform.python_version(),
        'pika': pika.__version__, 'amount_unit': options.amount_unit, 'rabbitmq': options.broker_version,
    }, indent=2) + '\n')
    (target / 'input.jsonl').write_text(''.join(json.dumps(source) + '\n' for source, _ in inputs))
    with (target / 'events.jsonl').open('x') as stream:
        journal = Journal(stream, run_id)
        outcomes, broker = run_broker(amqp_url, client, config, inputs, options, journal)
        rows = audit(client, inputs, outcomes)
        metrics = checked(client, 'GET', f'/runs/{run_id}/metrics', 200)
        result = {'run_id': run_id, 'configuration': config, 'broker': broker, 'metrics': metrics,
                  'summary': summarize(rows, journal, metrics)}
    with (target / 'orders.csv').open('x', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row, order_refs=json.dumps(row['order_refs'])))
    (target / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('generator_root', type=Path)
    parser.add_argument('--strategy', required=True, choices=STRATEGIES)
    parser.add_argument('--scenario', required=True, choices=SCENARIOS)
    parser.add_argument('--amount-unit', required=True, choices=('kopecks', 'rubles'))
    parser.add_argument('--count', type=int, default=100)
    parser.add_argument('--seed', type=int, default=101)
    parser.add_argument('--timeout-ms', type=int, default=5000)
    parser.add_argument('--max-attempts', type=int, default=3)
    parser.add_argument('--output', type=Path, default=Path('.local/runs'))
    parser.add_argument('--element-version', required=True)
    parser.add_argument('--generator-commit', required=True)
    parser.add_argument('--runner-commit', required=True)
    parser.add_argument('--broker-version', required=True)
    options = parser.parse_args()
    if not (1 <= options.count <= 500 and 0 <= options.seed < 2**31
            and 1 <= options.timeout_ms <= 60000 and 1 <= options.max_attempts <= 10):
        parser.error('Count, seed, timeout or retry limit falls outside the Element contract')
    return options


def main():
    options = parse_args()
    client = ElementClient.from_environment(options.timeout_ms / 1000)
    try:
        result = run_experiment(options, client, os.environ['RELIABILITYLAB_AMQP_URL'])
    finally:
        client.session.close()
    print(json.dumps(result, indent=2))
    return int(bool(result['summary']['rejects'] or result['summary']['confirmed_order_mismatches']))


if __name__ == '__main__':
    raise SystemExit(main())
