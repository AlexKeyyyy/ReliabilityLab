"""One synthetic delivery from Lina's generator through RabbitMQ to Element."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

import pika

from test_element_api import get_service_token, request, run_body


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def call(method, path, expected, payload=None):
    status, result = request(method, path, payload)
    require(status == expected, f'{method} {path}: HTTP {status}, expected {expected}')
    return result


def element_message(source):
    # Lina's generator uses IDs local to a dataset and a separate attempt label.
    # For this synthetic smoke check we interpret its integer amount as kopecks.
    return {
        'schema_version': source['schema_version'],
        'run_id': source['run_id'],
        'operation_id': f"{source['run_id']}_{source['operation_id']}",
        'order_id': source['order_id'],
        'amount_minor': source['amount'],
        'currency': 'RUB',
        'created_at': datetime.fromisoformat(source['created_at']).astimezone(timezone.utc)
        .isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
        'attempt_id': str(uuid4()),
    }


def smoke(generator_root, amqp_url):
    sys.path.insert(0, str(generator_root.resolve()))
    from generator.order_generator import OrderGenerator

    config = dict(run_body('duplicate_delivery'), expected_count=1, dataset_id='lina_smoke')
    run_id = config['run_id']
    source, = OrderGenerator(count=1, seed=config['seed'], run_id=run_id).generate()
    events = []

    def log(event, **details):
        events.append({'event': event, 'at': datetime.now(timezone.utc).isoformat(), **details})

    connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
    try:
        publisher = connection.channel()
        queue = publisher.queue_declare(queue='', exclusive=True).method.queue
        publisher.confirm_delivery()
        call('POST', '/runs', 201, config)
        publisher.basic_publish('', queue, json.dumps(source).encode(), mandatory=True)
        log('publisher_confirmed')

        worker = connection.channel()
        delivery, _, body = worker.basic_get(queue=queue, auto_ack=False)
        require(delivery is not None and not delivery.redelivered, 'First broker delivery missing')
        message = element_message(json.loads(body))
        op = '/operations/' + message['operation_id']
        receive = call('POST', '/messages', 201, message)
        require(receive['durable_stage'] == 'INBOX_PERSISTED', 'Inbox not confirmed')
        log('inbox_persisted', attempt_id=message['attempt_id'])
        first_attempt = str(uuid4())
        result = call('POST', op + '/process', 200, {'attempt_id': first_attempt})
        require(result['durable_stage'] == 'BUSINESS_COMMITTED' and not result['replayed'],
                'Business commit not confirmed')
        order_ref = result['result']['order_ref']
        log('committed', order_ref=order_ref, attempt_id=first_attempt)
        worker.close()  # No ACK on this channel. Real broker requeue on channel close.
        log('closed_before_ack')

        worker = connection.channel()
        delivery, _, body = worker.basic_get(queue=queue, auto_ack=False)
        require(delivery is not None and delivery.redelivered, 'Expected broker redelivery')
        repeated = element_message(json.loads(body))
        require(repeated['operation_id'] == message['operation_id'], 'Wrong redelivered operation')
        log('broker_redelivery', redelivered=True)
        receive = call('POST', '/messages', 200, repeated)
        require(receive['replayed'], 'Second receipt was not identified as a repeat')
        retry_id = str(uuid4())
        result = call('POST', op + '/retry', 200, {'attempt_id': retry_id})
        require(result['replayed'] and result['durable_stage'] == 'BUSINESS_COMMITTED'
                and result['result']['order_ref'] == order_ref, 'C did not recover the same order')
        log('result_replayed', order_ref=order_ref, attempt_id=retry_id)
        state = call('GET', op, 200)
        require(state['business_effect_count'] == 1 and state['orders'][0]['order_ref'] == order_ref,
                'Element did not confirm exactly one order in the smoke check')
        log('order_verified', count=state['business_effect_count'], order_ref=order_ref)
        worker.basic_ack(delivery.delivery_tag)
        worker.close()  # Ensures ACK was processed before closing; pending messages would requeue.
        log('consumer_ack')
        require(publisher.queue_declare(queue, passive=True).method.message_count == 0,
                'Queue is not empty after ACK and channel close')
        log('queue_empty_after_ack')
        return {'run_id': run_id, 'operation_id': message['operation_id'], 'configuration': config,
                'generator_message': source, 'element_state': state, 'events': events}
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('generator_root', type=Path, help="Path to Lina's python/ directory")
    args = parser.parse_args()
    if not os.environ.get('RELIABILITYLAB_API_TOKEN'):
        os.environ['RELIABILITYLAB_API_TOKEN'] = get_service_token()
    print(json.dumps(smoke(args.generator_root, os.environ['RELIABILITYLAB_AMQP_URL']), indent=2))


if __name__ == '__main__':
    main()
