"""Bounded recovery policy. Broker settlement belongs to the caller."""
from dataclasses import dataclass
from datetime import datetime, timezone
import re
import time
from uuid import uuid4

from .element import UncertainResponse

IDENTIFIER = re.compile(r'[A-Za-z0-9_-]{1,96}')
STRATEGIES = ('A', 'B', 'C')
SCENARIOS = ('normal', 'fail_before_commit', 'drop_response_after_commit', 'duplicate_delivery')


def normalize(source, run_id, amount_unit):
    """Translate Lina's generator schema and preserve stable business fields."""
    if not isinstance(source, dict) or source.get('run_id') != run_id:
        raise ValueError('Message belongs to a different run')
    if type(source.get('schema_version')) is not int or source['schema_version'] != 1:
        raise ValueError('Unsupported message schema version')
    local_id, order_id = source.get('operation_id'), source.get('order_id')
    if not isinstance(local_id, str) or not IDENTIFIER.fullmatch(local_id):
        raise ValueError('Invalid operation ID')
    operation_id = f'{run_id}_{local_id}'
    if not IDENTIFIER.fullmatch(operation_id) or not isinstance(order_id, str) or not IDENTIFIER.fullmatch(order_id):
        raise ValueError('Invalid Element operation or order ID')
    if amount_unit not in ('kopecks', 'rubles'):
        raise ValueError('Choose a unit for the generator amount')
    amount = source.get('amount')
    if type(amount) is not int or amount <= 0:
        raise ValueError('Expected a positive integer amount')
    amount *= 100 if amount_unit == 'rubles' else 1
    if amount > 999999999999:
        raise ValueError('Amount exceeds the Element limit')
    timestamp = source.get('created_at')
    if not isinstance(timestamp, str):
        raise ValueError('Expected an ISO 8601 timestamp')
    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    if timestamp.utcoffset() is None:
        raise ValueError('Expected a timezone-aware timestamp')
    return {
        'schema_version': 1, 'run_id': run_id, 'operation_id': operation_id, 'order_id': order_id,
        'amount_minor': amount, 'currency': 'RUB', 'created_at': timestamp.astimezone(timezone.utc)
        .isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
    }


@dataclass(frozen=True)
class Outcome:
    state: str
    order_ref: str | None = None
    reason: str | None = None


def uncertainty(status, body):
    error = body.get('error')
    return (status >= 500 or status in (408, 429)
            or (status == 409 and isinstance(error, dict) and error.get('code') == 'ATTEMPT_IN_PROGRESS'))


def matches_order(value, message):
    return (isinstance(value, dict) and isinstance(value.get('order_ref'), str) and bool(value['order_ref'])
            and all(value.get(field) == message[field] for field in ('order_id', 'amount_minor', 'currency')))


class Recovery:
    def __init__(self, client, strategy, max_attempts, journal, pause=time.sleep):
        if strategy not in STRATEGIES or not 1 <= max_attempts <= 10:
            raise ValueError('Invalid recovery parameters')
        self.client = client
        self.strategy = strategy
        self.max_attempts = max_attempts
        self.journal = journal
        self.pause = pause

    def run(self, message, *, recover=False, drop_response=False):
        operation_id = message['operation_id']
        path = '/operations/' + operation_id
        started = time.monotonic()
        unknown_since = None

        def record(event, **data):
            self.journal(event, operation_id=operation_id, **data)

        def exchange(method, route, payload=None):
            began = time.monotonic()
            try:
                status, body = self.client.request(method, route, payload)
            except UncertainResponse:
                status, body = 0, {}
            error = body.get('error')
            record('http_exchange', method=method, path=route, status=status,
                   attempt_id=payload.get('attempt_id') if payload else None,
                   error=error.get('code') if isinstance(error, dict) else None,
                   elapsed_ms=round((time.monotonic() - began) * 1000, 3))
            return status, body

        def flag_unknown(reason):
            nonlocal unknown_since
            if unknown_since is None:
                unknown_since = time.monotonic()
                record('outcome_unknown', reason=reason, detection_ms=round((unknown_since - started) * 1000, 3))

        def confirmed(order):
            now = time.monotonic()
            record('order_confirmed', order_ref=order['order_ref'], elapsed_ms=round((now - started) * 1000, 3),
                   recovery_ms=round((now - unknown_since) * 1000, 3) if unknown_since is not None else None)
            return Outcome('COMPLETED', order['order_ref'])

        for index in range(self.max_attempts):
            status, body = exchange('POST', '/messages', dict(message, attempt_id=str(uuid4())))
            if (status in (200, 201) and body.get('durable_stage') == 'INBOX_PERSISTED'
                    and body.get('operation_id') == operation_id and body.get('run_id') == message['run_id']):
                break
            if status and not uncertainty(status, body) and status not in (200, 201):
                state = 'UNKNOWN' if unknown_since is not None or recover else 'REJECTED'
                return Outcome(state, reason=f'RECEIVE_HTTP_{status}')
            flag_unknown('INBOX_UNCONFIRMED')
            if index + 1 < self.max_attempts:
                self.pause(0.1)
        else:
            return Outcome('UNKNOWN', reason='INBOX_RETRY_LIMIT')

        recovering = recover
        for index in range(self.max_attempts):
            if index:
                self.pause(0.1)
            if recovering and self.strategy == 'B':
                status, body = exchange('GET', path)
                if (status != 200 or body.get('operation_id') != operation_id
                        or body.get('run_id') != message['run_id'] or not isinstance(body.get('orders'), list)
                        or body.get('business_effect_count') != len(body['orders'])):
                    flag_unknown('STATE_UNCONFIRMED')
                    continue
                if body['orders']:
                    if body.get('server_state') == 'COMPLETED' and all(matches_order(order, message) for order in body['orders']):
                        return confirmed(body['orders'][0])
                    flag_unknown('INCONSISTENT_ORDER_STATE')
                    continue
                if body.get('server_state') != 'RECEIVED':
                    flag_unknown('INCONSISTENT_EMPTY_STATE')
                    continue

            attempt_id = str(uuid4())
            status, body = exchange('POST', path + ('/retry' if recovering else '/process'), {'attempt_id': attempt_id})
            order = body.get('result')
            if (status == 200 and body.get('durable_stage') == 'BUSINESS_COMMITTED'
                    and body.get('operation_id') == operation_id and body.get('run_id') == message['run_id']
                    and body.get('attempt_id') == attempt_id and matches_order(order, message)):
                if not (drop_response and index == 0):
                    return confirmed(order)
                record('response_discarded', attempt_id=attempt_id)
                flag_unknown('MODELED_RESPONSE_LOSS')
            elif status == 500 and isinstance(body.get('error'), dict) and body['error'].get('code') == 'INJECTED_BEFORE_COMMIT':
                record('attempt_rolled_back', attempt_id=attempt_id)
            elif status and status != 200 and not uncertainty(status, body):
                # A rejection of a retry cannot establish the outcome of an earlier ambiguous call.
                state = 'UNKNOWN' if unknown_since is not None or recovering else 'REJECTED'
                return Outcome(state, reason=f'PROCESS_HTTP_{status}')
            else:
                flag_unknown('PROCESS_UNCONFIRMED')
            recovering = True
        return Outcome('UNKNOWN', reason='RECOVERY_RETRY_LIMIT')
