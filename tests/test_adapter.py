import unittest
from uuid import UUID

from reliabilitylab.adapter import Recovery, normalize
from reliabilitylab.element import UncertainResponse
from reliabilitylab.runner import settle

SOURCE = {'run_id': 'run', 'operation_id': 'op', 'order_id': 'order', 'schema_version': 1,
          'amount': 1500, 'created_at': '2026-11-20T03:00:00+03:00'}
MESSAGE = normalize(SOURCE, 'run', 'kopecks')
ORDER = dict(order_id='order', amount_minor=1500, currency='RUB', order_ref='original-order')
IDENTITY = dict(operation_id='run_op', run_id='run')


class ScriptedElement:
    def __init__(self, *replies):
        self.replies = iter(replies)
        self.calls = []

    def request(self, method, path, data):
        self.calls.append((method, path, data))
        result = next(self.replies)
        if isinstance(result, Exception):
            raise result
        if callable(result):
            return result(data)
        return result


def accepted():
    return 201, dict(IDENTITY, durable_stage='INBOX_PERSISTED')


def committed(data):
    return 200, dict(IDENTITY, attempt_id=data['attempt_id'], durable_stage='BUSINESS_COMMITTED', result=ORDER)


class RecoveryTests(unittest.TestCase):
    def run_policy(self, strategy, *replies, **options):
        client = ScriptedElement(*replies)
        self.events = []
        outcome = Recovery(client, strategy, 3, lambda event, **fields: self.events.append((event, fields)),
                           pause=lambda _: None).run(MESSAGE, **options)
        return outcome, client.calls

    def test_b_confirms_committed_order_via_read_without_retry(self):
        state = dict(IDENTITY, orders=[ORDER], business_effect_count=1, server_state='COMPLETED')
        outcome, calls = self.run_policy('B', accepted(), committed, (200, state), drop_response=True)
        self.assertEqual(outcome.order_ref, ORDER['order_ref'])
        self.assertEqual([method for method, _, _ in calls], ['POST', 'POST', 'GET'])
        self.assertIn('outcome_unknown', [event for event, _ in self.events])

    def test_b_does_not_turn_unavailable_state_into_absent_order(self):
        outcome, calls = self.run_policy('B', accepted(), UncertainResponse(), (503, {}), (503, {}))
        self.assertEqual(outcome.state, 'UNKNOWN')
        self.assertEqual([method for method, _, _ in calls], ['POST', 'POST', 'GET', 'GET'])

    def test_a_and_c_retry_with_a_new_attempt_and_stable_operation(self):
        for strategy in ('A', 'C'):
            with self.subTest(strategy=strategy):
                outcome, calls = self.run_policy(strategy, accepted(), committed, committed, drop_response=True)
                self.assertEqual(outcome.state, 'COMPLETED')
                self.assertEqual([path for _, path, _ in calls],
                                 ['/messages', '/operations/run_op/process', '/operations/run_op/retry'])
                ids = [UUID(data['attempt_id']) for _, _, data in calls]
                self.assertEqual(len(set(ids)), 3)
                self.assertTrue(all(identifier.version == 4 for identifier in ids))

    def test_malformed_success_never_causes_ack(self):
        outcome, _ = self.run_policy('C', accepted(), (200, {}), (200, {}), (200, {}))
        self.assertEqual(outcome.state, 'UNKNOWN')

        class Delivery:
            def __init__(self):
                self.rejected = []

            def basic_ack(self, _):
                raise AssertionError('Unconfirmed order was acknowledged')

            def basic_reject(self, delivery, requeue):
                self.rejected.append((delivery, requeue))

        channel = Delivery()
        settle(channel, 17, outcome, lambda *_args, **_kwargs: None, MESSAGE['operation_id'])
        self.assertEqual(channel.rejected, [(17, False)])

    def test_normalization_requires_explicit_unit_and_timezone(self):
        self.assertEqual(normalize(SOURCE, 'run', 'rubles')['amount_minor'], 150000)
        self.assertEqual(MESSAGE['created_at'], '2026-11-20T00:00:00.000Z')
        for invalid in (dict(SOURCE, amount=True), dict(SOURCE, created_at='2026-11-20T00:00:00'),
                        dict(SOURCE, operation_id='../other')):
            with self.assertRaises(ValueError):
                normalize(invalid, 'run', 'kopecks')
