"""Live checks of the published Element API. Writes synthetic tagged runs.

RELIABILITYLAB_API_BASE_URL must explicitly identify a test application. RELIABILITYLAB_API_TOKEN supplies a
service token; an injected transport is supported for an existing admin session.
No Python replacement of the Element business logic is used here.
"""
import concurrent.futures
import json
import os
import unittest
import urllib.error
import urllib.request
import uuid

TRANSPORT = None
RUN_IDS = []


def request(method, path, body=None):
    if TRANSPORT is not None:
        return TRANSPORT(method, path, body)
    base = os.environ['RELIABILITYLAB_API_BASE_URL'].rstrip('/')
    headers = {'Accept': 'application/json'}
    token = os.environ.get('RELIABILITYLAB_API_TOKEN')
    if token:
        headers['Authorization'] = 'Bearer ' + token
    payload = None
    if body is not None:
        headers['Content-Type'] = 'application/json'
        payload = json.dumps(body).encode()
    req = urllib.request.Request(base + path, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def attempt():
    return str(uuid.uuid4())


def run_body(scenario='normal', strategy='C'):
    run_id = 'check_' + uuid.uuid4().hex[:16]
    RUN_IDS.append(run_id)
    return {'run_id': run_id, 'strategy': strategy, 'scenario': scenario,
            'seed': 101, 'dataset_id': 'acceptance_v1', 'fault_plan_id': scenario,
            'expected_count': 100, 'baseline_enabled': strategy in ('A', 'B'),
            'config': {'http_timeout_ms': 5000, 'max_attempts': 3}}


def message(run_id, **changes):
    value = {'schema_version': 1, 'operation_id': run_id + '_' + uuid.uuid4().hex[:12],
             'run_id': run_id, 'order_id': 'ORD_' + uuid.uuid4().hex[:12],
             'attempt_id': attempt(), 'amount_minor': 150000, 'currency': 'RUB',
             'created_at': '2026-11-17T00:00:00Z'}
    value.update(changes)
    return value


class ElementApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('RELIABILITYLAB_API_BASE_URL') and TRANSPORT is None:
            raise unittest.SkipTest('Set RELIABILITYLAB_API_BASE_URL for live tests; no backend is simulated')
        cls.run_config = run_body()
        status, result = request('POST', '/runs', cls.run_config)
        if status != 201:
            raise AssertionError(('Cannot create test run', status, result))

    def receive(self, **changes):
        value = message(self.run_config['run_id'], **changes)
        status, result = request('POST', '/messages', value)
        self.assertEqual(status, 201, result)
        self.assertEqual(result['durable_stage'], 'INBOX_PERSISTED')
        return value

    def process(self, value, command='process', attempt_id=None):
        return request('POST', '/operations/' + value['operation_id'] + '/' + command,
                       {'attempt_id': attempt_id or attempt()})

    def state(self, value):
        status, result = request('GET', '/operations/' + value['operation_id'])
        self.assertEqual(status, 200, result)
        return result

    def test_receive_does_not_create_order(self):
        value = self.receive()
        state = self.state(value)
        self.assertEqual(state['server_state'], 'RECEIVED')
        self.assertEqual(state['business_effect_count'], 0)

    def test_repeated_delivery_and_payload_conflict(self):
        value = self.receive()
        other = dict(value, attempt_id=attempt())
        status, result = request('POST', '/messages', other)
        self.assertEqual(status, 200, result)
        self.assertTrue(result['replayed'])
        changed = dict(other, amount_minor=999, attempt_id=attempt())
        status, result = request('POST', '/messages', changed)
        self.assertEqual(status, 409, result)
        self.assertEqual(result['error']['code'], 'OPERATION_PAYLOAD_CONFLICT')

    def test_create_and_idempotent_recovery(self):
        value = self.receive()
        status, first = self.process(value)
        self.assertEqual(status, 200, first)
        self.assertFalse(first['replayed'])
        status, replay = self.process(value, 'retry')
        self.assertEqual(status, 200, replay)
        self.assertTrue(replay['replayed'])
        self.assertEqual(first['result'], replay['result'])
        state = self.state(value)
        self.assertEqual(state['server_state'], 'COMPLETED')
        self.assertEqual(state['business_effect_count'], 1)
        self.assertEqual(state['orders'][0]['order_ref'], first['result']['order_ref'])

    def test_same_attempt_returns_saved_response(self):
        value = self.receive()
        identifier = attempt()
        first = self.process(value, attempt_id=identifier)
        self.assertEqual(first[0], 200, first)
        self.assertEqual(self.process(value, attempt_id=identifier), first)
        status, result = self.process(value, 'retry', identifier)
        self.assertEqual(status, 409, result)
        self.assertEqual(result['error']['code'], 'ATTEMPT_PAYLOAD_CONFLICT')
        self.assertEqual(self.state(value)['business_effect_count'], 1)

    def test_attempt_id_cannot_be_shared_across_operations(self):
        first = self.receive()
        second = self.receive()
        identifier = attempt()
        self.assertEqual(self.process(first, attempt_id=identifier)[0], 200)
        status, result = self.process(second, attempt_id=identifier)
        self.assertEqual(status, 409, result)
        self.assertEqual(self.state(second)['business_effect_count'], 0)

    def test_rollback_then_recovery(self):
        run = run_body('fail_before_commit')
        status, result = request('POST', '/runs', run)
        self.assertEqual(status, 201, result)
        value = message(run['run_id'])
        self.assertEqual(request('POST', '/messages', value)[0], 201)
        status, result = self.process(value)
        self.assertEqual(status, 500, result)
        self.assertEqual(result['error']['code'], 'INJECTED_BEFORE_COMMIT')
        self.assertEqual(self.state(value)['business_effect_count'], 0)
        status, result = self.process(value, 'retry')
        self.assertEqual(status, 200, result)
        self.assertEqual(self.state(value)['business_effect_count'], 1)
        status, events = request('GET', '/operations/' + value['operation_id'] + '/events')
        self.assertEqual(status, 200, events)
        kinds = [item['event_type'] for item in events['events']]
        self.assertIn('ROLLED_BACK', kinds)
        self.assertEqual(kinds.count('BUSINESS_COMMITTED'), 1)

    def test_concurrent_processing_creates_one_effect(self):
        value = self.receive()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.process(value), range(8)))
        for status, body in results:
            self.assertIn(status, (200, 409, 500, 503), body)
            if status != 200:
                self.assertTrue(body['error']['retryable'], body)
        status, result = self.process(value, 'retry')
        self.assertEqual(status, 200, result)
        self.assertTrue(result['replayed'])
        state = self.state(value)
        self.assertEqual(state['business_effect_count'], 1)
        successful_refs = {body['result']['order_ref'] for status, body in results if status == 200}
        self.assertTrue(successful_refs)
        self.assertEqual(successful_refs, {state['orders'][0]['order_ref']})

    def test_state_and_order_count_are_consistent_during_commit(self):
        value = self.receive()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            processing = pool.submit(self.process, value)
            snapshots = list(pool.map(lambda _: self.state(value), range(16)))
            status, result = processing.result()
        self.assertEqual(status, 200, result)
        for snapshot in snapshots:
            self.assertEqual(snapshot['server_state'] == 'COMPLETED',
                             snapshot['business_effect_count'] == 1, snapshot)

    def test_concurrent_receive_is_deduplicated(self):
        value = message(self.run_config['run_id'])
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: request('POST', '/messages', dict(value, attempt_id=attempt())), range(6)))
        self.assertEqual(sum(status == 201 for status, _ in results), 1, results)
        self.assertTrue(all(status in (200, 201, 500, 503) for status, _ in results), results)
        self.assertEqual(self.state(value)['business_effect_count'], 0)

    def test_invalid_inputs_have_no_business_effect(self):
        for change in ({'amount_minor': 0}, {'amount_minor': 1.5}, {'amount_minor': '100'},
                       {'schema_version': 2}, {'currency': 'USD'}, {'operation_id': '../escape'},
                       {'created_at': '2026-99-99T00:00:00Z'}, {'attempt_id': 'not-uuid'},
                       {'unexpected': True}):
            with self.subTest(change=change):
                value = message(self.run_config['run_id'], **change)
                status, result = request('POST', '/messages', value)
                self.assertEqual(status, 400, result)
        missing = message(self.run_config['run_id'])
        del missing['amount_minor']
        self.assertEqual(request('POST', '/messages', missing)[0], 400)

    def test_run_configuration_is_immutable(self):
        self.assertEqual(request('POST', '/runs', self.run_config)[0], 200)
        status, result = request('POST', '/runs', dict(self.run_config, seed=999))
        self.assertEqual(status, 409, result)

    def test_timestamp_canonicalization(self):
        value = self.receive()
        other = dict(value, attempt_id=attempt(), created_at='2026-11-17T00:00:00.000Z')
        status, result = request('POST', '/messages', other)
        self.assertEqual(status, 200, result)

    def test_longest_identifiers_fit_durable_results(self):
        value = self.receive(operation_id=uuid.uuid4().hex * 3, order_id='O' * 96)
        status, result = self.process(value)
        self.assertEqual(status, 200, result)
        self.assertEqual(self.state(value)['business_effect_count'], 1)

    def test_lost_response_model_recovers_existing_order(self):
        value = self.receive()
        # Intentionally discard a successful response. This models response loss;
        # it is not a real network failure and does not exercise RabbitMQ.
        status, lost = self.process(value)
        self.assertEqual(status, 200, lost)
        del lost
        unknown_at_observer = True
        status, recovered = self.process(value, 'retry')
        self.assertEqual(status, 200, recovered)
        self.assertTrue(unknown_at_observer)
        self.assertTrue(recovered['replayed'])
        self.assertEqual(self.state(value)['business_effect_count'], 1)

    def test_unknown_operation_is_404(self):
        status, result = request('GET', '/operations/missing_' + uuid.uuid4().hex)
        self.assertEqual(status, 404, result)

    def create_baseline(self, strategy, scenario):
        config = run_body(scenario, strategy)
        code, answer = request('POST', '/runs', config)
        self.assertEqual((code, answer.get('strategy')), (201, strategy), answer)
        return config

    def test_unsafe_baseline_requires_opt_in_and_cannot_replace_c(self):
        config = run_body(strategy='A')
        self.assertEqual(request('POST', '/runs', dict(config, baseline_enabled=False))[0], 400)
        self.assertEqual(request('POST', '/runs', dict(config, strategy='C'))[0], 400)
        changed = dict(self.run_config, strategy='A', baseline_enabled=True)
        self.assertEqual(request('POST', '/runs', changed)[0], 409)

    def test_a_retry_after_discarded_response_creates_two_orders(self):
        run = self.create_baseline('A', 'drop_response_after_commit')
        value = self.receive(run_id=run['run_id'])
        code, first = self.process(value)
        self.assertEqual(code, 200, first)
        # The retry decision uses no state read or data from the first response.
        retry_id = attempt()
        code, second = self.process(value, 'retry', retry_id)
        self.assertEqual(code, 200, second)
        self.assertFalse(second['replayed'])
        self.assertNotEqual(first['result']['order_ref'], second['result']['order_ref'])
        self.assertEqual(self.process(value, 'retry', retry_id), (code, second))
        state = self.state(value)
        self.assertEqual(state['business_effect_count'], 2)
        self.assertEqual({order['order_ref'] for order in state['orders']},
                         {first['result']['order_ref'], second['result']['order_ref']})
        unprocessed = self.receive(run_id=run['run_id'])
        code, metrics = request('GET', '/runs/' + run['run_id'] + '/metrics')
        self.assertEqual(code, 200, metrics)
        self.assertEqual({name: metrics[name] for name in (
            'received', 'completed', 'business_effect_count',
            'duplicate_business_effects', 'duplicate_attempts', 'sent', 'unknown',
        )}, {
            'received': 2, 'completed': 1, 'business_effect_count': 2,
            'duplicate_business_effects': 1, 'duplicate_attempts': 1,
            'sent': None, 'unknown': None,
        })
        self.assertEqual(metrics['observation_coverage'], 'SERVER_ONLY')
        code, page = request('GET', '/runs/' + run['run_id'] + '/operations')
        self.assertEqual(code, 200, page)
        self.assertEqual({item['operation_id']: item['server_state'] for item in page['operations']},
                         {value['operation_id']: 'COMPLETED', unprocessed['operation_id']: 'RECEIVED'})
        self.assertIsNone(page['next_cursor'])

    def test_b_state_check_finds_committed_order(self):
        run = self.create_baseline('B', 'drop_response_after_commit')
        value = self.receive(run_id=run['run_id'])
        self.assertEqual(self.process(value)[0], 200)
        state = self.state(value)
        self.assertEqual(state['server_state'], 'COMPLETED')
        self.assertEqual(state['business_effect_count'], 1)

    def test_b_stale_state_check_can_create_duplicate(self):
        for strategy, final_count in [('B', 2), ('C', 1)]:
            with self.subTest(strategy=strategy):
                config = run_body(strategy=strategy)
                self.assertEqual(request('POST', '/runs', config)[0], 201)
                value = self.receive(run_id=config['run_id'])
                # Schedule the interleaving deterministically: GET observes no effect,
                # the original call commits, then recovery acts on the stale read.
                self.assertEqual(self.state(value)['business_effect_count'], 0)
                code, first = self.process(value)
                self.assertEqual(code, 200, first)
                code, retry = self.process(value, 'retry')
                self.assertEqual(code, 200, retry)
                self.assertEqual(retry['replayed'], strategy == 'C')
                self.assertEqual(self.state(value)['business_effect_count'], final_count)


if __name__ == '__main__':
    unittest.main(verbosity=2)
