"""Summarize finished comparable runs and render an evidence-based chart."""
import argparse
from collections import defaultdict
import csv
from hashlib import sha256
import json
import math
from pathlib import Path
from statistics import median, mean


def percentile(values, quantile):
    return sorted(values)[math.ceil(len(values) * quantile) - 1] if len(values) >= 100 else None


def load_series(root, scenario):
    runs = []
    inputs_by_seed = {}
    for path in sorted(root.glob('*/manifest.json')):
        manifest = json.loads(path.read_text())
        if manifest['config']['scenario'] != scenario:
            continue
        result_path = path.with_name('result.json')
        if not result_path.exists():
            raise ValueError(f'Incomplete comparable run: {path.parent.name}')
        config = manifest['config']
        inputs = [json.loads(line) for line in path.with_name('input.jsonl').read_text().splitlines()]
        if len(inputs) != config['expected_count']:
            raise ValueError(f'Input count does not match the manifest: {path.parent.name}')
        business_inputs = [{key: value for key, value in item.items() if key not in ('run_id', 'attempt_id')}
                           for item in inputs]
        checksum = sha256(json.dumps(business_inputs, sort_keys=True).encode()).hexdigest()
        if inputs_by_seed.setdefault(config['seed'], checksum) != checksum:
            raise ValueError(f'Business inputs differ for seed {config["seed"]}')
        runs.append((manifest, json.loads(result_path.read_text())))
    if not runs:
        raise ValueError('No finished runs for the requested scenario')
    comparable = set()
    for manifest, result in runs:
        config = manifest['config']
        if config != result['configuration']:
            raise ValueError(f'Manifest mismatch: {result["run_id"]}')
        comparable.add((config['expected_count'], config['fault_plan_id'], json.dumps(config['config'], sort_keys=True),
                        manifest['element_version'], manifest['generator_commit'], manifest['runner_commit'],
                        manifest['rabbitmq'], manifest['amount_unit']))
    if len(comparable) != 1:
        raise ValueError('Runs use different datasets sizes, timeouts, versions or amount units')
    seed_counts = {strategy: sorted(m['config']['seed'] for m, _ in runs if m['config']['strategy'] == strategy)
                   for strategy in 'ABC'}
    if not seed_counts['A'] or not seed_counts['A'] == seed_counts['B'] == seed_counts['C']:
        raise ValueError('Provide paired seeds for A, B and C')
    return runs


def records(runs):
    for manifest, result in runs:
        config, summary = manifest['config'], result['summary']
        values = {key: value for key, value in summary.items() if not isinstance(value, list)}
        for stem in ('client_processing', 'detection', 'recovery'):
            samples = summary[f'{stem}_ms']
            values[f'{stem}_samples'] = len(samples)
            values[f'{stem}_p50_ms'] = round(median(samples), 3) if samples else None
            values[f'{stem}_p95_ms'] = percentile(samples, 0.95)
        yield {'run_id': result['run_id'], 'strategy': config['strategy'], 'scenario': config['scenario'],
               'seed': config['seed'], **values}


def render(rows, scenario, path):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    groups = defaultdict(list)
    for row in rows:
        groups[row['strategy']].append(row)
    metrics = [('correctly_completed_operations', 'Exactly one order'),
               ('duplicate_business_effects', 'Extra orders'),
               ('unknown_final_operations', 'Final unknown outcomes')]
    fig, axes = plt.subplots(1, 3, figsize=(10, 3.6), layout='constrained', sharey=True)
    colors = ['#b45309', '#2563eb', '#047857']
    for ax, (field, title) in zip(axes, metrics):
        for index, strategy in enumerate('ABC'):
            values = [row[field] for row in groups[strategy]]
            ax.bar(index, mean(values), color=colors[index], alpha=.85, width=.6)
            ax.scatter([index + (i - (len(values) - 1) / 2) * .06 for i in range(len(values))],
                       values, color='#172033', s=15, zorder=3, clip_on=False)
        ax.set_xticks(range(3), ['A', 'B', 'C'])
        ax.set_title(title)
        ax.set_ylim(bottom=0)
        ax.grid(axis='y', alpha=.2)
    fig.suptitle(f"{scenario.replace('_', ' ').capitalize()} · {rows[0]['sent_operations']} operations per run")
    axes[0].set_ylabel('Count per run')
    fig.supxlabel('Bars show means. Dots show individual runs.', fontsize=9)
    fig.savefig(path, dpi=170)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('runs', type=Path)
    parser.add_argument('--scenario', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = list(records(load_series(args.runs, args.scenario)))
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / 'summary.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    render(rows, args.scenario, args.output / 'comparison.png')


if __name__ == '__main__':
    main()
