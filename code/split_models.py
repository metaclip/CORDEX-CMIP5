#!/usr/bin/env python3
"""
split_models.py

Reads `code/cordex_filtered.csv`, adds separate columns for institution and model name
for GCM and RCM using reasonable heuristics, and writes a new CSV `cordex_filtered_with_split.csv`.

Also creates a `dataset_id` column with format: DOMAIN-GCMmodel-ensemble-RCMmodel-experiment
example: africa-gfdl_esm2g-r0i0p0-remo2009-historical

Does not modify the original file: creates a backup and a new file.
"""
import csv
import os
import shutil
import re

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
IN_CSV = os.path.join(ROOT, 'code', 'cordex_filtered.csv')
OUT_CSV = os.path.join(ROOT, 'code', 'cordex_filtered_with_split.csv')
BAK_CSV = IN_CSV + '.bak'


def tokens(s):
    return s.split('_') if s and '_' in s else [s] if s else []


def choose_gcm_model_name(tokens_list):
    # Heuristic: if there are >=2 tokens, take the last two as model name (e.g. gfdl_esm2g, cm5a_lr)
    if not tokens_list:
        return '', ''
    if len(tokens_list) == 1:
        return '', tokens_list[0]
    model_tokens = tokens_list[-2:]
    model_name = '_'.join(model_tokens)
    inst = '_'.join(tokens_list[:-2]) if len(tokens_list) > 2 else tokens_list[0]
    return inst, model_name


def choose_rcm_model_name(tokens_list):
    # Heuristic for RCM: find the minimal suffix that contains some digit (remo2009, rca4, cclm5_0_15)
    if not tokens_list:
        return '', ''
    # try suffix lengths 1..min(4,len)
    for k in range(1, min(4, len(tokens_list)) + 1):
        candidate = '_'.join(tokens_list[-k:])
        if re.search(r'\d', candidate):
            inst = '_'.join(tokens_list[:-k]) if len(tokens_list) > k else tokens_list[0] if len(tokens_list)>1 else ''
            return inst, candidate
    # fallback: last two tokens
    if len(tokens_list) == 1:
        return '', tokens_list[0]
    inst = '_'.join(tokens_list[:-2]) if len(tokens_list) > 2 else tokens_list[0]
    return inst, '_'.join(tokens_list[-2:])


def clean_token(s):
    if s is None:
        return ''
    s = str(s).strip()
    s = s.replace(' ', '_')
    s = re.sub(r'[^A-Za-z0-9_\-]', '', s)
    return s.lower()


def make_dataset_id(domain, gcm_model_name, ensemble_member, rcm_model_name, experiment):
    parts = [domain, gcm_model_name, ensemble_member, rcm_model_name, experiment]
    parts = [clean_token(p) for p in parts if p is not None]
    # join with hyphens to match existing OWX example
    return '-'.join(parts)


def process():
    if not os.path.exists(IN_CSV):
        print(f"Not found: {IN_CSV}")
        return

    # create backup if not exists
    if not os.path.exists(BAK_CSV):
        print('Creating backup copy:', BAK_CSV)
        shutil.copy2(IN_CSV, BAK_CSV)

    with open(IN_CSV, 'r', newline='', encoding='utf-8') as inf:
        reader = csv.DictReader(inf)
        fieldnames = list(reader.fieldnames) if reader.fieldnames else []

        # new columns
        extras = ['gcm_institution', 'gcm_model_name', 'rcm_institution', 'rcm_model_name', 'dataset_id']
        out_fieldnames = fieldnames + [c for c in extras if c not in fieldnames]

        with open(OUT_CSV, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.DictWriter(outf, fieldnames=out_fieldnames)
            writer.writeheader()
            count = 0
            unique_ids = set()
            for row in reader:
                gcm = row.get('gcm_model', '') or ''
                rcm = row.get('rcm_model', '') or ''
                domain = row.get('domain', '') or ''
                ensemble = row.get('ensemble_member', '') or ''
                experiment = row.get('experiment', '') or ''

                g_tokens = tokens(gcm)
                r_tokens = tokens(rcm)

                g_inst, g_model = choose_gcm_model_name(g_tokens)
                r_inst, r_model = choose_rcm_model_name(r_tokens)

                row['gcm_institution'] = g_inst
                row['gcm_model_name'] = g_model
                row['rcm_institution'] = r_inst
                row['rcm_model_name'] = r_model

                dataset_id = make_dataset_id(domain, g_model, ensemble, r_model, experiment)
                row['dataset_id'] = dataset_id
                unique_ids.add(dataset_id)

                writer.writerow(row)
                count += 1

    print(f'Processed {count} rows. File written: {OUT_CSV}')
    print(f'Unique IDs found: {len(unique_ids)} (showing 10):', list(unique_ids)[:10])


if __name__ == '__main__':
    process()
