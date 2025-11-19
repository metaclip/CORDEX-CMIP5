#!/usr/bin/env python3
"""
cordex_vocabulary_generator.py

Consolidated script for generating normalized CORDEX-CMIP5 vocabularies.

This script integrates all necessary functionalities to:
1. Normalize domain, GCM and RCM names according to CORDEX conventions
2. Generate correct dataset_ids
3. Insert entries into OWX files
4. Create automatic backups

Usage:
  python3 cordex_vocabulary_generator.py [--normalize-only] [--insert-only] [--test]

Options:
  --normalize-only : Only execute name normalization
  --insert-only    : Only insert OWX entries (requires normalized CSV)
  --test          : Execute in test mode (first 10 rows)
  --help          : Show this help

Input files:
  - code/cordex_filtered_with_split.csv (main CSV)
  - code/CORDEX_RCMs_ToU.txt (RCM mapping)
  - code/GCMModelName.txt (GCM mapping)

Generated files:
  - code/cordex_filtered_normalized.csv (normalized CSV)
  - CORDEX-CMIP5-datasets.owx (updated with new entries)
  - CORDEX-CMIP5-datasets.owx.bak (backup copy)
"""

import csv
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
import argparse

# =============================================================================
# CONFIGURATION
# =============================================================================

ROOT = Path(__file__).resolve().parents[1]
CODE_DIR = ROOT / 'code'
IN_CSV = CODE_DIR / 'cordex_filtered_with_split.csv'
ALT_CSV = CODE_DIR / 'cordex_filtered.csv'
NORMALIZED_CSV = CODE_DIR / 'cordex_filtered_normalized.csv'
OWX = ROOT / 'CORDEX-CMIP5-datasets.owx'
BAK_OWX = OWX.with_suffix('.owx.bak')
RCMS_FILE = CODE_DIR / 'CORDEX_RCMs_ToU.txt'
GCMS_FILE = CODE_DIR / 'GCMModelName.txt'

# Domain mapping to standard CORDEX codes (base names without resolution)
DOMAIN_BASE_MAPPING = {
    'africa': 'AFR',
    'antarctic': 'ANT', 
    'arctic': 'ARC',
    'australasia': 'AUS',
    'central_america': 'CAM',
    'central_asia': 'CAS',
    'east_asia': 'EAS',
    'europe': 'EUR',
    'mediterranean': 'MED',
    'middle_east_and_north_africa': 'MNA',
    'north_america': 'NAM',
    'south_america': 'SAM',
    'south_asia': 'WAS',
    'south_east_asia': 'SEA'
}

# =============================================================================
# MAPPING LOADING FUNCTIONS
# =============================================================================

def load_rcm_mapping():
    """Loads RCM mapping from CORDEX_RCMs_ToU.txt"""
    rcm_mapping = {}
    if not RCMS_FILE.exists():
        print(f"Warning: {RCMS_FILE} not found")
        return rcm_mapping
        
    print(f"Loading RCMs from {RCMS_FILE}...")
    with open(RCMS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#') or not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                rcm_id = parts[0]
                # Create flexible mappings
                key = rcm_id.lower().replace('-', '_')
                rcm_mapping[key] = rcm_id
                
                # Additional mappings for common variants
                if '_' in key:
                    base_key = key.split('_')[0]
                    if base_key not in rcm_mapping:
                        rcm_mapping[base_key] = rcm_id
                
    print(f"  Loaded {len(rcm_mapping)} RCM mappings")
    return rcm_mapping

def load_gcm_mapping():
    """Loads GCM mapping from GCMModelName.txt"""
    gcm_mapping = {}
    if not GCMS_FILE.exists():
        print(f"Warning: {GCMS_FILE} not found")
        return gcm_mapping
        
    print(f"Loading GCMs from {GCMS_FILE}...")
    with open(GCMS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#') or not line:
                continue
            parts = line.split(None, 1)  # Only split on first space
            if len(parts) >= 1:
                gcm_id = parts[0]
                key = gcm_id.lower().replace('-', '_')
                gcm_mapping[key] = gcm_id
                
                # Additional mapping for model name without institution
                if '-' in gcm_id:
                    model_part = gcm_id.split('-', 1)[1]
                    model_key = model_part.lower().replace('-', '_')
                    if model_key not in gcm_mapping:
                        gcm_mapping[model_key] = gcm_id
                        
    print(f"  Loaded {len(gcm_mapping)} GCM mappings")
    return gcm_mapping

# =============================================================================
# NORMALIZATION FUNCTIONS
# =============================================================================

def extract_resolution_from_horizontal_resolution(horizontal_resolution):
    """
    Extracts resolution number from horizontal_resolution field
    
    Examples:
    - '0_44_degree_x_0_44_degree' -> '44'
    - '0_22_degree_x_0_22_degree' -> '22'
    - '0_11_degree_x_0_11_degree' -> '11'
    - 'interpolated_0_44_degree_x_0_44_degree' -> '44'
    """
    import re
    if not horizontal_resolution:
        return '44'  # default fallback
    
    # Extract first number pattern after '0_' 
    match = re.search(r'0_(\d+)_degree', str(horizontal_resolution))
    if match:
        return match.group(1)
    
    return '44'  # default fallback

def normalize_domain(domain, horizontal_resolution=None):
    """
    Normalizes domain name to standard CORDEX code with resolution
    
    Args:
        domain: Domain name (e.g., 'africa')
        horizontal_resolution: Resolution string (e.g., '0_44_degree_x_0_44_degree')
    
    Returns:
        CORDEX domain code (e.g., 'AFR-44')
    """
    domain_lower = domain.lower().strip()
    base_code = DOMAIN_BASE_MAPPING.get(domain_lower)
    
    if not base_code:
        return domain  # return original if not found
    
    # Extract resolution from horizontal_resolution field
    resolution = extract_resolution_from_horizontal_resolution(horizontal_resolution)
    
    return f"{base_code}-{resolution}"

def normalize_gcm(gcm_model, gcm_mapping):
    """Normalizes GCM name using reference mapping"""
    if not gcm_model:
        return gcm_model
        
    gcm_key = gcm_model.lower().replace('-', '_').strip()
    
    # Search for exact matches first
    if gcm_key in gcm_mapping:
        return gcm_mapping[gcm_key]
    
    # Search for partial matches
    for key, value in gcm_mapping.items():
        if gcm_key in key or key in gcm_key:
            return value
    
    return gcm_model

def normalize_rcm(rcm_model, rcm_mapping):
    """Normalizes RCM name using reference mapping"""
    if not rcm_model:
        return rcm_model
        
    rcm_key = rcm_model.lower().replace('-', '_').strip()
    
    # Search for exact matches first
    if rcm_key in rcm_mapping:
        return rcm_mapping[rcm_key]
    
    # Search for partial matches
    for key, value in rcm_mapping.items():
        if rcm_key in key or key in rcm_key:
            return value
            
    return rcm_model

def clean_token(s):
    """Cleans a token for use in dataset_id"""
    if not s:
        return ''
    s = str(s).strip().replace(' ', '_')
    s = re.sub(r'[^A-Za-z0-9_\-]', '', s)
    return s.lower()

def last_tokens(s, n=2):
    """Extracts the last n tokens from a string separated by '_'"""
    parts = s.split('_') if s else []
    if not parts:
        return ''
    return '_'.join(parts[-n:])

def regenerate_dataset_id(row):
    """Regenerates dataset_id with normalized names"""
    domain = row['domain']
    gcm = row['gcm_model_name'] 
    ensemble = row['ensemble_member']
    rcm = row['rcm_model_name']
    experiment = row['experiment']
    
    # Clean and extract relevant parts
    domain_clean = clean_token(domain)
    gcm_name = last_tokens(clean_token(gcm), 2)
    ensemble_clean = clean_token(ensemble)
    rcm_name = last_tokens(clean_token(rcm), 1)
    exp_clean = clean_token(experiment)
    
    parts = [p for p in [domain_clean, gcm_name, ensemble_clean, rcm_name, exp_clean] if p]
    return '-'.join(parts)

# =============================================================================
# MAIN NORMALIZATION FUNCTION
# =============================================================================

def normalize_csv(test_mode=False):
    """
    Normalizes the main CSV using CORDEX conventions
    
    Args:
        test_mode (bool): If True, only processes first 10 rows
    
    Returns:
        dict: Process statistics
    """
    if not IN_CSV.exists():
        print(f"Error: {IN_CSV} not found")
        return None
    
    print("=== CORDEX NAME NORMALIZATION ===")
    print("Loading reference mappings...")
    rcm_mapping = load_rcm_mapping()
    gcm_mapping = load_gcm_mapping()
    
    # Statistics counters
    stats = {
        'total_rows': 0,
        'domains_normalized': 0,
        'gcms_normalized': 0,
        'rcms_normalized': 0
    }
    
    output_file = NORMALIZED_CSV if not test_mode else CODE_DIR / 'test_normalized.csv'
    max_rows = 10 if test_mode else None
    
    print(f"Processing {IN_CSV} -> {output_file}")
    if test_mode:
        print("TEST MODE: Only processing first 10 rows")
    
    try:
        with open(IN_CSV, 'r', newline='', encoding='utf-8') as infile, \
             open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in reader:
                if max_rows and stats['total_rows'] >= max_rows:
                    break
                    
                stats['total_rows'] += 1
                
                # Show progress every 10000 rows (if not test mode)
                if not test_mode and stats['total_rows'] % 10000 == 0:
                    print(f"Processed {stats['total_rows']:,} rows...")
                
                # Normalize domain
                original_domain = row['domain']
                row['domain'] = normalize_domain(original_domain, row.get('horizontal_resolution'))
                if row['domain'] != original_domain:
                    stats['domains_normalized'] += 1
                    if test_mode:
                        print(f"  Domain: {original_domain} -> {row['domain']}")
                
                # Normalize GCM
                original_gcm = row['gcm_model_name']
                row['gcm_model_name'] = normalize_gcm(original_gcm, gcm_mapping)
                if row['gcm_model_name'] != original_gcm:
                    stats['gcms_normalized'] += 1
                    if test_mode:
                        print(f"  GCM: {original_gcm} -> {row['gcm_model_name']}")
                
                # Normalize RCM  
                original_rcm = row['rcm_model_name']
                row['rcm_model_name'] = normalize_rcm(original_rcm, rcm_mapping)
                if row['rcm_model_name'] != original_rcm:
                    stats['rcms_normalized'] += 1
                    if test_mode:
                        print(f"  RCM: {original_rcm} -> {row['rcm_model_name']}")
                        
                # Regenerate dataset_id with normalized names
                old_id = row['dataset_id']
                row['dataset_id'] = regenerate_dataset_id(row)
                if test_mode:
                    print(f"  ID: {old_id} -> {row['dataset_id']}")
                
                writer.writerow(row)
        
        print(f"\nProcess completed. File generated: {output_file}")
        print(f"Statistics:")
        print(f"  - Total rows processed: {stats['total_rows']:,}")
        print(f"  - Domains normalized: {stats['domains_normalized']:,}")
        print(f"  - GCMs normalized: {stats['gcms_normalized']:,}")
        print(f"  - RCMs normalized: {stats['rcms_normalized']:,}")
        
        return stats
        
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return None

# =============================================================================
# FUNCTIONS FOR GENERATING OWX ENTRIES
# =============================================================================

def ensure_csv():
    """Ensures an input CSV exists, prioritizing the normalized one"""
    if NORMALIZED_CSV.exists():
        print(f"Using normalized CSV: {NORMALIZED_CSV}")
        return NORMALIZED_CSV
    elif IN_CSV.exists():
        print(f"Using original CSV: {IN_CSV}")
        return IN_CSV
    elif ALT_CSV.exists():
        print(f"Using alternative CSV: {ALT_CSV}")
        return ALT_CSV
    else:
        raise FileNotFoundError('No input CSV found.')

def collect_dataset_ids(csv_path):
    """Extracts unique dataset_ids from CSV"""
    ids = set()
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if 'dataset_id' in (reader.fieldnames or []):
            for r in reader:
                v = (r.get('dataset_id') or '').strip()
                if v:
                    ids.add(v)
            return ids

    # If dataset_id is not present, calculate on the fly
    print('dataset_id not present; calculating from existing columns...')
    
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            domain = r.get('domain','')
            g = r.get('gcm_model','')
            rcm = r.get('rcm_model','')
            ens = r.get('ensemble_member','')
            exp = r.get('experiment','')
            
            # Use the same ID generation logic
            row_mock = {
                'domain': domain,
                'gcm_model_name': g,
                'ensemble_member': ens,
                'rcm_model_name': rcm,
                'experiment': exp
            }
            did = regenerate_dataset_id(row_mock)
            if did:
                ids.add(did)
    return ids

def read_existing_ids(owx_path):
    """Reads existing IDs from OWX file"""
    text = owx_path.read_text(encoding='utf-8')
    # Find all IRI="#id" occurrences
    found = set(re.findall(r'IRI="#([^"]+)"', text))
    return found, text

def build_snippet(did):
    """Builds XML snippet for a dataset_id"""
    return (
        '    <Declaration>\n'
        f'        <NamedIndividual IRI="#{did}"/>\n'
        '    </Declaration>\n'
        '    <ClassAssertion>\n'
        '        <Class IRI="https://metaclip.org/datasource/datasource.owl#Dataset"/>\n'
        f'        <NamedIndividual IRI="#{did}"/>\n'
        '    </ClassAssertion>\n'
    )

def insert_entries(dids, owx_path):
    """Inserts new entries into OWX file"""
    existing, text = read_existing_ids(owx_path)
    to_add = [d for d in sorted(dids) if d not in existing]
    
    print(f'Total IDs in CSV: {len(dids)}')
    print(f'IDs already present in OWX: {len(existing)}')
    print(f'New ones to insert: {len(to_add)}')
    
    if not to_add:
        print('Nothing to insert.')
        return 0

    # Create backup
    if not BAK_OWX.exists():
        print(f'Creating OWX backup at {BAK_OWX}')
        shutil.copy2(owx_path, BAK_OWX)
    else:
        print(f'Backup already exists: {BAK_OWX}')

    insert_text = '\n'.join(build_snippet(d) for d in to_add)

    # Insert before closing </Ontology> tag
    if '</Ontology>' in text:
        new_text = text.replace('</Ontology>', insert_text + '\n</Ontology>')
    else:
        # fallback: append to end
        new_text = text + '\n' + insert_text

    owx_path.write_text(new_text, encoding='utf-8')
    print(f'Written {len(to_add)} entries to {owx_path}')
    return len(to_add)

def generate_owx_entries():
    """Generates and inserts OWX entries from CSV"""
    print("=== OWX ENTRY GENERATION ===")
    
    csvp = ensure_csv()
    ids = collect_dataset_ids(csvp)
    
    if not OWX.exists():
        raise FileNotFoundError(f'Could not find {OWX}')
    
    inserted = insert_entries(ids, OWX)
    print(f'Done. Inserted: {inserted}')
    return inserted

# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    """Main function with command line options"""
    parser = argparse.ArgumentParser(
        description='CORDEX-CMIP5 normalized vocabulary generator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage examples:
  python3 cordex_vocabulary_generator.py                 # Complete process
  python3 cordex_vocabulary_generator.py --normalize-only # Only normalize
  python3 cordex_vocabulary_generator.py --insert-only    # Only insert OWX
  python3 cordex_vocabulary_generator.py --test           # Test mode
        """
    )
    
    parser.add_argument('--normalize-only', action='store_true',
                        help='Only execute name normalization')
    parser.add_argument('--insert-only', action='store_true',
                        help='Only insert OWX entries (requires normalized CSV)')
    parser.add_argument('--test', action='store_true',
                        help='Execute in test mode (first 10 rows)')
    
    args = parser.parse_args()
    
    try:
        if args.test:
            print("=== TEST MODE ===")
            stats = normalize_csv(test_mode=True)
            return
            
        if args.normalize_only or (not args.insert_only):
            print("Executing normalization...")
            stats = normalize_csv()
            if not stats:
                print("Error in normalization. Aborting.")
                return
        
        if args.insert_only or (not args.normalize_only):
            print("\nExecuting OWX entry insertion...")
            inserted = generate_owx_entries()
        
        print("\n=== PROCESS COMPLETED ===")
        print("Generated files:")
        if NORMALIZED_CSV.exists():
            print(f"  - Normalized CSV: {NORMALIZED_CSV}")
        if BAK_OWX.exists():
            print(f"  - Backup copy: {BAK_OWX}")
        print(f"  - Updated OWX: {OWX}")
        
    except Exception as e:
        print(f"Error in process: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
