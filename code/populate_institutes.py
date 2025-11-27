#!/usr/bin/env python3
"""
Populate CORDEX-CMIP5-institutes.owx with institutions from institutions.csv

Usage:
  python3 code/populate_institutes.py \
      --csv code/institutions.csv \
      --owx CORDEX-CMIP5-institutes.owx

The script is idempotent: it will not add duplicate NamedIndividual entries.
It creates a timestamped backup of the OWX file before writing.
"""
import argparse
import csv
import datetime
import shutil
import sys
import xml.etree.ElementTree as ET
from xml.dom import minidom


NS_OWL = "http://www.w3.org/2002/07/owl#"
NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_XML = "http://www.w3.org/XML/1998/namespace"
NS_XSD = "http://www.w3.org/2001/XMLSchema#"
NS_RDFS = "http://www.w3.org/2000/01/rdf-schema#"


def parse_csv(path):
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader, None)
        for r in reader:
            if not r or len(r) < 2:
                continue
            name = r[0].strip()
            acronym = r[1].strip()
            if not acronym:
                continue
            rows.append((name, acronym))
    return rows


def prettify_xml(elem):
    rough = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough)
    return reparsed.toprettyxml(indent="   ", encoding='utf-8')


def already_has_individual(root, iri):
    # search for any NamedIndividual with attribute IRI == iri
    for ni in root.findall(f".//{{{NS_OWL}}}NamedIndividual"):
        if ni.get('IRI') == iri:
            return True
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', default='code/institutions.csv')
    parser.add_argument('--owx', default='CORDEX-CMIP5-institutes.owx')
    parser.add_argument('--backup', action='store_true', help='Create a timestamped backup (default True)')
    args = parser.parse_args()

    csv_path = args.csv
    owx_path = args.owx

    entries = parse_csv(csv_path)
    if not entries:
        print('No rows found in', csv_path)
        sys.exit(1)

    # register namespaces so output keeps prefixes/default namespace
    ET.register_namespace('', NS_OWL)
    ET.register_namespace('rdf', NS_RDF)
    ET.register_namespace('xml', NS_XML)
    ET.register_namespace('xsd', NS_XSD)
    ET.register_namespace('rdfs', NS_RDFS)

    tree = ET.parse(owx_path)
    root = tree.getroot()

    # backup
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    backup_path = f"{owx_path}.bak.{timestamp}"
    shutil.copy2(owx_path, backup_path)
    print(f'Backup created: {backup_path}')

    added = 0
    skipped = 0

    # For each institution, add Declaration, ClassAssertion and AnnotationAssertion
    for name, acro in entries:
        iri_ref = f"#{acro}"
        if already_has_individual(root, iri_ref):
            skipped += 1
            continue

        # Declaration
        decl = ET.SubElement(root, f"{{{NS_OWL}}}Declaration")
        ET.SubElement(decl, f"{{{NS_OWL}}}NamedIndividual", {'IRI': iri_ref})

        # ClassAssertion
        class_assertion = ET.SubElement(root, f"{{{NS_OWL}}}ClassAssertion")
        ET.SubElement(class_assertion, f"{{{NS_OWL}}}Class", {'IRI': 'https://metaclip.org/datasource/datasource.owl#ModellingCenter'})
        ET.SubElement(class_assertion, f"{{{NS_OWL}}}NamedIndividual", {'IRI': iri_ref})

        # AnnotationAssertion
        ann = ET.SubElement(root, f"{{{NS_OWL}}}AnnotationAssertion")
        ET.SubElement(ann, f"{{{NS_OWL}}}AnnotationProperty", {'IRI': 'http://purl.org/dc/elements/1.1/description'})
        iri_elem = ET.SubElement(ann, 'IRI')
        iri_elem.text = iri_ref
        lit = ET.SubElement(ann, 'Literal')
        lit.text = name

        added += 1

    # write back prettified
    pretty = prettify_xml(root)
    with open(owx_path, 'wb') as f:
        f.write(pretty)

    print(f'Added: {added}, Skipped (already present): {skipped}')
    print('Done. Edited file:', owx_path)


if __name__ == '__main__':
    main()
