#!/usr/bin/env python3
"""
Extract sequences from BV-BRC JSONL data and convert to FASTA format.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract sequences from BV-BRC JSONL data to FASTA format"
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        default="downloads/raw_data.jsonl",
        help="Input JSONL file (default: downloads/raw_data.jsonl)"
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        default="sequences.fasta",
        help="Output FASTA file (default: sequences.fasta)"
    )
    return parser.parse_args()




def extract_sequence_data(record: dict) -> Optional[tuple[str, str, str]]:
    """
    Extract accession, sequence, and description from a JSONL record.
    Returns (accession, sequence, description) or None if invalid.
    """
    try:
        # Get the outer accession
        accession = record.get("accession")
        if not accession:
            return None
        
        # Get the data array
        data = record.get("data", [])
        if not data or not isinstance(data, list):
            return None
        
        # Get the first (and typically only) sequence record
        seq_record = data[0]
        if not isinstance(seq_record, dict):
            return None
        
        # Extract sequence
        sequence = seq_record.get("sequence", "")
        if not sequence or not sequence.strip():
            return None
        
        # Extract description (optional)
        description = seq_record.get("description", "")
        
        return accession, sequence.strip(), description
        
    except (KeyError, IndexError, TypeError):
        return None


def main():
    args = parse_args()
    
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    if not input_path.exists():
        print(f"Error: Input file {input_path} does not exist", file=sys.stderr)
        sys.exit(1)
    
    
    processed = 0
    skipped = 0
    sequences = []
    
    # First pass: collect all valid sequences
    with open(input_path, "r", encoding="utf-8") as infile:
        
        for line_num, line in enumerate(infile, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Line {line_num}: JSON decode error - {e}\n")
                skipped += 1
                continue
            
            # Extract sequence data
            result = extract_sequence_data(record)
            if result is None:
                accession = record.get("accession", "unknown")
                print(f"Line {line_num}: Skipped {accession} - missing accession, sequence, or invalid data structure\n")
                skipped += 1
                continue
            
            accession, sequence, description = result
            
            # Create SeqRecord
            seq_record = SeqRecord(
                Seq(sequence),
                id=accession,
                description=description
            )
            sequences.append(seq_record)
            processed += 1
            
            if processed % 1000 == 0:
                print(f"Processed {processed} sequences...")
    
    # Second pass: write all sequences to FASTA using Biopython
    print(f"Writing {len(sequences)} sequences to FASTA...")
    SeqIO.write(sequences, output_path, "fasta")
    
    print("\nExtraction completed!")
    print(f"Successfully processed: {processed} sequences")
    print(f"Skipped: {skipped} sequences")
    print(f"Output written to: {output_path}")


if __name__ == "__main__":
    main()
