#!/usr/bin/env python3
"""
Process Newick tree to simplify node names and add descriptions as annotations.

This script reads a Newick tree file where node names contain both accession numbers
and descriptions separated by '|', and transforms them to use only accession numbers
as names while moving descriptions to node annotations.
"""

from Bio import Phylo
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Process Newick tree file")
    parser.add_argument("input_file", type=str, help="Path to input Newick tree file")
    parser.add_argument("output_file", type=str, help="Path to output Newick tree file")
    return parser.parse_args()


def process_tree_node(node):
    """Process a single tree node to split name and add description annotation."""
    if node.name and "|" in node.name:
        # Remove triple quotes if present
        clean_name = node.name.strip("'")
        
        # Split by first '|' to separate accession from description
        parts = clean_name.split("|", 5)
        if len(parts) == 5:
            accession, isolate_name, subtype, host , country = parts
            label = f'name={isolate_name},subtype={subtype},host={host},country={country}'
        elif len(parts) == 4:
            accession, isolate_name, subtype, host = parts
            label = f'name={isolate_name},subtype={subtype},host={host}'
        else:
            print("invalid name, skipping", clean_name)
            
        # Update node name to just the accession
        node.name = accession
        
        # Add description to node comment
        label = f'name={isolate_name},subtype={subtype},host={host}'
        if hasattr(node, "comment") and node.comment:
            # If there's already a comment, append to it
            comment = node.comment.replace("\\[", "").replace("\\]", "")
            node.comment = f'{comment},{label}'
        else:
            node.comment = f'&{label}'
    
    # Recursively process child nodes
    for child in node.clades:
        process_tree_node(child)


def fix_bracket_escaping(tree_string):
    """Fix the double-escaped brackets in the tree string."""
    # Replace double-escaped brackets with properly formatted ones
    # First remove the escaped double brackets produced by BioPython, then add a comma at the end
    # so taxonium doesn't add the bracket to the last label.
    tree_string = tree_string.replace("\\[", "").replace("\\]", "").replace("]", ",]")
    return tree_string


def main():
    args = parse_args()
    input_file = args.input_file
    output_file = args.output_file
    
    print(f"Reading tree from {input_file}...")
    tree = Phylo.read(input_file, "newick")
    
    # Process all nodes in the tree
    print("Processing node names...")
    process_tree_node(tree.root)
    
    # Write the modified tree
    print(f"Writing processed tree to {output_file}...")
    Phylo.write(tree, output_file, "newick")
    
    # Fix the bracket escaping issue
    print("Fixing bracket formatting...")
    with open(output_file, "r") as f:
        tree_content = f.read()
    
    fixed_content = fix_bracket_escaping(tree_content)
    
    with open(output_file, "w") as f:
        f.write(fixed_content)
    


if __name__ == "__main__":
    main()
