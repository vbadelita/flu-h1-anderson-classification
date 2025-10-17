from Bio import Phylo

import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Convert Nexus tree to Newick format")
    parser.add_argument("nexus_file", type=str, help="Path to Nexus tree file")
    parser.add_argument("newick_file", type=str, help="Path to Newick tree file")
    return parser.parse_args()

args = parse_args()

tree = Phylo.read(args.nexus_file, "nexus")

Phylo.write(tree, args.newick_file, "newick", plain_newick=True)