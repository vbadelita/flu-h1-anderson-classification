# flu-h1-anderson-classification
Simple data repo for https://doi.org/10.1128/msphere.00275-16 paper


## Layout

This repository is organized as follows:

- `raw_data/` contains the original files obtained from the paper.
    - Link: https://pmc.ncbi.nlm.nih.gov/articles/PMC5156671/#_ad93_
- `data/` contains the processed data files.
- `scripts/` contains different python scripts used to process the data.

## Requirements

All the output files are provided under the `data/`.
If you need to re-run any of the scripts, I have used the following software:
- `csvkit` for general csv file manipulation (https://csvkit.readthedocs.io/en/latest/)
- `seqkit` for general fasta file manipulation (https://bioinf.shenwei.me/seqkit/)
- `pixi` for python package management with both pypi and conda support (https://pixi.sh/latest/)
    - You may need to rewrite the pixi.toml file to work with conda or some other package manager.
- `mafft` for sequence alignment (https://mafft.cbrc.jp/alignment/server/index.html)
- `taxonium` for tree visualisation (https://taxonium.org/)
- See `pixi.toml` for a complete list of python dependencies


## Licence and attribution

See `LICENSE` for details.
All the software in this repo is provided under the *MIT Licence*, where applicable.
The data used is not mine and is shared by the original paper under *Creative Commons Attribution 4.0 International*
license. The data is obtained from the Anderson et. al. paper material and the https://www.bv-brc.org/
public database, and only processed by me.