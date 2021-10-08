# Calculating allele frequencies

Ok, so let's say we hypothetically wanted to compute allele frequencies across a growing set of genomic data samples, and... we wanted to do that recursively. You might think that's an over-complicated approach, and you're likely right. But, either way its easy to do in redun ;-P.

To run this example, first simulate 100 samples of genomic data with:

```sh
redun run workflow.py sim_samples
```

This will produce 100 coverage regions files in `data/regions` and 100 SNP mutation files in `data/muts`. These file formats are extremely simplified. We model only a single chromosome (and so skip recording its name) and only model SNP mutation locations along the chromosome.

We can then process some of the samples to compute allele frequencies using the following:

```sh
redun run workflow.py calc_allele_freq --nsamples 10
```

which will produce an allele frequency file in `computed/allele_freqs/0_9.allele_freqs`. For example, the file will contain four columns: chromosome position, number of alt alleles (AC), number of sequenced haplotypes (AN = 2 * number samples sequenced in that region), and the estimated frequency of the alt allele (AF = AC / AN).

```
58      1       2       0.5
84      1       4       0.25
93      1       4       0.25
185     1       4       0.25
834     1       2       0.5
```

You use increasing number of the samples for the calculation and see that the compute is incremental between each run:

```sh
redun run workflow.py calc_allele_freq --nsamples 12
redun run workflow.py calc_allele_freq --nsamples 20
redun run workflow.py calc_allele_freq --nsamples 50
redun run workflow.py calc_allele_freq --nsamples 100
```

