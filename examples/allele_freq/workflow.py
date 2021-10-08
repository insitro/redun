import random
from itertools import groupby
from math import log
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, cast

from redun import File, task
from redun.functools import seq

redun_namespace = "redun.examples.allele_freq"


Region = Tuple[int, int, int]
Mutation = Tuple[int, int]
AlleleFreq = Tuple[int, int, int, float]


def sample_exponential(lambd: float) -> float:
    """
    Sample from exponential distribution with rate lambda.
    """
    return -log(random.random()) / lambd


def write_regions(out_path: str, regions: Iterable[Region]) -> File:
    file = File(out_path)
    with file.open("w") as out:
        for start, end, depth in regions:
            out.write(f"{start}\t{end}\t{depth}\n")
    return file


def read_regions(file: File) -> Iterator[Region]:
    with file.open() as infile:
        for line in infile:
            yield cast(Region, tuple(map(int, line.rstrip().split("\t"))))


def write_mutations(out_path: str, muts: Iterable[Mutation]) -> File:
    file = File(out_path)
    with file.open("w") as out:
        for mut, count in muts:
            out.write(f"{mut}\t{count}\n")
    return file


def read_mutations(file: File) -> Iterator[Mutation]:
    with file.open() as infile:
        for line in infile:
            yield cast(Mutation, tuple(map(int, line.rstrip().split("\t"))))


def write_allele_freqs(out_path: str, allele_freqs: Iterable[AlleleFreq]) -> File:
    file = File(out_path)
    with file.open("w") as out:
        for pos, num, denom, freq in allele_freqs:
            out.write(f"{pos}\t{num}\t{denom}\t{freq}\n")
    return file


@task()
def sim_sample(
    out_prefix: str,
    sample_id: int,
    chrom_start: int = 0,
    chrom_end: int = 10000,
    start_rate: float = 0.001,
    end_rate: float = 0.01,
    mut_rate: float = 0.01,
) -> Dict[str, File]:
    """
    Simulate sequencing data for one sample (assume one chromosome).

    regions are sequenced intervals of a chromsome.
    muts are SNP locations, assume heterozygous.
    """
    regions = []
    muts = []
    region_start: Optional[int]

    # Sample initial state.
    non_seq_len = 1 / start_rate
    seq_len = 1 / end_rate
    if random.random() < seq_len / (seq_len + non_seq_len):
        region_start = chrom_start
    else:
        region_start = None

    # Use poisson process to sample regions and mutation sites.
    pos = chrom_start
    while pos < chrom_end:
        pos += 1
        if region_start is None:
            pos += int(sample_exponential(start_rate))
            if pos >= chrom_end:
                break
            region_start = pos
        else:
            region_end = min(pos + int(sample_exponential(end_rate)), chrom_end - 1)
            mut_pos = pos + int(sample_exponential(mut_rate))
            if region_end <= mut_pos:
                regions.append((region_start, region_end, 2))
                region_start = None
                pos = region_end
            else:
                pos = mut_pos
                muts.append((mut_pos, 1))

    return {
        "regions": write_regions(f"{out_prefix}/regions/{sample_id}.regions", regions),
        "mutations": write_mutations(f"{out_prefix}/muts/{sample_id}.muts", muts),
    }


@task(check_valid="shallow")
def sim_samples(
    out_path: str = "data",
    nsamples: int = 100,
    chrom_start: int = 0,
    chrom_end: int = 10000,
    start_rate: float = 0.001,
    end_rate: float = 0.01,
    mut_rate: float = 0.01,
):
    """
    Simulate multiple samples.
    """
    return [
        sim_sample(
            out_path,
            sample_id,
            chrom_start=chrom_start,
            chrom_end=chrom_end,
            start_rate=start_rate,
            end_rate=end_rate,
            mut_rate=mut_rate,
        )
        for sample_id in range(nsamples)
    ]


def iter_merge(iter1: Iterator, iter2: Iterator) -> Iterator:
    """
    Merge two sorted iterables.
    """
    cur1 = next(iter1, None)
    cur2 = next(iter2, None)

    while cur1 is not None or cur2 is not None:
        if cur1 is None:
            yield cur2
            cur2 = next(iter2, None)
        elif cur2 is None:
            yield cur1
            cur1 = next(iter1, None)
        elif cur1 <= cur2:
            yield cur1
            cur1 = next(iter1, None)
        else:
            yield cur2
            cur2 = next(iter2, None)


def find_midpoint(n: int):
    """
    Returns the biggest power of 2 less than n.
    """
    k = 1
    m = n / 2
    while k < m:
        k *= 2
    return k


@task()
def merge_regions(
    out_path: str, sample1_id: int, regions1_file: File, sample2_id: int, regions2_file: File
) -> File:
    """
    Merge two sorted region files into one.
    """

    def iter_points(regions):
        for start, end, depth in regions:
            yield (start, "start", depth)
            yield (end, "end", -depth)

    def iter_regions(points):
        first_point = next(points, None)
        if first_point is None:
            return
        start, _, depth = first_point

        for pos, kind, delta in points:
            if pos > start:
                yield (start, pos, depth)
                start = pos
            depth += delta

    regions1 = read_regions(regions1_file)
    regions2 = read_regions(regions2_file)
    points1 = iter_points(regions1)
    points2 = iter_points(regions2)
    points = iter_merge(points1, points2)
    regions = iter_regions(points)

    region_path = f"{out_path}/regions/{sample1_id}_{sample2_id}.regions"
    return write_regions(region_path, regions)


@task(check_valid="shallow")
def merge_all_regions(out_path: str, id_regions: List[Tuple[int, File]]) -> Tuple[int, int, File]:
    """
    Recursively merge a list of region files.
    """
    if len(id_regions) == 1:
        # Base case 1.
        [(sample_id, region_file)] = id_regions
        return (sample_id, sample_id, region_file)
    elif len(id_regions) == 2:
        # Base case 2.
        [(sample1_id, region1_file), (sample2_id, region2_file)] = id_regions
    else:
        # Recursive case.
        k = find_midpoint(len(id_regions))
        sample1_id, _, region1_file = merge_all_regions(out_path, id_regions[:k])
        _, sample2_id, region2_file = merge_all_regions(out_path, id_regions[k:])
    return (
        sample1_id,
        sample2_id,
        merge_regions(out_path, sample1_id, region1_file, sample2_id, region2_file),
    )


@task()
def merge_mutations(
    out_path: str, sample1_id: int, mutations1_file: File, sample2_id: int, mutations2_file: File
) -> File:
    """
    Merge two sorted mutation files.
    """
    muts1 = read_mutations(mutations1_file)
    muts2 = read_mutations(mutations2_file)
    muts = iter_merge(muts1, muts2)
    groups = groupby(muts, key=lambda mut: mut[0])
    muts2 = ((pos, sum(count for _, count in group)) for pos, group in groups)
    mut_path = f"{out_path}/muts/{sample1_id}_{sample2_id}.muts"
    return write_mutations(mut_path, muts2)


@task(check_valid="shallow")
def merge_all_mutations(
    out_path: str, id_mutations: List[Tuple[int, File]]
) -> Tuple[int, int, File]:
    """
    Recursively merge a list of mutation files.
    """
    if len(id_mutations) == 1:
        # Base case 1.
        [(sample_id, mutations_file)] = id_mutations
        return (sample_id, sample_id, mutations_file)
    elif len(id_mutations) == 2:
        # Base case 2.
        [(sample1_id, mutations1_file), (sample2_id, mutations2_file)] = id_mutations
    else:
        # Recursive case.
        k = find_midpoint(len(id_mutations))
        sample1_id, _, mutations1_file = merge_all_mutations(out_path, id_mutations[:k])
        _, sample2_id, mutations2_file = merge_all_mutations(out_path, id_mutations[k:])
    return (
        sample1_id,
        sample2_id,
        merge_mutations(out_path, sample1_id, mutations1_file, sample2_id, mutations2_file),
    )


@task()
def join_mutations_regions(
    out_path: str, sample1_id: int, sample2_id: int, mutations_file: File, regions_file: File
) -> File:
    """
    Join mutations and regions together to compute an allele frequence.
    """

    def iter_mut_points(muts):
        for pos, count in muts:
            yield pos, "mut", count

    def iter_region_points(regions):
        for start, end, depth in regions:
            yield start - 0.5, "region", depth

    def iter_allele_freqs(points):
        denom = 0
        for pos, kind, count in points:
            if kind == "region":
                denom = count
            elif kind == "mut":
                yield pos, count, denom, count / denom

    points1 = iter_mut_points(read_mutations(mutations_file))
    points2 = iter_region_points(read_regions(regions_file))
    points = iter_merge(points1, points2)
    allele_freqs = iter_allele_freqs(points)
    allele_freqs_path = f"{out_path}/allele_freqs/{sample1_id}_{sample2_id}.allele_freqs"
    return write_allele_freqs(allele_freqs_path, allele_freqs)


@task()
def calc_allele_freq(
    nsamples: int = 100, data_path: str = "data", out_path: str = "computed"
) -> File:
    """
    Calculate allele frequencies across `nsamples` samples.
    """
    id_regions = [(i, File(f"{data_path}/regions/{i}.regions")) for i in range(nsamples)]
    id_mutations = [(i, File(f"{data_path}/muts/{i}.muts")) for i in range(nsamples)]

    sample1_id, sample2_id, regions_file = merge_all_regions(out_path, id_regions)
    _, _, mutations_file = merge_all_mutations(out_path, id_mutations)
    return join_mutations_regions(out_path, sample1_id, sample2_id, mutations_file, regions_file)


@task()
def main(nsamples: int = 100, data_path: str = "data", out_path: str = "computed") -> File:
    """
    Simulate a dataset of genomic samples and then compute their allele frequency.
    """
    datasets = sim_samples(data_path, nsamples)
    allele_freqs_file = calc_allele_freq(nsamples, data_path, out_path)
    return seq([datasets, allele_freqs_file])[-1]
