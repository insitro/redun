import gzip
import os
import re
import shutil
from collections import defaultdict
from itertools import chain
from typing import Any, Dict, List, Optional, cast

from redun import task, script, Dir, File
from redun.file import glob_file
from redun.functools import const, eval_
from redun.tools import copy_file, copy_dir


redun_namespace = "redun.examples.k8s"


# Filename pattern for FASTQ files.
FASTQ_PATTERN = (
    ".*/.*/(?P<sample>.*)_(?P<flowcell>.*)_(?P<lane>.*)_(?P<read>.*).fastq.gz"
)

# File extensions for BWA genome reference index.
BWA_INDEX_EXTS = ["0123", "amb", "ann", "bwt.2bit.64", "pac"]

# Programs.
PICARD_JAR = "/software/picard.jar"

# Public genome reference.
genome_ref_src = File(
    "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/chromosomes/chr22.fa.gz"
)

# Public known sites files.
sites_src_files = [
    File(
        "gs://genomics-public-data/resources/broad/hg38/v0/1000G_phase1.snps.high_confidence.hg38.vcf.gz"
    ),
    File(
        "gs://genomics-public-data/resources/broad/hg38/v0/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz"
    ),
]


@task(version="1")
def download_genome_ref(
    genome_ref_src: File, ref_dir: str, skip_if_exists: bool = True
) -> File:
    """
    Download reference genome to our own directory/bucket.
    """
    dest_path = os.path.join(ref_dir, genome_ref_src.basename())
    if genome_ref_src.path.endswith(".gz"):
        # Unzip genome if it is zipped.
        dest_file = File(dest_path[: -len(".gz")])
        if not skip_if_exists or not dest_file.exists():
            with dest_file.open("wb") as outfile:
                with genome_ref_src.open("rb") as infile:
                    with gzip.open(infile, "rb") as infile2:
                        shutil.copyfileobj(infile2, outfile)
    else:
        # Copy genome as is.
        dest_file = File(dest_path)
        genome_ref_src.copy_to(dest_file, skip_if_exists=skip_if_exists)

    return dest_file


@task(version="1")
def index_genome_ref(genome_ref_file: File) -> Dict[str, File]:
    """
    Create genome reference indexes.
    """
    base_path, fa = genome_ref_file.path.rsplit(".", 1)
    keys = ["0123", "amb", "ann", "bwt.2bit.64", "pac", "fai", "dict"]
    index_exts = [(f"{fa}.{key}" if key != "dict" else key) for key in keys]
    genome_ref_local = genome_ref_file.basename()

    return script(
        f"""
        bwa-mem2 index {genome_ref_local}

        samtools faidx {genome_ref_local}

        gatk CreateSequenceDictionary -R {genome_ref_local}
        """,
        executor="k8s",
        inputs=[
            genome_ref_file.stage(genome_ref_local),
        ],
        outputs={
            key: File(f"{base_path}.{ext}").stage()
            for key, ext in zip(keys, index_exts)
        },
    )


@task(version="1")
def download_sites_files(
    sites_src_files: List[File], ref_dir: str, skip_if_exists: bool = True
) -> List[File]:
    """
    Download all known sites files to our directory/bucket.
    """
    # Download tabix indexes as well.
    index_src_files = [File(f"{file.path}.tbi") for file in sites_src_files]

    sites_files = [
        copy_file(
            file,
            os.path.join(ref_dir, file.basename()),
            skip_if_exists=skip_if_exists,
        )
        for file in sites_src_files
    ]
    index_files = [
        copy_file(
            file,
            os.path.join(ref_dir, file.basename()),
            skip_if_exists=skip_if_exists,
        )
        for file in index_src_files
    ]
    # Download both kinds of files, but just return the sites_files.
    # This is the same as: `[sites_files, index_files][0]`
    return const(sites_files, index_files)


def group_sample_read_pairs(
    read_files: List[str], pattern: str = FASTQ_PATTERN
) -> Dict[str, Dict[str, List[str]]]:
    """
    Group fastq filenames into a nested dict: Dict[sample_id, Dict[read, List[filename]]]

    Example filenames:
    - expt_FC1_L4_1.fastq.gz
    - expt_FC1_L4_2.fastq.gz
    - expt_FC1_L5_1.fastq.gz
    - expt_FC1_L5_2.fastq.gz
    - toy_FC1_L4_1.fastq.gz
    - toy_FC1_L4_2.fastq.gz
    - toy_FC2_L5_1.fastq.gz
    - toy_FC2_L5_2.fastq.gz

    Example nested dict:
    sample_read_files = {
        'expt': {
            '1': ['expt_FC1_L4_1.fastq.gz', 'expt_FC1_L5_1.fastq.gz'],
            '2': ['expt_FC1_L4_2.fastq.gz', 'expt_FC1_L5_2.fastq.gz'],
        },
        'toy': {
            '1': ['toy_FC1_L4_1.fastq.gz', 'toy_FC2_L5_1.fastq.gz'],
            '2': ['toy_FC1_L4_2.fastq.gz', 'toy_FC2_L5_2.fastq.gz'],
        }
    }
    """
    sample_read_files: Dict[str, Dict[str, List[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for read_file in read_files:
        groups = re.match(pattern, read_file)
        assert groups
        sample_read_files[groups["sample"]][groups["read"]].append(read_file)
    return sample_read_files


@task(version="1")
def get_sample_read_pairs(
    read_dirs: List[str],
) -> Dict[str, Dict[str, List[str]]]:
    """
    Determine sample read pair files from a directory.
    """
    sample_read_pairs = {}
    for read_dir in read_dirs:
        read_files = glob_file(read_dir + "/*")
        sample_read_pairs.update(group_sample_read_pairs(read_files))
    return sample_read_pairs


@task(version="1")
def concat_files(src_files: List[File], dest_path: str) -> File:
    """
    Concatenate several source files into one destination file.
    """
    dest_file = File(dest_path)
    with dest_file.open("wb") as out:
        for src_file in src_files:
            with src_file.open("rb") as infile:
                shutil.copyfileobj(infile, out)
    return dest_file


@task(version="1")
def merge_fastq(
    reads1: List[File], reads2: List[File], output_path: str
) -> Dict[str, File]:
    """
    Merge together FASTQs for the same strand.
    """
    return {
        "reads1": concat_files(reads1, f"{output_path}.1.fastq.gz"),
        "reads2": concat_files(reads2, f"{output_path}.2.fastq.gz"),
    }


@task(version="1")
def cut_adapters(
    reads1: File,
    reads2: File,
    output_path: str,
    adapter1_file: Optional[File] = None,
    adapter2_file: Optional[File] = None,
    n_ratio_cutoff: float = 0.1,
) -> Dict[str, File]:
    """
    Trim the adapter sequences from reads in a FASTQ.
    """
    # Conditionally add additional input files and arguments.
    adapter_arg = ""
    adapter_stages = []
    if adapter1_file and adapter2_file:
        adapter_arg = f"-g file:adapter1 -G file:adapter2"
        adapter_stages = [
            adapter1_file.stage("adapter1"),
            adapter2_file.stage("adapter2"),
        ]

    return script(
        f"""
        cutadapt \
            {adapter_arg} \
            -o cutadapt.1.fastq.gz \
            -p cutadapt.2.fastq.gz \
            --pair-filter=any \
            --max-n {n_ratio_cutoff} \
            reads1.fq.gz \
            reads2.fq.gz \
            2>&1 | tee cutadapt.log
        """,
        executor="k8s",
        inputs=[
            reads1.stage("reads1.fq.gz"),
            reads2.stage("reads2.fq.gz"),
        ]
        + adapter_stages,
        outputs={
            "log": File(f"{output_path}.log").stage("cutadapt.log"),
            "reads1": File(f"{output_path}.1.fastq.gz").stage(
                "cutadapt.1.fastq.gz"
            ),
            "reads2": File(f"{output_path}.2.fastq.gz").stage(
                "cutadapt.2.fastq.gz"
            ),
        },
    )


@task(version="2")
def run_bwa(
    sample_id: str,
    reads1: File,
    reads2: File,
    output_bam_path: str,
    genome_ref_file: File,
    genome_ref_index: Dict[str, File],
    platform: str = "illumina",
) -> Dict[str, File]:
    """
    Align FASTQ reads to a reference genome to produce a BAM alignment.
    """
    local_genome_ref = genome_ref_file.basename()

    return script(
        f"""
        bwa-mem2 mem \
            -R "@RG\\tID:{sample_id}\\tSM:{sample_id}\\tPL:{platform}" \
            -o sample.aln.sam \
            {local_genome_ref} \
            reads1.fastq.gz \
            reads2.fastq.gz
        samtools view -o sample.aln.bam sample.aln.sam

        gatk \
            --java-options "-Xmx14G" \
            SortSam \
            --INPUT sample.aln.bam \
            --OUTPUT /dev/stdout \
            --SORT_ORDER "coordinate" \
            --CREATE_INDEX false \
            --CREATE_MD5_FILE false \
            | \
        gatk \
            --java-options "-Xmx14G" \
             SetNmMdAndUqTags \
            --INPUT /dev/stdin \
            --OUTPUT sample.aln.sorted.settag.bam \
            --CREATE_INDEX true \
            --CREATE_MD5_FILE true \
            --REFERENCE_SEQUENCE {local_genome_ref}
        """,
        executor="k8s",
        memory=30,
        inputs=[
            reads1.stage("reads1.fastq.gz"),
            reads2.stage("reads2.fastq.gz"),
            genome_ref_file.stage(),
            [file.stage() for file in genome_ref_index.values()],
        ],
        outputs={
            "bam": File(output_bam_path).stage("sample.aln.sorted.settag.bam"),
            "stdout": File("-"),
        },
    )


@task(version="1")
def mark_dups(bam: File, output_path: str) -> Dict[str, File]:
    """
    Mark duplicate reads in an alignment.
    """
    return script(
        f"""
        gatk \
            --java-options "-Xmx18G" \
            MarkDuplicates \
            --INPUT sample.bam \
            --OUTPUT mark_dup.bam \
            --METRICS_FILE markdup.metrics.txt \
            --VALIDATION_STRINGENCY SILENT \
            --OPTICAL_DUPLICATE_PIXEL_DISTANCE 2500 \
            --ASSUME_SORT_ORDER "queryname" \
            --CREATE_MD5_FILE true
        """,
        executor="k8s",
        memory=20,
        inputs=[
            bam.stage("sample.bam"),
        ],
        outputs={
            "bam": File(f"{output_path}.bam").stage("sample.bam"),
            "dup_metrics": File(f"{output_path}.markdup.metrics.txt").stage(
                "markdup.metrics.txt"
            ),
        },
    )


@task(version="2")
def base_recalib(
    bam: File,
    output_path: str,
    genome_ref_file: File,
    genome_ref_fai_file: File,
    genome_ref_dict_file: File,
    sites_files: List[File],
) -> Dict[str, File]:
    """
    Recalibrate base calls given a BAM alignment.
    """
    # Prepare known sites files.
    sites_index_files = [File(f"{file.path}.tbi") for file in sites_files]

    # Automatically name local file name after basename of remote url.
    sites_stage_files = [file.stage() for file in sites_files]
    sites_index_stage_files = [file.stage() for file in sites_index_files]

    # Use the local name for creating the command arguments.
    known_sites_args = " ".join(
        f"--known-sites {f.local.path}" for f in sites_stage_files
    )

    return script(
        f"""
        gatk \
            --java-options "-Xmx18G" \
            BaseRecalibrator \
            -R genome.fa \
            {known_sites_args} \
            -I sample.bam \
            --use-original-qualities \
            -O recal.table

        gatk \
            --java-options "-Xmx18G" \
            ApplyBQSR \
            -I sample.bam \
            -bqsr recal.table \
            -O sample.processed.bam \
            --static-quantized-quals 10 \
            --static-quantized-quals 20 \
            --static-quantized-quals 30 \
            --add-output-sam-program-record \
            --create-output-bam-md5 \
            --use-original-qualities

        samtools index sample.processed.bam sample.processed.bam.bai
        """,
        executor="k8s",
        memory=20,
        inputs=[
            bam.stage("sample.bam"),
            genome_ref_file.stage("genome.fa"),
            genome_ref_fai_file.stage("genome.fa.fai"),
            genome_ref_dict_file.stage("genome.dict"),
            sites_stage_files,
            sites_index_stage_files,
        ],
        outputs={
            "bam": File(f"{output_path}.bam").stage("sample.processed.bam"),
            "bai": File(f"{output_path}.bam.bai").stage(
                "sample.processed.bam.bai"
            ),
            "recal": File(f"{output_path}.recal.table").stage("recal.table"),
        },
    )


@task(version="1")
def collect_align_metrics(
    bam: File,
    output_path: str,
    genome_ref_file: File,
) -> Dict[str, Any]:
    """
    Collect alignment metrics.
    """
    result = script(
        f"""
        java -jar {PICARD_JAR} \
            CollectAlignmentSummaryMetrics \
            R=genome_ref.fa \
            I=sample.bam \
            O=sample.alignment_metrics.txt

        java -jar {PICARD_JAR} \
            CollectInsertSizeMetrics \
            INPUT=sample.bam \
            OUTPUT=sample.insert_metrics.txt \
            HISTOGRAM_FILE=sample.insert_size_histogram.pdf

        # Make empty files if they don't exist.
        if [ ! -f sample.insert_metrics.txt ]; then
            echo -n fail > sample.insert_metrics.status
            touch sample.insert_metrics.txt
            touch sample.insert_size_histogram.pdf
        else
            echo -n ok > sample.insert_metrics.status
        fi
        """,
        executor="k8s",
        inputs=[
            bam.stage("sample.bam"),
            genome_ref_file.stage("genome_ref.fa"),
        ],
        outputs={
            "align_metrics": File(f"{output_path}.alignment_metrics.txt").stage(
                "sample.alignment_metrics.txt"
            ),
            "insert_metrics.status": File(
                f"{output_path}.insert_metrics.status"
            ).stage("sample.insert_metrics.status"),
            "insert_metrics": File(f"{output_path}.insert_metrics.txt").stage(
                "sample.insert_metrics.txt"
            ),
            "insert_size_hist": File(
                f"{output_path}.insert_size_histogram.pdf"
            ).stage("sample.insert_size_histogram.pdf"),
        },
    )

    # Small post-processing.
    return eval_(
        """{
            **result,
            "has_insert_metrics": result["insert_metrics.status"].read() == "ok",
        }""",
        result=result,
    )


@task(version="1")
def collect_depth_metrics(bam: File, output_path: str) -> Dict[str, File]:
    """
    Collect read alignment depth metrics.
    """
    return script(
        """
        samtools depth -a sample.bam > sample.depth_out.txt
        """,
        executor="k8s",
        inputs=[
            bam.stage("sample.bam"),
        ],
        outputs={
            "depth": File(f"{output_path}.depth.txt").stage(
                "sample.depth_out.txt"
            ),
        },
    )


@task()
def align_sample(
    sample_id: str,
    read_pairs: Dict[str, str],
    genome_ref_file: File,
    genome_ref_index: Dict[str, File],
    sites_files: List[File],
    output_path: str,
) -> Dict[str, Any]:
    """
    Align a single sample's paired FASTQs.
    """
    # Merge fastqs across lanes.
    read1_files = list(map(File, read_pairs["1"]))
    read2_files = list(map(File, read_pairs["2"]))

    output_fastq_path = os.path.join(output_path, sample_id)
    reads = merge_fastq(read1_files, read2_files, output_fastq_path)

    # Cut adapters off of reads.
    output_cutadapt_path = os.path.join(output_path, f"{sample_id}.cutadapt")
    reads = cut_adapters(reads["reads1"], reads["reads2"], output_cutadapt_path)

    # Run alignment.
    output_bam_path = os.path.join(
        output_path, f"{sample_id}.aln.sorted.settag.bam"
    )
    genome_ref_bwa_index = {
        ext: genome_ref_index[ext] for ext in BWA_INDEX_EXTS
    }
    align = run_bwa(
        sample_id,
        reads["reads1"],
        reads["reads2"],
        output_bam_path,
        genome_ref_file=genome_ref_file,
        genome_ref_index=genome_ref_bwa_index,
    )

    # Postprocess alignment.
    align2 = mark_dups(
        align["bam"], os.path.join(output_path, f"{sample_id}.mark_dup")
    )
    align3 = base_recalib(
        align2["bam"],
        os.path.join(output_path, f"{sample_id}.recalib"),
        genome_ref_file=genome_ref_file,
        genome_ref_fai_file=genome_ref_index["fai"],
        genome_ref_dict_file=genome_ref_index["dict"],
        sites_files=sites_files,
    )
    align_metrics = collect_align_metrics(
        align3["bam"],
        os.path.join(output_path, sample_id),
        genome_ref_file=genome_ref_file,
    )
    depth_metrics = collect_depth_metrics(
        align3["bam"], os.path.join(output_path, sample_id)
    )

    return {
        "bam": align3["bam"],
        "bai": align3["bai"],
        "dup_metrics": align2["dup_metrics"],
        "align_metrics": align_metrics,
        "depth_metrics": depth_metrics,
    }


@task()
def align_samples(
    sample_read_pairs,
    genome_ref_file: File,
    genome_ref_index: Dict[str, File],
    sites_files: List[File],
    output_path: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Perform alignment for sample read pairs.
    """
    return {
        sample_id: align_sample(
            sample_id,
            read_pairs,
            genome_ref_file,
            genome_ref_index,
            sites_files,
            os.path.join(output_path, sample_id),
        )
        for sample_id, read_pairs in sample_read_pairs.items()
    }


@task()
def write_samples_file(files: List[File], output_path: str) -> File:
    """
    Write a samples.txt for a list of FASTQ files.
    """
    sample_paths = sorted({file.dirname() for file in files})
    output_file = File(output_path)
    with output_file.open("w") as out:
        for sample_path in sample_paths:
            out.write(f"{sample_path}\n")
    return output_file


@task()
def prepare_samples(output_path: str) -> File:
    """
    Prepare this example workflow by copying the local dataset to S3.

    Parameters
    ----------
    output_path : str
        An S3 path to write example sample data.

    Returns
    -------
    File
        An S3 file containing a sample manifest.
    """
    samples_path = os.path.join(output_path, "samples")
    Dir(samples_path).rmdir(recursive=True)
    data_dir = copy_dir(Dir("data"), samples_path)
    samples_file = write_samples_file(
        data_dir.files(), os.path.join(output_path, "samples.txt")
    )
    return samples_file


@task()
def main(
    samples_file: File,
    output_path: str,
    genome_ref_src: File = genome_ref_src,
    sites_src_files: List[File] = sites_src_files,
):
    """
    Take a directory of sample FASTQs and align them to a reference genome.

    Parameters
    ----------
    samples_file : File
        An S3 path to a sample manifest. Use `prepare_samples()` to generate
        an example file.
    output_path : str
        An S3 path prefix for writing all outputs.
    genome_ref_src : File
        A genome reference file.
    sites_src_files : List[File]
        A list of sites files to use for quality metrics.
    """
    if not samples_file.exists():
        raise ValueError(
            "samples_file does not exist. "
            "You can prepare one with: `redun run workflow.py prepare_samples`"
        )

    # Download reference genome and sites files.
    ref_dir = os.path.join(output_path, "references")
    genome_ref_file = download_genome_ref(genome_ref_src, ref_dir)
    sites_files = download_sites_files(sites_src_files, ref_dir)

    # Index genome.
    genome_ref_index = index_genome_ref(genome_ref_file)

    # Read manifest of read directories.
    read_dirs = [cast(str, f.strip()) for f in samples_file.readlines()]

    # Group read files by sample and read index.
    sample_read_pair_lists = get_sample_read_pairs(read_dirs)

    # Align all samples.
    return align_samples(
        sample_read_pair_lists,
        genome_ref_file=genome_ref_file,
        genome_ref_index=genome_ref_index,
        sites_files=sites_files,
        output_path=output_path,
    )
