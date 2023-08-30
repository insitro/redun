# Real workflows: a bioinformatics pipeline for read alignment

In this example, we implement a whole genome sequencing read alignment pipeline using redun to run shell commands on AWS Batch.

This workflow has similar requirements and setup as [05_aws_batch](../05_aws_batch). Please follow the same setup and configuration steps for this workflow before running it.


## Setup

This example uses a Docker image to run bioinformatics commands like `samtools`, `bwa`, and `gatk`. We will build an image with these commands installed as well as the AWS CLI. In this example, we show that redun is not required to be installed inside the docker image, if only shell scripts are executed on AWS Batch.

Following similar commands from [05_aws_batch](../05_aws_batch/README.md), we can make our Docker image using the make command. To specify a specific registry and image name, please edit the variables in [Makefile](docker/Makefile).

```sh
cd docker
make login
make build
```

After the image builds, we need to publish it to ECR so that it is accessible by AWS Batch. There are several steps for doing that, which are covered in these make commands:

```sh
# If the docker repo does not exist yet.
make create-repo

# Push the locally built image to ECR.
make push
```

Again, similar to the previous example, we need three things to run tasks on AWS Batch:
- An AWS Batch queue that we can submit to.
- A Docker image published in ECR that contains the bioinformatics commands we wish to run.
- An S3 prefix redun can use for scratch space (communicating task input/output values).

Once we have these resources ready, we can configure redun to use them, using a config, `.redun/redun.ini`, in our project directory. Here is an example of one such config file:

```ini
# .redun/redun.ini

[executors.batch]
type = aws_batch
image = YOUR_ACCOUNT_ID.dkr.ecr.us-west-2.amazonaws.com/redun_example_bioinfo_batch:latest
queue = YOUR_QUEUE_NAME
s3_scratch = s3://YOUR_BUCKET/redun/
debug = False
role = arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_ROLE
```

For more details, follow the notes in the previous example, [05_aws_batch](../05_aws_batch/README.md#configuring-executor).


## Running the workflow

In this example, we will perform the following steps:
- Upload a small downsampled sequencing read dataset (FASTQ files) of GM12878 to an S3 bucket of our choice.
- Download the reference genome of chromosome 22 from UCSC to our S3 bucket.
- Index the reference genome for the BWA and GATK tools.
- Concatenate FASTQs across flow cell lanes.
- Align FASTQs to reference genome using BWA to produce a sequencing alignment BAM file.
- Mark duplicate reads in BAM.
- Recalibrate the base calls using the BAM.
- Collect metrics for the alignment, inserts, and overall read depth.

First, we need to install [`gcsfs`](https://pypi.org/project/gcsfs/) because the sites files are downloaded from google cloud.
This task runs on the local executor, not on AWS batch so you can run `pip install gcsfs` to enable redun to read files from google cloud.

Second, we need to prepare a dataset into cloud storage by copying the local FASTQs to S3.

```sh
redun run workflow.py prepare_samples --output-path s3://YOUR_BUCKET/bioinfo_batch/
```

This will upload the example FASTQ files in `data/` to s3://YOUR_BUCKET/bioinfo_batch/samples/`.

We can then run the rest of our pipeline as usual. This may take several minutes to complete:

```sh
redun run workflow.py main \
    --samples-file s3://YOUR_BUCKET/bioinfo_batch/samples.txt \
    --output-path s3://YOUR_BUCKET/bioinfo_batch/
```

Also feel free to call any individual task to debug a single step:

```sh
redun run workflow.py run_bwa --sample_id=... --reads1=s3://... --reads2=s3://... --output_bam_path=s3://...
```

If everything works, you should see the final status as something like this:

```
redun run workflow.py main

...

[redun] | JOB STATUS 2021/07/06 17:07:05
[redun] | TASK                                                PENDING RUNNING  FAILED  CACHED    DONE   TOTAL
[redun] |
[redun] | ALL                                                       0       0       0      45       1      46
[redun] | redun.const                                               0       0       0       1       0       1
[redun] | redun.copy_file                                           0       0       0       4       0       4
[redun] | redun.eval_                                               0       0       0       2       0       2
[redun] | redun.examples.bioinfo_docker.align_sample                0       0       0       2       0       2
[redun] | redun.examples.bioinfo_docker.align_samples               0       0       0       1       0       1
[redun] | redun.examples.bioinfo_docker.base_recalib                0       0       0       2       0       2
[redun] | redun.examples.bioinfo_docker.collect_align_metrics       0       0       0       2       0       2
[redun] | redun.examples.bioinfo_docker.collect_depth_metrics       0       0       0       2       0       2
[redun] | redun.examples.bioinfo_docker.concat_files                0       0       0       4       0       4
[redun] | redun.examples.bioinfo_docker.cut_adapters                0       0       0       2       0       2
[redun] | redun.examples.bioinfo_docker.download_genome_ref         0       0       0       1       0       1
[redun] | redun.examples.bioinfo_docker.download_sites_files        0       0       0       1       0       1
[redun] | redun.examples.bioinfo_docker.get_sample_read_pairs       0       0       0       1       0       1
[redun] | redun.examples.bioinfo_docker.index_genome_ref            0       0       0       1       0       1
[redun] | redun.examples.bioinfo_docker.main                        0       0       0       0       1       1
[redun] | redun.examples.bioinfo_docker.mark_dups                   0       0       0       2       0       2
[redun] | redun.examples.bioinfo_docker.merge_fastq                 0       0       0       2       0       2
[redun] | redun.examples.bioinfo_docker.run_bwa                     0       0       0       2       0       2
[redun] | redun.script                                              0       0       0      13       0      13
[redun]
[redun] Execution duration: 8.82 seconds
{'expt': {'align_metrics': {'align_metrics': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.alignment_metrics.txt, hash=fb23d375),
                            'has_insert_metrics': True,
                            'insert_metrics': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.insert_metrics.txt, hash=ad4cf78e),
                            'insert_metrics.status': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.insert_metrics.status, hash=daaf8d5e),
                            'insert_size_hist': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.insert_size_histogram.pdf, hash=828a55f7)},
          'bai': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam.bai, hash=8c83723a),
          'bam': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam, hash=0f74f997),
          'depth_metrics': {'depth': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.depth.txt, hash=4f823939)},
          'dup_metrics': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.mark_dup.markdup.metrics.txt, hash=5d8d233d)},
 'toy': {'align_metrics': {'align_metrics': File(path=s3://YOUR_BUCKET/bioinfo_batch/toy/toy.alignment_metrics.txt, hash=ebb9027f),
                           'has_insert_metrics': True,
                           'insert_metrics': File(path=s3://YOUR_BUCKET/bioinfo_batch/toy/toy.insert_metrics.txt, hash=a260420b),
                           'insert_metrics.status': File(path=s3://YOUR_BUCKET/bioinfo_batch/toy/toy.insert_metrics.status, hash=e67f69c9),
                           'insert_size_hist': File(path=s3://YOUR_BUCKET/bioinfo_batch/toy/toy.insert_size_histogram.pdf, hash=b844442d)},
         'bai': File(path=s3://YOUR_BUCKET/bioinfo_batch/toy/toy.recalib.bam.bai, hash=a0a99f12),
         'bam': File(path=s3://YOUR_BUCKET/bioinfo_batch/toy/toy.recalib.bam, hash=15bdb8ab),
         'depth_metrics': {'depth': File(path=s3://YOUR_BUCKET/bioinfo_batch/toy/toy.depth.txt, hash=37f830e6)},
         'dup_metrics': File(path=s3://YOUR_BUCKET/bioinfo_batch/toy/toy.mark_dup.markdup.metrics.txt, hash=2a4169e6)}}
```


## Data provenance

To see all the files used by the workflow try this command:

```
redun log --file

File f7a27591 2021-07-05 10:27:43 r  data/sample1/expt_FC1_L4_1.fastq.gz
File bfd36535 2021-07-05 10:27:43 r  data/sample1/expt_FC1_L4_2.fastq.gz
File 13c4a339 2021-07-05 10:27:43 r  data/sample1/expt_FC1_L5_1.fastq.gz
File a6d18f79 2021-07-05 10:27:43 r  data/sample1/expt_FC1_L5_2.fastq.gz
File f86cdda2 2021-07-05 10:27:43 r  data/sample2/toy_FC1_L4_1.fastq.gz
File aaddde5c 2021-07-05 10:27:43 r  data/sample2/toy_FC1_L4_2.fastq.gz
File dab52320 2021-07-05 10:27:43 r  data/sample2/toy_FC2_L5_1.fastq.gz
File 28bef6cd 2021-07-05 10:27:43 r  data/sample2/toy_FC2_L5_2.fastq.gz
File 1e9dd305 2021-07-05 08:18:32 r  gs://genomics-public-data/resources/broad/hg38/v0/1000G_phase1.snps.high_confidence.hg38.vcf.gz
File 01544142 2021-07-05 08:18:34 r  gs://genomics-public-data/resources/broad/hg38/v0/1000G_phase1.snps.high_confidence.hg38.vcf.gz.tbi
File 2f5a4094 2021-07-05 08:18:32 r  gs://genomics-public-data/resources/broad/hg38/v0/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz
File 1356dab2 2021-07-05 08:18:34 r  gs://genomics-public-data/resources/broad/hg38/v0/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi
File 91081d95 2021-07-04 20:12:45 r  https://hgdownload.soe.ucsc.edu/goldenPath/hg38/chromosomes/chr22.fa.gz
File f09322e8 2021-07-04 22:43:00 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.1.fastq.gz
File 59073771 2021-07-04 22:43:00 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.2.fastq.gz
File fb23d375 2021-07-05 09:47:24 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.alignment_metrics.txt
File 59bde4f8 2021-07-05 08:14:18 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.aln.sorted.settag.bam
File 1f7106c0 2021-07-04 22:43:02 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.cutadapt.1.fastq.gz
File 0a4dad34 2021-07-04 22:43:02 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.cutadapt.2.fastq.gz
File 6079970d 2021-07-04 22:43:02 w  s3://YOUR_BUCKET/bioinfo_batch/expt/expt.cutadapt.log
File 4f823939 2021-07-04 23:16:28 w  s3://YOUR_BUCKET/bioinfo_batch/expt/expt.depth.txt
File daaf8d5e 2021-07-04 23:16:28 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.insert_metrics.status
File ad4cf78e 2021-07-05 09:47:24 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.insert_metrics.txt
File 828a55f7 2021-07-05 09:47:24 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.insert_size_histogram.pdf
File 0c7b0075 2021-07-05 08:17:57 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.mark_dup.bam
File 5d8d233d 2021-07-05 08:17:57 w  s3://YOUR_BUCKET/bioinfo_batch/expt/expt.mark_dup.markdup.metrics.txt
File 0f74f997 2021-07-05 09:47:24 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam
File 8c83723a 2021-07-05 09:47:24 w  s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam.bai
File 681f1020 2021-07-05 09:47:31 w  s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.recal.table
File ff680b01 2021-07-05 09:25:03 rw s3://YOUR_BUCKET/bioinfo_batch/references/1000G_phase1.snps.high_confidence.hg38.vcf.gz
File a95af907 2021-07-05 09:34:18 rw s3://YOUR_BUCKET/bioinfo_batch/references/1000G_phase1.snps.high_confidence.hg38.vcf.gz.tbi
File a2b33c68 2021-07-05 09:34:30 r  s3://YOUR_BUCKET/bioinfo_batch/references/1000G_phase1.snps.high_confidence.hg38.vcf.gz.tbi.tbi
File 669bb281 2021-07-05 09:05:51 rw s3://YOUR_BUCKET/bioinfo_batch/references/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz
File 77ddcf39 2021-07-05 09:34:18 rw s3://YOUR_BUCKET/bioinfo_batch/references/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi
File 9209dc2c 2021-07-05 09:34:30 r  s3://YOUR_BUCKET/bioinfo_batch/references/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi.tbi
File 789aa94a 2021-07-04 22:37:47 rw s3://YOUR_BUCKET/bioinfo_batch/references/chr22.dict
File f1555fba 2021-07-04 20:18:02 rw s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa
File 1971acf7 2021-07-04 22:37:47 rw s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.0123
File 0686ceb5 2021-07-04 22:37:47 rw s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.amb
File 1345f1f0 2021-07-04 22:37:47 rw s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.ann
File b4fdbbdb 2021-07-04 22:37:47 rw s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.bwt.2bit.64
File 25a1a9e6 2021-07-04 22:37:47 rw s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.fai
File d221c107 2021-07-04 17:34:58 r  s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.gz
File 619e1ec4 2021-07-04 22:37:47 rw s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.pac
File 069d7b54 2021-07-05 14:28:38 r  s3://YOUR_BUCKET/bioinfo_batch/samples.txt
File 6d57c3a0 2021-07-05 10:26:34 rw s3://YOUR_BUCKET/bioinfo_batch/samples/read_dir_sheet
File e318f6c7 2021-07-05 10:26:34 rw s3://YOUR_BUCKET/bioinfo_batch/samples/sample1/expt_FC1_L4_1.fastq.gz
File a2c8f670 2021-07-05 10:26:34 rw s3://YOUR_BUCKET/bioinfo_batch/samples/sample1/expt_FC1_L4_2.fastq.gz
File f42ed783 2021-07-05 10:26:34 rw s3://YOUR_BUCKET/bioinfo_batch/samples/sample1/expt_FC1_L5_1.fastq.gz
File bad69e02 2021-07-05 10:26:34 rw s3://YOUR_BUCKET/bioinfo_batch/samples/sample1/expt_FC1_L5_2.fastq.gz
File b8ec8acc 2021-07-05 10:26:34 rw s3://YOUR_BUCKET/bioinfo_batch/samples/sample2/toy_FC1_L4_1.fastq.gz
File 7e67bfca 2021-07-05 10:26:34 rw s3://YOUR_BUCKET/bioinfo_batch/samples/sample2/toy_FC1_L4_2.fastq.gz
File 0ca1cb28 2021-07-05 10:26:34 rw s3://YOUR_BUCKET/bioinfo_batch/samples/sample2/toy_FC2_L5_1.fastq.gz
File f266e8d7 2021-07-05 10:26:34 rw s3://YOUR_BUCKET/bioinfo_batch/samples/sample2/toy_FC2_L5_2.fastq.gz
File d67e1e6e 2021-07-04 22:43:00 rw s3://YOUR_BUCKET/bioinfo_batch/toy/toy.1.fastq.gz
File 4c9892de 2021-07-04 22:43:00 rw s3://YOUR_BUCKET/bioinfo_batch/toy/toy.2.fastq.gz
File ebb9027f 2021-07-05 09:47:24 rw s3://YOUR_BUCKET/bioinfo_batch/toy/toy.alignment_metrics.txt
File c38c1625 2021-07-05 08:14:18 rw s3://YOUR_BUCKET/bioinfo_batch/toy/toy.aln.sorted.settag.bam
File 9b3031c6 2021-07-04 22:43:04 rw s3://YOUR_BUCKET/bioinfo_batch/toy/toy.cutadapt.1.fastq.gz
File 64f7d96d 2021-07-04 22:43:04 rw s3://YOUR_BUCKET/bioinfo_batch/toy/toy.cutadapt.2.fastq.gz
File 01cde286 2021-07-04 22:43:04 w  s3://YOUR_BUCKET/bioinfo_batch/toy/toy.cutadapt.log
```

To see the upstream dataflow of a particular output BAM alignnment, use the `redun log` command like this:

```
redun log s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam

File 0f74f997 2021-07-05 09:47:24 rw s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam
Produced by Job 0dd1bca5

  Job 0dd1bca5-437f-43cc-bc41-42ae9123d986 [ DONE ] 2021-07-05 09:52:00:  redun.postprocess_script([0, b'download: s3://YOUR_BUCKET/bioinfo_batch/references/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi to ./Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.t..., {'bam': StagingFile(local=File(path=sample.processed.bam, hash=16d7085f), remote=File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam, hash=20d34447)), 'bai..., temp_path=None)
  Traceback: Exec 1a29b158 > (4 Jobs) > Job 10f86bff script > Job 0dd1bca5 postprocess_script
  Duration: 0:00:00.25

    CallNode 1c93e82b4b728348f323bde2cec0f1f33f7c6373 redun.postprocess_script
      Args:   [0, b'download: s3://YOUR_BUCKET/bioinfo_batch/references/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi to ./Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.t..., {'bam': StagingFile(local=File(path=sample.processed.bam, hash=16d7085f), remote=File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam, hash=20d34447)), 'bai..., temp_path=None
      Result: {'bam': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam, hash=0f74f997), 'bai': File(path=s3://YOUR_BUCKET/bioinfo_batch...

    Task f6fac1c39779ca13e0f35723b46fba19c3f4ebb5 redun.postprocess_script

      def postprocess_script(result: Any, outputs: Any, temp_path: Optional[str] = None) -> Any:
          """
          Postprocess the results of a script task.
          """

          def get_file(value: Any) -> Any:
              if isinstance(value, File) and value.path == "-":
                  # File for script stdout.
                  return result
              elif isinstance(value, Staging):
                  # Staging files and dir turn into their remote versions.
                  cls = type(value.remote)
                  return cls(value.remote.path)
              else:
                  return value

          if temp_path:
              shutil.rmtree(temp_path)

          return map_nested_value(get_file, outputs)


    Upstream dataflow:

      result = {'bam': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam, hash=0f74f997), 'bai': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.bam.bai, hash=8c83723a), 'recal': File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib.recal.table, hash=681f1020)}

      result <-- <2c617548> base_recalib(bam, output_path, genome_ref_file, genome_ref_idx_file, genome_ref_dict_file, sites_files)
        bam                  = <0c7b0075> File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.mark_dup.bam, hash=0c7b0075)
        output_path          = <4d2e7c87> 's3://YOUR_BUCKET/bioinfo_batch/expt/expt.recalib'
        genome_ref_dict_file = <789aa94a> File(path=s3://YOUR_BUCKET/bioinfo_batch/references/chr22.dict, hash=789aa94a)
        genome_ref_fai_file  = <25a1a9e6> File(path=s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.fai, hash=25a1a9e6)
        genome_ref_file      = <f1555fba> File(path=s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa, hash=f1555fba)
        sites_files          = <d28a3ca1> [File(path=s3://YOUR_BUCKET/bioinfo_batch/references/1000G_phase1.snps.high_confidence.hg38.vcf.gz, hash=ff680b01), File(path=s3://YOUR_BUCKET/...

      bam <-- <428d12bb> mark_dups(bam_2, output_path_2)
        bam_2         = <59bde4f8> File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.aln.sorted.settag.bam, hash=59bde4f8)
        output_path_2 = <21d9ae41> 's3://YOUR_BUCKET/bioinfo_batch/expt/expt.mark_dup'

      bam_2 <-- <89c40bd9> run_bwa(sample_id, reads1, reads2, output_bam_path, platform, genome_ref_file_2, genome_ref_bwa_idx_files)
        sample_id         = <e985deba> 'expt'
        reads1            = <1f7106c0> File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.cutadapt.1.fastq.gz, hash=1f7106c0)
        reads2            = <0a4dad34> File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.cutadapt.2.fastq.gz, hash=0a4dad34)
        output_bam_path   = <9f002265> 's3://YOUR_BUCKET/bioinfo_batch/expt/expt.aln.sorted.settag.bam'
        genome_ref_file_2 = <f1555fba> File(path=s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa, hash=f1555fba)
        genome_ref_index  = <15e1231d> {'0123': File(path=s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.0123, hash=1971acf7), 'amb': File(path=s3://YOUR_BUCKET/...
        platform          = <dc0134d0> 'illumina'

      sample_id <-- argument of <817070e0> align_sample(sample_id, read_pairs, genome_ref_file, genome_ref_index, sites_files, output_path)
                <-- origin

      reads1 <-- derives from
        cut_adapters_result = <9f663e35> {'stdout': [0, b'download: s3://YOUR_BUCKET/bioinfo_batch/expt/expt.2.fastq.gz to ./reads2.fq.gz\ndownload: s3://YOUR_BUCKET/...

      reads2 <-- derives from
        cut_adapters_result = <9f663e35> {'stdout': [0, b'download: s3://YOUR_BUCKET/bioinfo_batch/expt/expt.2.fastq.gz to ./reads2.fq.gz\ndownload: s3://YOUR_BUCKET/...

      cut_adapters_result <-- <6c54ce6c> cut_adapters(reads1_2, reads2_2, output_path_3, adapter1_file, adapter2_file, n_ratio_cutoff)
        reads1_2       = <f09322e8> File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.1.fastq.gz, hash=f09322e8)
        reads2_2       = <59073771> File(path=s3://YOUR_BUCKET/bioinfo_batch/expt/expt.2.fastq.gz, hash=59073771)
        output_path_3  = <74fc7e5c> 's3://YOUR_BUCKET/bioinfo_batch/expt/expt.cutadapt'
        adapter1_file  = <8ca852f0> None
        adapter2_file  = <8ca852f0> None
        n_ratio_cutoff = <9ae2a179> 0.1

      reads1_2 <-- <afc2e334> merge_fastq(reads1, reads2, output_path)
               <-- <1748511f> concat_files(src_files, dest_path)
        src_files = <aeb9fd22> [File(path=s3://YOUR_BUCKET/bioinfo_batch/samples/sample1/expt_FC1_L4_1.fastq.gz, hash=1826e2c7), File(path=s3://YOUR_BUCKET/...
        dest_path = <d42683ce> 's3://YOUR_BUCKET/bioinfo_batch/expt/expt.1.fastq.gz'

      src_files <-- argument of <afc2e334> merge_fastq(reads1, reads2, output_path)
                <-- origin

      reads2_2 <-- <afc2e334> merge_fastq(reads1, reads2, output_path)
               <-- <adf14aab> concat_files(src_files_2, dest_path_2)
        src_files_2 = <c725263f> [File(path=s3://YOUR_BUCKET/bioinfo_batch/samples/sample1/expt_FC1_L4_2.fastq.gz, hash=12406a0c), File(path=s3://YOUR_BUCKET/...
        dest_path_2 = <cbb71808> 's3://YOUR_BUCKET/bioinfo_batch/expt/expt.2.fastq.gz'

      src_files_2 <-- argument of <afc2e334> merge_fastq(reads1, reads2, output_path)
                  <-- origin

      genome_ref_dict_file <-- argument of <817070e0> align_sample(sample_id, read_pairs, genome_ref_file, genome_ref_index, sites_files, output_path)
                           <-- argument of <c3ffdad5> align_samples(sample_read_pairs, genome_ref_file, genome_ref_index, sites_files, output_path)
                           <-- <c3ffdad5> genome_ref_dict_file_2

      genome_ref_fai_file <-- argument of <817070e0> align_sample(sample_id, read_pairs, genome_ref_file, genome_ref_index, sites_files, output_path)
                          <-- argument of <c3ffdad5> align_samples(sample_read_pairs, genome_ref_file, genome_ref_index, sites_files, output_path)
                          <-- <c3ffdad5> genome_ref_fai_file_2

      genome_ref_dict_file_2 <-- derives from
        index_genome_ref_result = <76ff4d25> {'0123': File(path=s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.0123, hash=1971acf7), 'amb': File(path=s3://YOUR_BUCKEY/...

      genome_ref_fai_file_2 <-- derives from
        index_genome_ref_result = <76ff4d25> {'0123': File(path=s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa.0123, hash=1971acf7), 'amb': File(path=s3://YOUR_BUCKET/...

      index_genome_ref_result <-- <79f7c4b0> index_genome_ref(genome_ref_file_3)
        genome_ref_file_3 = <f1555fba> File(path=s3://YOUR_BUCKET/bioinfo_batch/references/chr22.fa, hash=f1555fba)

      genome_ref_file_3 <-- <d977bf82> download_genome_ref(genome_ref_src, ref_dir)
        genome_ref_src = <91081d95> File(path=https://hgdownload.soe.ucsc.edu/goldenPath/hg38/chromosomes/chr22.fa.gz, hash=91081d95)
        ref_dir        = <d4fe1848> 's3://YOUR_BUCKET/bioinfo_batch/references'

      genome_ref_src <-- argument of <c4f98b4c> main(read_dirs_file, output_path, genome_ref_src, sites_src_files)
                     <-- origin

      genome_ref_file_2 <-- <817070e0> genome_ref_file_4

      genome_ref_file <-- <817070e0> genome_ref_file_4

      genome_ref_file_4 <-- argument of <c3ffdad5> align_samples(sample_read_pairs, genome_ref_file, genome_ref_index, sites_files, output_path)
                        <-- <c9ea99ed> download_genome_ref(genome_ref_src_2, ref_dir_2, skip_if_exists)
        genome_ref_src_2 = <91081d95> File(path=https://hgdownload.soe.ucsc.edu/goldenPath/hg38/chromosomes/chr22.fa.gz, hash=91081d95)
        ref_dir_2        = <d4fe1848> 's3://YOUR_BUCKET/bioinfo_batch/references'
        skip_if_exists   = <4ea83061> True

      genome_ref_src_2 <-- argument of <cc658576> main(samples_file, output_path, genome_ref_src, sites_src_files)
                       <-- origin

      sites_files <-- argument of <817070e0> align_sample(sample_id, read_pairs, genome_ref_file, genome_ref_index, sites_files, output_path)
                  <-- argument of <c3ffdad5> align_samples(sample_read_pairs, genome_ref_file, genome_ref_index, sites_files, output_path)
                  <-- <ae9f6785> download_sites_files(sites_src_files, ref_dir, skip_if_exists)
                  <-- <2d85ba90> const(x, _)
        x = <d28a3ca1> [File(path=s3://YOUR_BUCKET/bioinfo_batch/references/1000G_phase1.snps.high_confidence.hg38.vcf.gz, hash=ff680b01), File(path=s3://YOUR_BUCKET/...
        _ = <c914d1d8> [File(path=s3://YOUR_BUCKET/bioinfo_batch/references/1000G_phase1.snps.high_confidence.hg38.vcf.gz.tbi, hash=a95af907), File(path=s3://YOUR_BUCKET/...

      x <-- derives from
        copy_file_result   = <669bb281> File(path=s3://YOUR_BUCKET/bioinfo_batch/references/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz, hash=669bb281)
        copy_file_result_2 = <ff680b01> File(path=s3://YOUR_BUCKET/bioinfo_batch/references/1000G_phase1.snps.high_confidence.hg38.vcf.gz, hash=ff680b01)

      copy_file_result_2 <-- <94a076e4> copy_file(src_file, dest_path_3, skip_if_exists_2)
        src_file         = <1e9dd305> File(path=gs://genomics-public-data/resources/broad/hg38/v0/1000G_phase1.snps.high_confidence.hg38.vcf.gz, hash=1e9dd305)
        dest_path_3      = <0cf1daa4> 's3://YOUR_BUCKET/bioinfo_batch/references/1000G_phase1.snps.high_confidence.hg38.vcf.gz'
        skip_if_exists_2 = <4ea83061> True

      src_file <-- argument of <33006910> download_sites_files(sites_src_files, ref_dir, skip_if_exists)
               <-- argument of <d275fd65> main(read_dirs_file, output_path, genome_ref_src, sites_src_files)
               <-- origin

      copy_file_result <-- <e2b5c44b> copy_file(src_file_2, dest_path_4, skip_if_exists_3)
        src_file_2       = <2f5a4094> File(path=gs://genomics-public-data/resources/broad/hg38/v0/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz, hash=2f5a4094)
        dest_path_4      = <b2aa8eff> 's3://YOUR_BUCKET/bioinfo_batch/references/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz'
        skip_if_exists_3 = <4ea83061> True

      src_file_2 <-- argument of <33006910> download_sites_files(sites_src_files, ref_dir, skip_if_exists)
                 <-- argument of <d275fd65> main(read_dirs_file, output_path, genome_ref_src, sites_src_files)
                 <-- origin

      _ <-- derives from
        copy_file_result_3 = <77ddcf39> File(path=s3://YOUR_BUCKET/bioinfo_batch/references/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi, hash=77ddcf39)
        copy_file_result_4 = <a95af907> File(path=s3://YOUR_BUCKET/bioinfo_batch/references/1000G_phase1.snps.high_confidence.hg38.vcf.gz.tbi, hash=a95af907)

      copy_file_result_4 <-- <2e7bcc94> copy_file(src_file_3, dest_path_5, skip_if_exists_4)
        src_file_3       = <01544142> File(path=gs://genomics-public-data/resources/broad/hg38/v0/1000G_phase1.snps.high_confidence.hg38.vcf.gz.tbi, hash=01544142)
        dest_path_5      = <4f3acabf> 's3://YOUR_BUCKET/bioinfo_batch/references/1000G_phase1.snps.high_confidence.hg38.vcf.gz.tbi'
        skip_if_exists_4 = <4ea83061> True

      copy_file_result_3 <-- <6ec755b8> copy_file(src_file_4, dest_path_6, skip_if_exists_5)
        src_file_4       = <1356dab2> File(path=gs://genomics-public-data/resources/broad/hg38/v0/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi, hash=1356dab2)
        dest_path_6      = <d05b8a6d> 's3://YOUR_BUCKET/bioinfo_batch/references/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi'
        skip_if_exists_5 = <4ea83061> True

      skip_if_exists_2 <-- <33006910> skip_if_exists_6

      skip_if_exists_3 <-- <33006910> skip_if_exists_6

      skip_if_exists_4 <-- <33006910> skip_if_exists_6

      skip_if_exists_5 <-- <33006910> skip_if_exists_6

      skip_if_exists_6 <-- origin
```

## Conclusion

In this example, we ran a realistic bioinformatic pipeline in AWS Batch. The example combines the techniques we've seen in the earlier examples:

- Building up a workflow with hierarchical tasks.
- Using `File` for reactivity and provenance tracking.
- Using nested values to pass multiple arguments and return multiple results.
- Using `script()` to easily call scripts.
- Using `File.stage()` to stage files to and from cloud object storage.
- Using the `aws_batch` executor to run Docker containers on batch.

If you look closely, there are a few new features used as well:

- Tasks are manually versioned using `@task(version="1")`. This allows the user to refactor a task without invalidating past cached results. The user can manually update the version when they want to trigger task re-execution.
- `glob_file()` is used to find `File`s within a directory path.
- `copy_file()` and `copy_dir()` are used to easily copy `File`s and `Dir`s (directories) between local and cloud locations.
- `eval_()` is used to do a small post-processing of the `collect_align_metrics` script task.
