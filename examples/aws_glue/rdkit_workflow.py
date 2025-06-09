"""
Adds the inchi field to a DataFrame, using rdkit.

Usage:
    redun run rdkit_workflow.py calculate_inchi \
        --input-dir <s3_path> \
        --output-dir <s3_path> \
        --smiles-col "smiles"
"""

import logging

from redun import ShardedS3Dataset, glue, task


@glue.udf
def get_inchi(smiles: str) -> str:
    from rdkit import Chem

    try:
        result = Chem.MolToInchi(Chem.MolFromSmiles(smiles))
    except Exception as e:
        logging.getLogger().error(f"PROBLEM: {e}")
        result = "ERROR"

    return result


@task(
    executor="glue", workers=10, worker_type="G.1X", additional_libs=["rdkit>=2021and<2023.09.3"]
)
def calculate_inchi(
    input_dir: ShardedS3Dataset, output_dir: str, smiles_col: str = "smiles"
) -> ShardedS3Dataset:
    """
    Adds the "inchi" column to a DataFrame as calculated from a column containing
    SMILES strings.

    Parameters:
    ----------

    input_dir: ShardedS3Dataset
        Location on S3 containing input dataframes. Can be sharded into one or more shards.
        Should be in parquet format for this example.

    output_dir: str
        Location on S3 where output dataframes will be written as parquet-format
        shards.

    smiles_col: str
        Column name containing SMILES strings. Defaults to "smiles_0_1_2_3".
    """

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Load dataset
    logger.info("Loading data")
    dataset = input_dir.load_spark().repartition(10)

    # Generate INCHI column
    logger.info("Running UDF")
    with_inchi = dataset.withColumn("inchi", get_inchi(smiles_col))

    # Save dataset
    logger.info("Saving output")
    output_ds = ShardedS3Dataset(output_dir, format="parquet")
    output_ds.purge_spark(remove_older_than=0)
    output_ds.save_spark(with_inchi)

    return output_ds
