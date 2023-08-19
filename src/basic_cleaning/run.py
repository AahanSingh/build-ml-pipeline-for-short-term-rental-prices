#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning,
exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    logger.info("Starting WandB run")
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Getting input artifact")
    local_path = run.use_artifact(
        args.input_artifact).file()

    df = pd.read_csv(local_path)
    logger.info(
        "Dropping outliers outside the range ({}, {})".format(
            args.min_price,
            args.max_price))
    # Drop outliers
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    logger.info("Converting last_review column to datetime format")
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])
    logger.info("Saving cleaned csv")
    df.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    logger.info("Logging output artifact")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="A very basic data cleaning step")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Name of the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Output artifact type",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description about the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum price to filter properties",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum price to filter properties",
        required=True
    )

    args = parser.parse_args()

    go(args)
