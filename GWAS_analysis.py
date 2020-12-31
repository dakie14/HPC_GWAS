from DataManager import DataManager
from GWASTools import parallel_glm
from Enumerators import Format, Family
import pandas as pd
import time
import argparse
from ProcessLogger import ProcessLogger
from multiprocessing import cpu_count

logger = ProcessLogger("GWAS")

def parse_input():
    parser = argparse.ArgumentParser(description='main_input')
    parser.add_argument("--bs",
                        help="Specify how many snps to request from server",
                        type=int,
                        default=0)

    parser.add_argument("--model",
                        help="Specify regression model",
                        type=str,
                        required=True)

    parser.add_argument("--bgen",
                        help="Specify path to folder containing bgen files to use in analysis",
                        type=str,
                        required=True)

    return parser.parse_args()

if __name__ == "__main__":

    args = parse_input()

    batch_size = args.bs if args.bs > 0 else cpu_count()*100
    model = args.model
    bgen_path = args.bgen
    manager = DataManager()

    covariates = manager.get_supplementary_data("covariates")
    samples = manager.get_supplementary_data("samples")["id"].to_list()

    while True:
        overall_batch_time = time.time()

        batch = manager.get_batch(batch_size)
        if batch["chr"] == -1:
            break

        _chr = batch["chr"]
        seeks = batch["data"]

        data_path = bgen_path + "/ukb_imp_chr" + str(_chr) + "_v3.bgen"

        result = parallel_glm(
            model,
            data_path,
            covariates,
            Family.Binomial,
            seeks,
            samples=samples
        )

        manager.store_result(result.as_dict())
        logger.info("--- (batch) %s seconds ---" % (time.time() - overall_batch_time))

