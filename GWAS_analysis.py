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

    parser.add_argument("--sample",
                        help="Specify path to sample file",
                        type=str,
                        required=True)

    return parser.parse_args()

if __name__ == "__main__":

    args = parse_input()

    batch_size = args.bs if args.bs > 0 else cpu_count()*100
    model = args.model
    bgen_path = args.bgen
    samples_path = args.sample
    manager = DataManager()

    samples = pd.read_csv(samples_path, header=0, sep='\s+', skiprows=[1], usecols=["ID_1"])
    samples['idx'] = samples.index
    samples.rename(columns={"ID_1": "id"}, inplace=True)
    samples = samples.sort_values(by=['idx'])["id"].tolist()

    covariates = manager.get_covariates()

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
            Format.BGEN,
            covariates,
            Family.Binomial,
            seeks=seeks,
            samples=samples
        )

        manager.store_result(result.as_dict())
        logger.info("--- (batch) %s seconds ---" % (time.time() - overall_batch_time))

