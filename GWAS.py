from DataManager import DataManager
from GWASTools import parallel_glm
from Enumerators import Family
import pandas as pd
import time
from ProcessLogger import ProcessLogger

logger = ProcessLogger("GWAS")

def run(batch_size, model, bgen_path, gm, cores, sample_path, covar_path, family):
    total_time = time.time()
    manager = DataManager()

    if not covar_path:
        covariates = manager.get_supplementary_data("covariates")
    else:
        covariates = pd.read_csv(covar_path, header=0, sep='t')

    if not sample_path:
        samples = manager.get_supplementary_data("samples")
    else:
        samples = pd.read_csv(sample_path, header=0)["id"].to_list()

    if family == "binomial":
        fam = Family.Binomial
    else:
        print("Error: family (" + family + ") not recognised")
        exit(0)

    while True:
        overall_batch_time = time.time()

        batch = manager.get_batch(batch_size)
        if batch["chr"] == -1:
            break

        _chr = batch["chr"]
        seeks = batch["data"]

        data_path = bgen_path + "/chr" + str(_chr) + ".bgen"

        result = parallel_glm(
            model,
            data_path,
            covariates,
            fam,
            seeks,
            samples=samples,
            cores=cores,
            genetic_model=gm
        )

        manager.store_result(result.as_dict())
        logger.info("--- (batch) %s seconds ---" % (time.time() - overall_batch_time))

    logger.info("--- (total) %s seconds ---" % (time.time() - total_time))
    logger.info("Analysis finished")