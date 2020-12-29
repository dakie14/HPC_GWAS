######  GENERAL  ######
import pandas as pd
import time
from functools import partial
import enum

######  DATA_READERS  ######
from PyBGEN import PyBGEN, GeneticModel

######  LOGGER  ######
from ProcessLogger import ProcessLogger

######  PARALLELIZATION  ######
from multiprocessing import Pool, cpu_count

######  REGRESSION  ######
import statsmodels.api as sm
from patsy import dmatrices

class DataFormat(enum.Enum):
    BGEN = 0

class RegressionFamily(enum.Enum):
    Binomial = sm.families.Binomial()

class BGENData:
    def __init__(self, path, genetic_model, samples):
        self.__path = path
        self.__samples = samples
        if genetic_model.lower() == "additive":
            self.__genetic_model = GeneticModel.Additive
        elif genetic_model.lower() == "dominant":
            self.__genetic_model = GeneticModel.Dominant
        elif genetic_model.lower() == "recessive":
            self.__genetic_model = GeneticModel.Recessive

    def read(self, seeks):
        bgen_iter = PyBGEN(
            self.__path,
            _skip_index=True,
            genetic_model=self.__genetic_model
        ).iter_seeks(seeks)

        for info, dosage in bgen_iter:
            snp_id = str(int(info.chrom)) + ":" + str(int(info.pos)) + "_" + str(info.a1) + "_" + str(info.a2)
            df = pd.DataFrame({'id': self.__samples, 'dosage': dosage})
            yield snp_id, df

def __extract(snp_id, result):

    summary = result.summary()
    extracted_result = {}

    # First table - contain general statistics and data about the regression
    tbl_1 = pd.read_html(summary.tables[0].as_html())[0]
    tbl_1_dict = {}
    for i in range(0, 4, 2):
        columns = tbl_1[i].tolist()
        data = tbl_1[i + 1].tolist()
        for col_idx in range(len(columns)):
            if columns[col_idx] != columns[col_idx]:
                continue

            col = columns[col_idx].replace(":", "")
            if not [
                "Dep. Variable",
                "Time",
                "Date",
                "No. Iterations",
                "No. Observations",
                "Df Residuals",
                "Df Model",
                "Scale",
                "Log-Likelihood",
                "Deviance",
                "Pearson chi2"
            ].__contains__(col):
                continue

            value = data[col_idx]
            if col == "Dep. Variable":
                value = value.strip("[]").replace("'", "")
            if col == "No. Iterations" or col == "No. Observations":
                value = int(value)
            tbl_1_dict[col.replace(".", "").replace(" ", "_").lower()] = [value]

    df = pd.DataFrame(tbl_1_dict)
    df["id"] = [snp_id]

    extracted_result["reg_details"] = df

    tbl_2_vars = pd.read_html(
        summary.tables[1].as_html(),
        header=0,
        index_col=0
    )[0]

    tbl_2_vars.columns = [col.replace(".", "").replace(" ", "_").lower() for col in tbl_2_vars.columns]

    for var in list(tbl_2_vars.index):
        df = tbl_2_vars.loc[[var]]
        df.rename(columns={"p>|z|": "p", "[0025": "low_conf_int", "0975]": "high_conf_int"}, inplace=True)
        df["id"] = [snp_id]
        df["p"] = result.pvalues[var]
        df["coef"] = result.params["dosage"]
        conf_int = result.conf_int(alpha=0.05)
        df["low_conf_int"] = conf_int[0]
        df["high_conf_int"] = conf_int[1]
        extracted_result[var] = df

    return extracted_result

def __glm_fit(dataObject, covariates, model, family, seeks):
    batch_results = {}
    data_iter = dataObject.read(seeks)
    for snp_id, dosage_data in data_iter:
        df = pd.merge(dosage_data,
                      covariates, on="id", how="inner")
        df = df[df["dosage"].notnull()]

        y, X = dmatrices(model,
                         data=df,
                         return_type='dataframe')

        model = sm.GLM(y, X, family=family)
        result = model.fit()
        extracted = __extract(snp_id, result)

        for key in list(extracted):
            if key not in list(batch_results):
                batch_results[key] = extracted[key]
            else:
                batch_results[key] = batch_results[key].append(extracted[key])

    return batch_results

def parallel_glm(model, data_path, seeks, format, covariates, family, cores=cpu_count(), samples=None, genetic_model="additive"):

    if format == DataFormat.BGEN:
        data = BGENData(data_path, genetic_model, samples)

    pool = Pool(cores)
    analysis = partial(
        __glm_fit,
        data,
        covariates,
        model,
        family
    )

    results = pool.map(
        analysis,
        [seeks[_::cores] for _ in range(cores)]
    )

    combined_result = {}
    for result in results:
        for key in list(result):
            if key not in list(combined_result):
                combined_result[key] = result[key]
            else:
                combined_result[key] = combined_result[key].append(result[key])
    return combined_result




