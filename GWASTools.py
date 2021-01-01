######  GENERAL  ######
import pandas as pd
from functools import partial
from itertools import chain
import os
import signal

######  DATA_READERS  ######
from PyBGEN import PyBGEN
from Enumerators import Model as gm
from Enumerators import Family

######  LOGGER  ######
from ProcessLogger import ProcessLogger

######  PARALLELIZATION  ######
from multiprocessing import Pool, cpu_count

######  REGRESSION  ######
import statsmodels.api as sm
from patsy import dmatrices

logger = ProcessLogger("h")

class GenotypeData(object):
    def __init__(self, path, seeks, genetic_model=gm.Additive, samples=None):
        self.__samples = samples
        self._genetic_model = genetic_model
        self.__path = path
        self.__seeks = seeks
        self.__data_iter = None

    def __open_bgen(self, path, genetic_model, seeks):
        return PyBGEN(
            path,
            _skip_index=True,
            genetic_model=genetic_model
        ).iter_seeks(seeks)

    def get_iterator(self):
        if not self.__data_iter:
            self.__data_iter = self.__open_bgen(self.__path, self._genetic_model, self.__seeks)

        for info, dosage in self.__data_iter:
            snp_id = str(int(info.chrom)) + ":" + str(int(info.pos)) + "_" + str(info.a1) + "_" + str(info.a2)
            df = pd.DataFrame({'id': self.__samples, 'dosage': dosage})
            yield snp_id, df

class RegressionModel:

    def get_info(self, result):
        summary = result.summary()

        # First table - contain general statistics and data about the regression
        tbl_1 = pd.read_html(summary.tables[0].as_html())[0]
        dict = {}
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
                dict[col.replace(".", "").replace(" ", "_").lower()] = [value]

        return pd.DataFrame(dict)

    def get_results(self, result):
        summary = result.summary()
        df = pd.read_html(
            summary.tables[1].as_html(),
            header=0,
            index_col=0
        )[0]

        df.columns = [col.replace(".", "").replace(" ", "_").lower() for col in df.columns]
        df.rename(columns={"p>|z|": "p", "[0025": "5_conf_int", "0975]": "95_conf_int"}, inplace=True)
        conf_int = result.conf_int(alpha=0.05)
        df["5_conf_int"] = conf_int[0]
        df["95_conf_int"] = conf_int[1]
        df["p"] = result.pvalues

        return df

class GeneralisedLinearModel(RegressionModel):

    def __init__(self, dep, predictors, family):
        self.__model = sm.GLM(dep, predictors, family=family)
        self.__info = None
        self.__fitted_model = None

    def fit(self):
        result = self.__model.fit()
        self.__info = self.get_info(result)
        self.__fitted_model = self.get_results(result)
        return self

    def info(self):
        return self.__info.copy()

    def fitted_model(self):
        return self.__fitted_model.copy()

class ParallelResult:
    def __init__(self):
        self.__result = {}

    def add(self, _id, model):
        info = model.info()
        info["id"] = [_id]
        if "info" not in self.__result:
            info.reset_index(drop=True, inplace=True)
            self.__result["info"] = info
        else:
            self.__result["info"] = self.__result["info"].append(info, ignore_index=True)

        result = model.fitted_model()
        result["id"] = [_id for _ in range(len(result))]
        for idx in result.index:
            df = result[idx:idx]
            df.reset_index(drop=True, inplace=True)
            if idx not in self.__result:
                self.__result[idx] = df
            else:
                self.__result[idx] = self.__result[idx].append(result[idx:idx], ignore_index=True)

    def combine(self, results):
        for _id, result in list(chain.from_iterable(results)):
            self.add(_id, result)

    def get(self, identifier):
        if identifier in self.__result:
            return self.__result[identifier]
        return None

    def as_dict(self):
        return self.__result

def __glm_fit(covariates, model, family, data):
    batch_results = []
    for _id, dosage_data in data.get_iterator():
        df = pd.merge(dosage_data,
                      covariates, on="id", how="inner")
        df = df[df["dosage"].notnull()]

        y, X = dmatrices(model,
                         data=df,
                         return_type='dataframe')

        glm = GeneralisedLinearModel(y, X, sm.families.Binomial() if family == Family.Binomial else None)
        glm.fit()
        batch_results.append((_id, glm))

    return batch_results

def parallel_glm(reg_model, data_path, covariates, family, seeks, cores=cpu_count(), samples=None, genetic_model=gm.Additive):

    parallel_result = ParallelResult()

    if len(seeks) == 0:
        return parallel_result

    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pool = Pool(cores)
    signal.signal(signal.SIGINT, original_sigint_handler)

    analysis = partial(
        __glm_fit,
        covariates,
        reg_model,
        family
    )

    try:
        results = pool.map(
            analysis,
            [GenotypeData(data_path, batch, genetic_model=genetic_model, samples=samples) for batch in [seeks[_::cores] for _ in range(cores)]]
        )
    except KeyboardInterrupt:
        pool.terminate()
    else:
        pool.close()
        pool.join()
        parallel_result.combine(results)
    return parallel_result




