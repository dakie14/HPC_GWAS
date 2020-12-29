######  GENERAL  ######
from ToolBox import compress
from DBManager import DBManager

######  LOGGING  ######
from ProcessLogger import ProcessLogger

class AnalysisManager:
    def __init__(self, data_path):
        self.__logger = ProcessLogger("__ResultManager__")
        self.__dbm = DBManager(data_path)
        self.__chromosomes = []
        self.__seeks = {}
        self.__covariates = self.__dbm.get_data("covariates")

    def prepare_data(self, chromosomes, snps_to_exclude=None):
        for c in chromosomes:
            seeks = self.__dbm.get("variants_chr" + str(c),
                                   columns=["seek"],
                                   predicate="id not in " + str(tuple(snps_to_exclude[str(c)])) if snps_to_exclude else None)["seek"].to_list()
            if len(seeks) > 0:
                self.__chromosomes.append(str(c))
                self.__seeks[str(c)] = (0, seeks) # 0 is where to start in the list when returning batches

    def get_covariates(self):
        return self.__covariates

    def get_batch(self, size):
        if len(self.__chromosomes) == 0:
            # No more data to analyse, so send stop signal to workers
            return compress([])

        i, seeks = self.__seeks[str(self.__chromosomes[0])]
        batch = seeks[i:i+size]
        self.__seeks[str(self.__chromosomes[0])] = (i+size, seeks)

        if len(batch) == 0:
            # Remove first chromosome as no more data is available for that one
            self.__chromosomes = self.__chromosomes[1:]
            return self.get_batch(size)

        batch = {"chromosome": self.__chromosomes[0], "data": batch}

        # Rotate list of chromosomes
        self.__chromosomes = self.__chromosomes[1:] + self.__chromosomes[0]

        return compress(batch)

