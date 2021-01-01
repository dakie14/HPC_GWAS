######  GENERAL  ######
from ToolBox import compress
from DatabaseManager import DatabaseManager

######  LOGGING  ######
from ProcessLogger import ProcessLogger

class AnalysisManager:
    def __init__(self, data_path):
        self.__logger = ProcessLogger("__ResultManager__")
        self.__dbm = DatabaseManager(data_path)
        self.__chromosomes = []
        self.__seeks = {}
        self.__buffered_data = {}

    def prepare_data(self, chromosomes, snps_to_exclude={}):
        snp_count = 0
        for c in chromosomes:
            seeks = self.__dbm.get(
                "variants_chr" + str(c),
                columns=["seek"],
                predicate="id not in " + str(tuple(snps_to_exclude[c])) if c in snps_to_exclude else None
            )["seek"].to_list()
            if len(seeks) > 0:
                self.__seeks[str(c)] = seeks
                self.__chromosomes.append(c)
                snp_count += len(seeks)

        return snp_count

    def get_supplementary_data(self, name):
        if name not in self.__buffered_data:
            self.__buffered_data[name] = self.__dbm.get(name)
        return self.__buffered_data[name]

    def get_batch(self, size):
        if len(self.__chromosomes) == 0 or size == 0:
            # No more data to analyse, so send stop signal to workers
            return compress({"chr": -1, "data": []})

        seeks = self.__seeks[str(self.__chromosomes[0])]
        batch = seeks[:size]
        self.__seeks[str(self.__chromosomes[0])] = seeks[size:]

        if len(batch) == 0:
            # Remove first chromosome as no more data is available for that one
            self.__chromosomes = self.__chromosomes[1:]
            return self.get_batch(size)

        batch = {"chr": self.__chromosomes[0], "data": batch}

        # Rotate list of chromosomes
        self.__chromosomes = self.__chromosomes[1:] + self.__chromosomes[:1]

        return compress(batch)
