######  GENERAL  ######
from ToolBox import compress
from DatabaseManager import DatabaseManager

######  LOGGING  ######
from ProcessLogger import ProcessLogger

class AnalysisManager:
    def __init__(self, data_path, chromosomes, snps_to_exclude=None):
        self.__logger = ProcessLogger("__ResultManager__")
        self.__dbm = DatabaseManager(data_path)
        self.__chromosomes = chromosomes
        self.__seeks = {}
        self.__buffered_data = {}

        self.__prepare_data(snps_to_exclude)

    def __prepare_data(self, snps_to_exclude):
        for c in self.__chromosomes:
            seeks = self.__dbm.get(
                "variants_chr" + str(c),
                columns=["seek"],
                predicate="id not in " + str(tuple(snps_to_exclude)) if snps_to_exclude else None
            )["seek"].to_list()
            if len(seeks) > 0:
                self.__seeks[str(c)] = seeks

    def get_supplementary_data(self, name):
        if name not in self.__buffered_data:
            self.__buffered_data[name] = self.__dbm.get(name)
        return self.__buffered_data[name]

    def get_batch(self, size):
        if len(self.__chromosomes) == 0:
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
