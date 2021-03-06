from AnalysisManager import AnalysisManager
from ResultManager import ResultManager

class ServiceManager:
    def __init__(self, input, output, chromosomes, verbose=False):
        self.result_manager = ResultManager(output)
        exclude = self.result_manager.get_stored_ids(chromosomes)
        self.analysis_manager = AnalysisManager(input)
        snps_to_analyse = self.analysis_manager.prepare_data(chromosomes, snps_to_exclude=exclude)
        self.result_manager.run(snps_to_analyse)

    def get_batch(self, size):
        return self.analysis_manager.get_batch(size)

    def get_supplementary_data(self, name):
        return self.analysis_manager.get_supplementary_data(name)

    def store_result(self, result):
        self.result_manager.store_result(result)