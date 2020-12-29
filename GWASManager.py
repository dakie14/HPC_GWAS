from AnalysisManager import AnalysisManager
from ResultManager import ResultManager

class GWASManager:
    def __init__(self, data_path, result_path):
        self.analysis_manager = AnalysisManager(data_path)
        self.result_manager = ResultManager(result_path)

    def get_batch(self, size):
        return self.analysis_manager.get_batch(size)

    def get_covariates(self):
        return self.analysis_manager.get_covariates()

    def store_result(self, result):
        self.result_manager.store_result(result)

