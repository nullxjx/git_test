import os
import sys
from utils.file_util import check_file_exist_and_size, check_folder_exist_and_size
from apis import HTTPAPI, GrpcClient
from builtin import BuiltinFunctions
sys.path.append("../utils")


# 原始文件对象
# 管理原始文件的目的是确认和数据行的关系，确保数据行准确性，当数据行出现明显错误时，比如大量行数据缺失，可以找到原始数据进行审查
class RawFile:
    def __init__(self, dataset_id, raw_id, http_service: HTTPAPI, grpc_client: GrpcClient, name=None):
        self.http_api_service = http_service
        self.grpc_client = grpc_client
        self.dataset_id = dataset_id
        self.id = raw_id
        self.name = name

    def _check_raw_data_exists(self):
        pass

    def generate_data_row_by_builtin_func(self, filename, func: str, upload=False):
        if upload:
            upload_response = self.upload_single_file(filename)
            print(upload_response)

        # 使用纯 python 来增强兼容性
        os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
        builtin_func = BuiltinFunctions.get(func)
        if not builtin_func:
            raise ValueError(f"Invalid builtin function: {func}")
        job_func = builtin_func(filename, self.dataset_id, self.id, self.grpc_client.get_grpc_channel)
        job_func()

    def upload_folder_dataset(self, folder_dir, samples, upload=False):
        """
        apps数据集和huggingface数据集都可以用这个接口，他们都是一个目录下存在多行数据的数据集
        :param folder_dir:
        :param samples:
        :param upload:
        :return:
        """
        if upload:
            self.upload_folder(folder_dir)
        # 使用纯 python 来增强兼容性
        os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
        builtin_func = BuiltinFunctions.get("floder_dataset")
        if not builtin_func:
            raise ValueError(f"Invalid builtin function: floder_dataset")
        job_func = builtin_func(folder_dir, samples, self.dataset_id, self.id, self.grpc_client.get_grpc_channel)
        job_func()

    def upload_samples_with_single_file(self, file_path, samples, upload=False):
        """
        上传所有sample在同一个文件的数据集
        :param file_path: 文件路径
        :param samples:
        :param upload:
        :return:
        """
        if upload:
            self.upload_single_file(file_path)
        # 使用纯 python 来增强兼容性
        os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
        builtin_func = BuiltinFunctions.get("floder_dataset")
        if not builtin_func:
            raise ValueError(f"Invalid builtin function: floder_dataset")
        job_func = builtin_func("", samples, self.dataset_id, self.id, self.grpc_client.get_grpc_channel)
        job_func()

    def get_data_row_count(self):
        return self.http_api_service.get_raw_file_row_count(self.dataset_id, self.id).json()

    def upload_single_file(self, file: str):
        files_exist, error_msg = check_file_exist_and_size(file)
        if not files_exist:
            raise ValueError(error_msg)
        return self.http_api_service.upload_single_file(self.dataset_id, self.id, file).json()

    def upload_folder(self, folder: str):
        exist, error_msg = check_folder_exist_and_size(folder)
        if not exist:
            raise ValueError(error_msg)
        prefix = folder.split("/")[-1]
        return self.http_api_service.upload_folder(self.dataset_id, self.id, folder, prefix)
