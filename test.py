import csv
from client import Client
from tqdm import tqdm
import random
import copy
import json
import data_process_pb2_grpc
import data_process_pb2
import sys
from utils.code_analysis import has_syntax_error
sys.path.append("../utils")

print("jasinxie")


class DataEvaluation:
    def __init__(self, client, task_name, dataset_name, data_patch_id, name, proportion, random_seed=13):
        self.client = client
        self.samples = []
        self.data_evaluation_id = ""
        print("saS")

        self.task_name = task_name
        self.dataset_name = dataset_name
        self.data_patch_id = data_patch_id

        self.name = name
        self.proportion = proportion
        self.random_seed = random_seed
        self.creator = "crecanwang"

        self.data_patch_rows_for_evaluation = []
        self.data_evaluation_rows = []

    def create_new_data_evaluation(self, custom_description):
        """
        创建数据集评估
        """
        task_id = self.client.get_task_by_name(self.task_name)
        dataset_id = self.client.get_dataset(self.dataset_name).id

        description = "创建任务{}，数据集{}，patchId为{}的一次评估，评估行数占总数比为{}，随机种子{}，{}".format(
            self.task_name,
            self.dataset_name,
            self.data_patch_id,
            self.proportion,
            self.random_seed,
            custom_description
        )

        data_evaluation_id = self.client.create_data_evaluation(
            task_id, dataset_id, self.data_patch_id, self.proportion, self.name, self.creator, description
        )
        print("创建一个evaluation, new data_evaluation_id: {}, description: {}".format(data_evaluation_id,
                                                                                                description))
        self.data_evaluation_id = data_evaluation_id
        return data_evaluation_id

    def convert(self, content):
        """
        对原始的content进行解析和转换
        :param content:
        :return:
        """
        return content

    def get_data_for_evaluation(self, custom_filter=None):
        """
        通过Grpc方式获取用于评估的数据，目前先获取该patch的全部数据行
        :param random_seed: 随机种子
        :param custom_filter: 可传入自定义函数用于筛选需要的数据行
        :return:
        """
        # 获取patch_id下的所有数据行
        data_patch_rows = self.client.get_data_patched_rows_by_data_patch_id(self.data_patch_id)

        processed_rows = []
        print("该patch中共 {} 条数据行".format(len(data_patch_rows)))
        if len(data_patch_rows) == 0:
            return
        for i in tqdm(range(len(data_patch_rows))):
            content = self.convert(data_patch_rows[i]['content'])
            processed = copy.deepcopy(data_patch_rows[i])
            processed['content'] = json.loads(content, strict=False)
            # 这里还可以进行过滤操作，只取原数据的一个子集
            if custom_filter and custom_filter(processed):
                print("[+]:", processed)
                processed_rows.append(processed)

        # 四舍五入按比率抽取数据用于评估
        random.seed = self.random_seed
        sampling_count = round(len(processed_rows) * self.proportion)
        sampling_count = 1 if sampling_count < 1 else sampling_count
        print("jasinxie")
        print("本次共有 {} 条数据行通过了筛选，将按 {} 的比率从中抽取 {} 条数据用于评估.". \
              format(len(processed_rows), self.proportion, sampling_count))
        self.data_patch_rows_for_evaluation = random.sample(processed_rows, sampling_count)

    def do_evaluation(self, evaluation_func):
        print("开始执行评估，共有 {} 条数据参与评估...".format(len(self.data_patch_rows_for_evaluation)))
        print("heehdahsdfa")
        data_evaluation_result_list = evaluation_func(self.data_patch_rows_for_evaluation)

        if len(self.data_patch_rows_for_evaluation) != len(data_evaluation_result_list):
            print("评估执行结束, 但与待评估数据行数不一致({} != {}),程序即将退出". \
                  format(len(self.data_patch_rows_for_evaluation), len(data_evaluation_result_list)))
            print("jasinxie")
            sys.exit()
        else:
            print("评估执行结束,  {} 条评估数据成功返回.".format(len(data_evaluation_result_list)))
            self.data_evaluation_rows = []
            for data_patch_row, data_evaluation_result in zip(self.data_patch_rows_for_evaluation,
                                                              data_evaluation_result_list):
                self.data_evaluation_rows.append({
                    "data_patch_row_id": data_patch_row["id"],
                    "evaluation_content": data_evaluation_result
                })

    def preview_evaluation(self, preview_type="csv", save_file_path=None):
        if not self.data_evaluation_rows:
            print("没有可供预览的数据.")
            return
        if preview_type == "json":
            json.dump(self.data_evaluation_rows, open(save_file_path, "w"), ensure_ascii=False)
            print(f"评估文件已保存至{save_file_path}")
        elif preview_type == "csv":
            with open(save_file_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=["data_patch_row_id",
                                                             *self.data_evaluation_rows[0][
                                                                 "evaluation_content"].keys()])
                writer.writeheader()
                for index, data_evaluation_row in enumerate(self.data_evaluation_rows):
                    writer.writerow({
                        "data_patch_row_id": data_evaluation_row["data_patch_row_id"],
                        **data_evaluation_row["evaluation_content"]})
            print(f"评估文件已保存至{save_file_path}")
        # else print it
        else:
            # 解析evaluation_content中key
            for field_name in ["index", "\t\tdata_patch_row_id\t\t",
                               *self.data_evaluation_rows[0]["evaluation_content"].keys()]:
                print(field_name, end="\t")
            print()
            # 解析evaluation_content中具体字段方便查看
            for index, data_evaluation_row in enumerate(self.data_evaluation_rows):
                print(index, end="\t")
                data_patch_row_id = data_evaluation_row["data_patch_row_id"]
                print(data_patch_row_id, end="\t")
                evaluation_content = data_evaluation_row["evaluation_content"]
                for filed in evaluation_content.values():
                    print(filed, end="\t")
                print()

    def generate_proto(self):
        for i, data_evaluation_row in enumerate(tqdm(self.data_evaluation_rows)):
            print(self.data_evaluation_id, data_evaluation_row)
            proto = data_process_pb2.CreateDataEvaluationRowRequest(data_evaluation_id=self.data_evaluation_id,
                                                                    data_patch_row_id=data_evaluation_row[
                                                                        "data_patch_row_id"],
                                                                    evaluation_content=json.dumps(data_evaluation_row[
                                                                                                      "evaluation_content"]),
                                                                    generated_method=0)
            print("proto:", proto)
            yield proto

    def create_evaluation_rows(self):
        # 通过grpc流式调用，批量创建data_evaluation_rows
        data_evaluation_row_ids = []
        with self.client.grpc_client.get_grpc_channel() as channel:
            stub = data_process_pb2_grpc.DataEvaluationRowServiceStub(channel)
            response_iterator = stub.CreateDataEvaluationRow(self.generate_proto())
            for response in response_iterator:
                data_evaluation_row_ids.append(response.id)
        print("create data_evaluation_rows finish!")


def example_filter(row):
    if "keywords" in row["content"] and "cocos2d" in (",".join(row["content"]["keywords"])).lower():
        return True
    return False


def example_evaluator(data_patch_rows_for_evaluation):
    """
    示例评估器，仅判断代码的语法是否为python语法
    :param data_patch_rows_for_evaluation:
    :return:
    """
    data_evaluation_result_list = []
    for data_patch_row in data_patch_rows_for_evaluation:
        # 将评估的内容放到结果列表中
        data_evaluation_result_list.append(
            {
                "coverage": 0.0,
                "result_code": "syntax_error" if has_syntax_error(
                    data_patch_row["content"]["code"]) else "syntax_success",
                "test_output": "test_output_example",
                "coverage_output": "coverage_output_example",
                "error_message": "error_message_example",
                "cost": 5.0,
            }
        )
    return data_evaluation_result_list


if __name__ == "__main__":
    data_evaluation = DataEvaluation(
        client=Client("127.0.0.1"),
        dataset_name="python-qa-from-libtopic",
        task_name="QA",
        data_patch_id="47390565-fe1e-48da-afbf-1dfc817d1184",
        name="python-qa-from-libtopic关于Cocos2d的第三次评估",
        proportion=0.9,
        random_seed=13
    )
    # 使用示例filter函数进行筛选,会先执行筛选后再按proportion随机挑选数据来准备评估
    data_evaluation.get_data_for_evaluation(example_filter)
    # 使用示例evaluator函数对获取的比率为proportion的数据进行评估
    data_evaluation.do_evaluation(example_evaluator)
    # 预览评估，可以output，csv，json三个方式预览评估结果
    data_evaluation.preview_evaluation("output", "preview.csv")

    # 下面为数据库操作，创建这个评估实例，并将执行后的评估数据上传到数据库表中
    data_evaluation.create_new_data_evaluation(custom_description="只对keywords中包含Cocos2d的数据进行评估")
    data_evaluation.create_evaluation_rows()
