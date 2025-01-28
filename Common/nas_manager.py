import os
import pathlib
import traceback
from zipfile import ZipFile

from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import nas_manager_logger as logger


class NasManager:
    def __init__(self, dataset_id):
        logger.info(f"NAS 관리자 생성 --> dataset_id:{dataset_id}")
        self.dataset_id = dataset_id
        self.env = settings["DEPLOY_MODE"]
        self.mnt_prefix = settings["NFS_MNT_PREFIX"]

    def create_dir(self, type):
        try:
            dir_path = f"{self.mnt_prefix}/{self.env}/{self.dataset_id}/{type}"
            pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)
            return dir_path
        except:
            logger.error(f"{self.dataset_id} 의 [{type}] 디렉토리 생성 실패")
            logger.error(traceback.format_exc())
            return None

    def delete_target_file(self, file_path):
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"{file_path} 를 삭제하였습니다.")
            else:
                logger.info(f"{file_path} 가 존재하지 않습니다.")
            pass
        except:
            logger.error(f"{file_path} 의 삭제 실패")
            logger.error(traceback.format_exc())
            return None
        pass

    def make_zip_file(self, zip_type):
        """
        zip_type 은 "original_dataset_zip" , "azoo_product_zip" 이 있다.
        """
        try:
            if zip_type == "original_dataset_zip":

                output_dir_path = self.create_dir(type="zip_original_dataset")
                target_parent_path = (
                    f"{self.mnt_prefix}/{self.env}/{self.dataset_id}/original_dataset"
                )
                zip_output_file_path = f"{output_dir_path}/{self.dataset_id}.zip"

                if output_dir_path is None:
                    raise Exception(f"{self.dataset_id}-{zip_type} 디렉토리 생성 실패!")

                task_result = self.__make_zip_file(
                    zip_output_file_path=zip_output_file_path,
                    target_parent_dir=target_parent_path,
                )

                if task_result is None or task_result == False:
                    raise Exception(f"{self.dataset_id}-{zip_type} zip 파일 생성 실패!")

                logger.info(
                    f"{self.dataset_id} 의 {zip_type}이 생성되었습니다. --> {zip_output_file_path}"
                )
                return True
            elif zip_type == "azoo_product_zip":
                return True
            pass
        except:
            logger.error(f"{self.dataset_id} 의 {zip_type} 파일 생성 실패")
            logger.error(traceback.format_exc())
            return None
        pass

    def __make_zip_file(self, zip_output_file_path, target_parent_dir):
        try:
            # with ZipFile('E:/Zipped file.zip', 'w') as zip_object:
            with ZipFile(zip_output_file_path, "w") as zip_object:
                for folder_name, sub_folders, file_names in os.walk(target_parent_dir):
                    for filename in file_names:
                        # Create filepath of files in directory
                        file_path = os.path.join(folder_name, filename)

                        if filename.endswith(".zip"):
                            # Original Dataset 에 zip파일이 들어있는 상황 --> 제외
                            continue

                        # Add files to zip file
                        # zip_object.write(file_path, os.path.basename(file_path))
                        arcname = os.path.relpath(file_path, target_parent_dir)
                        zip_object.write(file_path, arcname)
                        pass

            # Check to see if the zip file is created
            if os.path.exists(zip_output_file_path):
                logger.info("ZIP file created")
                return True
            else:
                logger.error("ZIP file not created")
                return False
            pass
        except:
            logger.error("ZIP 압축 실패")
            logger.error(traceback.format_exc())
            return None
        pass

    # def zip_directory_exclude(self, directory_path, zip_filename, exclude_files):
    #     with ZipFile(zip_filename, 'w') as zipf:
    #         for foldername, subfolders, filenames in os.walk(directory_path):
    #             for filename in filenames:
    #                 file_path = os.path.join(foldername, filename)
    #                 if any(exclude in file_path for exclude in exclude_files):
    #                     continue
    #                 arcname = os.path.relpath(file_path, directory_path)
    #                 zipf.write(file_path, arcname)

    pass
