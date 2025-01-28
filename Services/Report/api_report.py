import time
import traceback
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.jwt import JWTBearer
from Common.report_parser import (report_thres_parser_v1 , report_thres_parser_v2 , 
                                  report_metrics_parser_v1 , report_metrics_parser_v2)
from Crud import genarating_synthetic_tasks
from Model.schema import (
    ReturnResultDictSchema,
    ReturnMessageSchema,
)
from Settings.Database.database import async_get_db
from Settings.Logger.logging_config import fastapi_logger as logger

# from sqlalchemy.sql.expression import func


router = APIRouter(
    prefix="/report",
)


@router.get(
    path="/evaluation",
    tags=["REPORT"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultDictSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_get_evaluation(
    azoo_product_id: int = 0,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):
    """
    Get Report Info
    """
    try:
        begin = time.time()
        end_point = f"{router.prefix}/evaluation"

        dataset_type, eval_json_value = await genarating_synthetic_tasks.read(
            db=db,
            params={"from": "api_get_evaluation()", "azoo_product_id": azoo_product_id},
        )
        if dataset_type is None:
            return JSONResponse(
                status_code=412,
                content={
                    "message": f"There is not the report info. of the azoo_product_id : {azoo_product_id} "
                },
            )
        """
        {"args": {}, "time": {}, "task_ids": {}, "filtering": {}, "evaluation": {"total-full": {}, "safety-full": {}, "total-filtered": {"safety": {"SSIM": 0.08655833142910255, "LPIPS": 0.6165686845779419}, "utility": {"KID": 0.04962127675486809, "Indistinguishability": {"value": 0.08750000000000002, "false_syn": 0.625, "false_real": 0.55}, "Downstream Classification Accuracy Rate": 1.0}}, "safety-filtered": {}}, "thresholds": {}}
        """        
        try:
            response_data = report_metrics_parser_v2(end_point=end_point , azoo_product_id=azoo_product_id , dataset_type=dataset_type , eval_json_value=eval_json_value)
        except:
            response_data = report_metrics_parser_v1(end_point=end_point , azoo_product_id=azoo_product_id , dataset_type=dataset_type , eval_json_value=eval_json_value)

        # response_data = dict()
        # total_filtered = eval_json_value["evaluation"]["total-filtered"]
        # downstream = total_filtered["downstream"]
        # safety = total_filtered["safety"]
        # utility = total_filtered["utility"]

        # if dataset_type.lower() == "image":  #### genarating ID:40
        #     try: # 컨플루언스에 올라 온 최신 구조                
        #         response_data["downstream_classification"] = downstream["Accuracy"]["Accuracy Rate"]
        #         response_data["kid"] = utility["Quality"]["KID"]
        #         response_data["one_class_classification"] = utility["Indistinguishability"]["OCC Accuracy"]["value"]
        #         response_data["lpips"] = safety["Perceptual Similarity"]["LPIPS"]
        #         response_data["ssim"] =  safety["Structural Similarity"]["SSIM"]
        #         pass
        #     except: # 이전에 사용하던 구조
        #         response_data = report_metrics_parser_v1(end_point=end_point , azoo_product_id=azoo_product_id , dataset_type=dataset_type , eval_json_value=eval_json_value)
        #         response_data["downstream_classification"] = eval_json_value["evaluation"][
        #             "total-filtered"
        #         ]["utility"]["Downstream Classification Accuracy Rate"]
        #         response_data["kid"] = eval_json_value["evaluation"]["total-filtered"][
        #             "utility"
        #         ]["KID"]
        #         response_data["one_class_classification"] = eval_json_value["evaluation"][
        #             "total-filtered"
        #         ]["utility"]["Indistinguishability"]["value"]
        #         response_data["lpips"] = eval_json_value["evaluation"]["total-filtered"][
        #             "safety"
        #         ]["LPIPS"]
        #         response_data["ssim"] = eval_json_value["evaluation"]["total-filtered"][
        #             "safety"
        #         ]["SSIM"]                

        # elif dataset_type.lower() == "tabular":   #### genarating ID:9
        #     try:
        #         response_data["downstream_classification"] = downstream["Accuracy"]["Accuracy Rate"]
        #         response_data["mmd"] = utility["Quality"]["MMD"]
        #         response_data["entropy"] = utility["Diversity"]["Entropy"]
        #         response_data["two_d_correlation_similarity"] = utility["Quality"]["2D Correlation Similarity"]
        #         response_data["one_class_classification"] = utility["Indistinguishability"]["OCC Accuracy"]["value"]
        #         response_data["duplication_rate"] = utility["Duplication"]["Duplication Rate"]
        #         response_data["identification_risk"] = safety["Structural Similarity"]["Identification Risk"]
        #         response_data["linkage_risk"] = safety["Perceptual Similarity"]["Linkage Risk"]
        #         response_data["inference_risk"] = safety["Perceptual Similarity"]["Inference Risk"]
        #     except:
        #         response_data["downstream_classification"] = eval_json_value["evaluation"][
        #         "total-filtered"
        #         ]["utility"]["Downstream Classification Accuracy Rate"]

        #         response_data["mmd"] = eval_json_value["evaluation"]["total-filtered"][
        #             "utility"
        #         ]["MMD"]

        #         response_data["entropy"] = eval_json_value["evaluation"]["total-filtered"][
        #             "utility"
        #         ]["Entropy"]

        #         response_data["two_d_correlation_similarity"] = eval_json_value[
        #             "evaluation"
        #         ]["total-filtered"]["utility"]["2D Correlation Similarity"]

        #         response_data["one_class_classification"] = eval_json_value["evaluation"][
        #             "total-filtered"
        #         ]["utility"]["Indistinguishability"]["value"]

        #         response_data["duplication_rate"] = eval_json_value["evaluation"][
        #             "total-filtered"
        #         ]["utility"]["Duplicated Rate"]                

        #         response_data["identification_risk"] = eval_json_value["evaluation"][
        #             "total-filtered"
        #         ]["safety"]["Identification Risk"]

        #         response_data["linkage_risk"] = eval_json_value["evaluation"][
        #             "total-filtered"
        #         ]["safety"]["Linkage Risk"]

        #         response_data["inference_risk"] = eval_json_value["evaluation"][
        #             "total-filtered"
        #         ]["safety"]["Inference Risk"]

        # elif dataset_type.lower() == "text":  #### genarating ID:35
        #     try:
        #         response_data["downstream_classification"] = downstream["Accuracy"]["Accuracy Rate"]
        #         response_data["mmd"] = utility["Quality"]["MMD"]
        #         response_data["one_class_classification"] = utility["Indistinguishability"]["OCC Accuracy"]["value"]
        #         response_data["rouge"] = safety["Structural Similarity"]["ROUGE"]
        #         response_data["bert_score"] = safety["Perceptual Similarity"]["BERTScore"]
        #     except:
        #         response_data["downstream_classification"] = eval_json_value["evaluation"][
        #             "total-filtered"
        #         ]["utility"]["Downstream Classification Accuracy Rate"]

        #         response_data["mmd"] = eval_json_value["evaluation"]["total-filtered"][
        #             "utility"
        #         ]["MMD"]

        #         response_data["one_class_classification"] = eval_json_value["evaluation"][
        #             "total-filtered"
        #         ]["utility"]["Indistinguishability"]["value"]

        #         response_data["rouge"] = eval_json_value["evaluation"]["total-filtered"][
        #             "safety"
        #         ]["ROUGE"]

        #         response_data["bert_score"] = eval_json_value["evaluation"][
        #             "total-filtered"
        #         ]["safety"]["BERTScore"]

        end = time.time()
        result = {
            "end_point": end_point,
            "result": {"data_type": dataset_type.lower(), "report_data": response_data},
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        logger.info(f"Result:{result} // azoo_product_id:{azoo_product_id}")

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        logger.error(traceback.format_exc())

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res


@router.get(
    path="/eval_and_thres",
    tags=["REPORT"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultDictSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_get_eval_and_thres(
    azoo_product_id: int = 0,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):
    """
    Get Premium Report Info
    """
    try:
        begin = time.time()
        end_point = f"{router.prefix}/eval_and_thres"

        dataset_type, eval_json_value = await genarating_synthetic_tasks.read(
            db=db,
            params={
                "from": "api_get_eval_and_thres()",
                "azoo_product_id": azoo_product_id,
            },
        )
        if dataset_type is None:
            return JSONResponse(
                status_code=412,
                content={
                    "message": f"There is not the report info. of the azoo_product_id : {azoo_product_id}"
                },
            )
        """
        {"args": {}, "time": {}, "task_ids": {}, "filtering": {}, "evaluation": {"total-full": {}, "safety-full": {}, "total-filtered": {"safety": {"SSIM": 0.08655833142910255, "LPIPS": 0.6165686845779419}, "utility": {"KID": 0.04962127675486809, "Indistinguishability": {"value": 0.08750000000000002, "false_syn": 0.625, "false_real": 0.55}, "Downstream Classification Accuracy Rate": 1.0}}, "safety-filtered": {}}, "thresholds": {}}
        """

        try:
            response_data = report_metrics_parser_v2(end_point=end_point , azoo_product_id=azoo_product_id , dataset_type=dataset_type , eval_json_value=eval_json_value)
        except:
            response_data = report_metrics_parser_v1(end_point=end_point , azoo_product_id=azoo_product_id , dataset_type=dataset_type , eval_json_value=eval_json_value)

        try:
            thres_data = report_thres_parser_v1(end_point=end_point , azoo_product_id=azoo_product_id , dataset_type=dataset_type , eval_json_value=eval_json_value)
        except:            
            thres_data = report_thres_parser_v2(end_point=end_point , azoo_product_id=azoo_product_id , dataset_type=dataset_type , eval_json_value=eval_json_value)
            if thres_data is None: # threshold 데이터가 존재하지 않음
                thres_data = dict()
                thres_data["threshold"] = {}
         
        response_data.update(thres_data)
        
        # response_data = dict()        
        # thresholds = eval_json_value["thresholds"]
        # total_filtered = eval_json_value["evaluation"]["total-filtered"]
        # downstream = total_filtered["downstream"]
        # safety = total_filtered["safety"]
        # utility = total_filtered["utility"]

        # if dataset_type.lower() == 'image':
        #     try: # 컨플루언스에 올라 온 최신 구조
        #         response_data["downstream_classification"] = downstream["Accuracy"]["Accuracy Rate"]
        #         response_data["kid"] = utility["Quality"]["KID"]
        #         response_data["one_class_classification"] = utility["Indistinguishability"]["OCC Accuracy"]["value"]
        #         response_data["lpips"] = safety["Perceptual Similarity"]["LPIPS"]
        #         response_data["ssim"] =  safety["Structural Similarity"]["SSIM"]

        #     except: # 이전에 사용하던 구조
        #         response_data["downstream_classification"] = total_filtered["utility"][
        #         "Downstream Classification Accuracy Rate"
        #         ]
        #         response_data["kid"] = total_filtered["utility"]["KID"]
        #         response_data["one_class_classification"] = total_filtered["utility"][
        #             "Indistinguishability"
        #         ]["value"]
        #         response_data["lpips"] = total_filtered["safety"]["LPIPS"]
        #         response_data["ssim"] = total_filtered["safety"]["SSIM"]

        #         #### ThresHold
        #         response_data["threshold"] = dict()
        #         response_data["threshold"]["downstream_classification"] = thresholds[
        #             "Downstream Classification Accuracy Rate"
        #         ]["threshold"]
        #         response_data["threshold"]["kid"] = thresholds["KID"]["threshold"]
        #         response_data["threshold"]["one_class_classification"] = thresholds[
        #             "Indistinguishability"
        #         ]["threshold"]
        #         response_data["threshold"]["lpips"] = thresholds["LPIPS"]["threshold"]
        #         response_data["threshold"]["ssim"] = thresholds["SSIM"]["threshold"]


        # elif dataset_type.lower() == "tabular":
        #     response_data["downstream_classification"] = total_filtered["utility"][
        #         "Downstream Classification Accuracy Rate"
        #     ]
        #     response_data["mmd"] = total_filtered["utility"]["MMD"]
        #     response_data["entropy"] = total_filtered["utility"]["Entropy"]
        #     response_data["two_d_correlation_similarity"] = total_filtered["utility"][
        #         "2D Correlation Similarity"
        #     ]
        #     response_data["one_class_classification"] = total_filtered["utility"][
        #         "Indistinguishability"
        #     ]["value"]
        #     response_data["duplication_rate"] = total_filtered["utility"]["Duplicated Rate"]
        #     response_data["identification_risk"] = total_filtered["safety"][
        #         "Identification Risk"
        #     ]
        #     response_data["linkage_risk"] = total_filtered["safety"]["Linkage Risk"]
        #     response_data["inference_risk"] = total_filtered["safety"]["Inference Risk"]

        #     #### ThresHold
        #     response_data["threshold"] = dict()
        #     response_data["threshold"]["downstream_classification"] = thresholds[
        #         "Downstream Classification Accuracy Rate"
        #     ]["threshold"]
        #     response_data["threshold"]["mmd"] = thresholds["MMD"]["threshold"]
        #     response_data["threshold"]["entropy"] = thresholds["Entropy"]["threshold"]
        #     response_data["threshold"]["two_d_correlation_similarity"] = thresholds[
        #         "2D Correlation Similarity"
        #     ]["threshold"]
        #     response_data["threshold"]["one_class_classification"] = thresholds[
        #         "Indistinguishability"
        #     ]["threshold"]
        #     response_data["threshold"]["duplication_rate"] = thresholds[
        #         "Duplicated Rate"
        #     ]["threshold"]
        #     response_data["threshold"]["identification_risk"] = thresholds[
        #         "Identification Risk"
        #     ]["threshold"]
        #     response_data["threshold"]["linkage_risk"] = thresholds["Linkage Risk"][
        #         "threshold"
        #     ]
        #     response_data["threshold"]["inference_risk"] = thresholds["Inference Risk"][
        #         "threshold"
        #     ]

        # elif dataset_type.lower() == "text":
        #     response_data["downstream_classification"] = total_filtered["utility"][
        #         "Downstream Classification Accuracy Rate"
        #     ]
        #     response_data["mmd"] = total_filtered["utility"]["MMD"]
        #     response_data["one_class_classification"] = total_filtered["utility"][
        #         "Indistinguishability"
        #     ]["value"]
        #     response_data["rouge"] = total_filtered["safety"]["ROUGE"]
        #     response_data["bert_score"] = total_filtered["safety"]["BERTScore"]

        #     response_data["threshold"] = dict()
        #     response_data["threshold"]["downstream_classification"] = thresholds[
        #         "Downstream Classification Accuracy Rate"
        #     ]["threshold"]
        #     response_data["threshold"]["mmd"] = thresholds["MMD"]["threshold"]
        #     response_data["threshold"]["one_class_classification"] = thresholds[
        #         "Indistinguishability"
        #     ]["threshold"]
        #     response_data["threshold"]["rouge"] = thresholds["ROUGE"]["threshold"]
        #     response_data["threshold"]["bert_score"] = thresholds["BERTScore"][
        #         "threshold"
        #     ]

        end = time.time()
        result = {
            "end_point": end_point,
            "result": {"data_type": dataset_type.lower(), "report_data": response_data},
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        logger.info(f"Result:{result} // azoo_product_id:{azoo_product_id}")

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        logger.error(traceback.format_exc())

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res


@router.get(
    path="/complete_list",
    tags=["REPORT"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultDictSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_get_complete_list(
    user_id: int, db=Depends(async_get_db), access_token=Depends(JWTBearer())
):
    """
    Get Completed Report List
    """
    try:
        begin = time.time()
        end_point = f"{router.prefix}/complete_list"

        #### order 테이블에서, user_id 에 해당되는
        result = await genarating_synthetic_tasks.read(
            db=db, params={"from": "api_get_complete_list()", "user_id": user_id}
        )

        end = time.time()
        result = {
            "end_point": end_point,
            "result": result,
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        logger.info(f"Result:{result} // user_id:{user_id}")
        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        logger.error(traceback.format_exc())

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res


@router.get(
    path="/syndata",
    tags=["REPORT"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultDictSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_get_syndata(
    task_id: UUID, db=Depends(async_get_db), access_token=Depends(JWTBearer())
):
    """
    Get Completed Report List
    """
    try:
        err_code = None
        begin = time.time()
        end_point = f"{router.prefix}/syndata"

        #### order 테이블에서, user_id 에 해당되는
        (data_format, report) = await genarating_synthetic_tasks.read(
            db=db, params={"from": "api_get_syndata()", "task_id": str(task_id)}
        )

        if report is None:
            err_code = 1
            raise Exception(f"task_id: {str(task_id)}에 대한 Report 정보가 존재하지 않습니다." )
        
        
        try:
            target_key_set = set()
            target_key_set = target_key_set.union(
                set(report["evaluation"]["total-filtered"]["safety"].keys())
            )
            target_key_set = target_key_set.union(
                set(report["evaluation"]["total-filtered"]["utility"].keys())
            )
            target_key_set = target_key_set.union(set(report["thresholds"].keys()))

            for class_elem in report["class"]:
                target_key_set = target_key_set.union(set(class_elem["threshold"].keys()))
                target_key_set = target_key_set.union(
                    set(class_elem["evaluation"]["safety"].keys())
                )
                target_key_set = target_key_set.union(
                    set(class_elem["evaluation"]["utility"].keys())
                )
                pass

            logger.info(f"변경할 Keys --> {target_key_set} , data_format : {data_format}")

            str_report = str(report)
            str_report = str_report.replace("total-full", "total_full")
            str_report = str_report.replace("safety-full", "safety_full")
            str_report = str_report.replace("total-filtered", "total_filtered")

            for key in target_key_set:
                new_key = "_".join(key.split(" "))
                logger.info(f"key: {key} --> new_key:{new_key}")
                str_report = str_report.replace(key, new_key)
                pass
            parsed_report = eval(str_report)
            parsed_report.update({"data_type": data_format})

            end = time.time()
            result = {
                "end_point": end_point,
                "result": parsed_report,
                "working_time_sec": end - begin,
                "response_date": datetime.now(),
            }
            # logger.info(f"Result:{result} // task_id:{str(task_id)}")
            res = JSONResponse(content=jsonable_encoder(result), status_code=200)
            return res
        except:
            raise Exception (f"task_id: {str(task_id)}의 report Premium Report 과 서식이 맞지 않습니다.")
    except Exception as e:
        logger.error(traceback.format_exc())

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
