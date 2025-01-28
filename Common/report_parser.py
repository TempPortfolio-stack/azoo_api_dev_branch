import traceback
from Settings.Logger.logging_config import fastapi_logger as logger


def report_metrics_parser_v1(end_point:str , azoo_product_id:int , dataset_type:str , eval_json_value:dict):
    try:
        logger.info(f"report_metrics_parser_v1() 에 요청이 들어왔습니다. --> origin:{end_point} , azoo_product_id:{azoo_product_id} , dataset_type:{dataset_type}")
        
        if (end_point is None) or \
           (azoo_product_id is None) or \
           (dataset_type is None) or \
           (eval_json_value is None):
            raise Exception("유효하지 않은 값이 포함되어 있습니다.")
        
        response_data = dict()

        total_filtered = eval_json_value["evaluation"]["total-filtered"]
        
        if dataset_type.lower() == 'image':
            response_data["downstream_classification"] = total_filtered["utility"]["Downstream Classification Accuracy Rate"]
            response_data["kid"] = total_filtered["utility"]["KID"]
            response_data["one_class_classification"] = total_filtered["utility"]["Indistinguishability"]["value"]
            response_data["lpips"] = total_filtered["safety"]["LPIPS"]
            response_data["ssim"] = total_filtered["safety"]["SSIM"]
            
        elif dataset_type.lower() == 'tabular':
            response_data["downstream_classification"] = total_filtered["utility"]["Downstream Classification Accuracy Rate"]
            response_data["mmd"] = total_filtered["utility"]["MMD"]
            response_data["entropy"] = total_filtered["utility"]["Entropy"]
            response_data["two_d_correlation_similarity"] = total_filtered["utility"]["2D Correlation Similarity"]
            response_data["one_class_classification"] = total_filtered["utility"]["Indistinguishability"]["value"]
            response_data["duplication_rate"] = total_filtered["utility"]["Duplicated Rate"]
            response_data["identification_risk"] = total_filtered["safety"]["Identification Risk"]
            response_data["linkage_risk"] = total_filtered["safety"]["Linkage Risk"]
            response_data["inference_risk"] = total_filtered["safety"]["Inference Risk"]
            
        elif dataset_type.lower() == "text":
            response_data["downstream_classification"] = total_filtered["utility"]["Downstream Classification Accuracy Rate"]
            response_data["mmd"] = total_filtered["utility"]["MMD"]
            response_data["one_class_classification"] = total_filtered["utility"]["Indistinguishability"]["value"]
            response_data["rouge"] = total_filtered["safety"]["ROUGE"]
            response_data["bert_score"] = total_filtered["safety"]["BERTScore"]

        return response_data
    except:
        logger.error("report_metrics_parser_v1()에서 에러 발생")
        logger.error(traceback.format_exc())
        raise # 위로 올림
    

def report_metrics_parser_v2(end_point:str , azoo_product_id:int , dataset_type:str , eval_json_value:dict):
    try:
        logger.info(f"report_metrics_parser_v2() 에 요청이 들어왔습니다. --> origin:{end_point} , azoo_product_id:{azoo_product_id} , dataset_type:{dataset_type}")
        
        if (end_point is None) or \
           (azoo_product_id is None) or \
           (dataset_type is None) or \
           (eval_json_value is None):
            raise Exception("유효하지 않은 값이 포함되어 있습니다.")
        
        response_data = dict()

        total_filtered = eval_json_value["evaluation"]["total-filtered"]
        downstream = total_filtered["downstream"]
        safety = total_filtered["safety"]
        utility = total_filtered["utility"]

        if dataset_type.lower() == 'image':
            response_data["downstream_classification"] = downstream["Accuracy"]["Accuracy Rate"]
            response_data["kid"] = utility["Quality"]["KID"]
            response_data["one_class_classification"] = utility["Indistinguishability"]["OCC Accuracy"]["value"]
            response_data["lpips"] = safety["Perceptual Similarity"]["LPIPS"]
            response_data["ssim"] =  safety["Structural Similarity"]["SSIM"]

        elif dataset_type.lower() == 'tabular':
            response_data["downstream_classification"] = downstream["Accuracy"]["Accuracy Rate"]
            response_data["mmd"] = utility["Quality"]["MMD"]
            response_data["entropy"] = utility["Diversity"]["Entropy"]
            response_data["two_d_correlation_similarity"] = utility["Quality"]["2D Correlation Similarity"]
            response_data["one_class_classification"] = utility["Indistinguishability"]["OCC Accuracy"]["value"]
            response_data["duplication_rate"] = utility["Duplication"]["Duplication Rate"]
            response_data["identification_risk"] = safety["Structural Similarity"]["Identification Risk"]
            response_data["linkage_risk"] = safety["Perceptual Similarity"]["Linkage Risk"]
            response_data["inference_risk"] = safety["Perceptual Similarity"]["Inference Risk"]

        elif dataset_type.lower() == "text":
            response_data["downstream_classification"] = downstream["Accuracy"]["Accuracy Rate"]
            response_data["mmd"] = utility["Quality"]["MMD"]
            response_data["one_class_classification"] = utility["Indistinguishability"]["OCC Accuracy"]["value"]
            response_data["rouge"] = safety["Structural Similarity"]["ROUGE"]
            response_data["bert_score"] = safety["Perceptual Similarity"]["BERTScore"]

        return response_data
    except:
        logger.error("report_metrics_parser_v2()에서 에러 발생")
        logger.error(traceback.format_exc())
        raise # 위로 올림




def report_thres_parser_v1(end_point:str , azoo_product_id:int , dataset_type:str , eval_json_value:dict):
    try:
        logger.info(f"report_thres_parser_v1() 에 요청이 들어왔습니다. --> origin:{end_point} , azoo_product_id:{azoo_product_id} , dataset_type:{dataset_type}")
        
        if (end_point is None) or \
           (azoo_product_id is None) or \
           (dataset_type is None) or \
           (eval_json_value is None):
            raise Exception("유효하지 않은 값이 포함되어 있습니다.")
        
        response_data = dict()
        thresholds = eval_json_value["thresholds"]
                
        if dataset_type.lower() == 'image':
            #### ThresHold
            response_data["threshold"] = dict()
            response_data["threshold"]["downstream_classification"] = thresholds[
                "Downstream Classification Accuracy Rate"
            ]["threshold"]
            response_data["threshold"]["kid"] = thresholds["KID"]["threshold"]
            response_data["threshold"]["one_class_classification"] = thresholds[
                "Indistinguishability"
            ]["threshold"]
            response_data["threshold"]["lpips"] = thresholds["LPIPS"]["threshold"]
            response_data["threshold"]["ssim"] = thresholds["SSIM"]["threshold"]
            
        elif dataset_type.lower() == 'tabular':
            response_data["threshold"] = dict()
            response_data["threshold"]["downstream_classification"] = thresholds[
                "Downstream Classification Accuracy Rate"
            ]["threshold"]
            response_data["threshold"]["mmd"] = thresholds["MMD"]["threshold"]
            response_data["threshold"]["entropy"] = thresholds["Entropy"]["threshold"]
            response_data["threshold"]["two_d_correlation_similarity"] = thresholds[
                "2D Correlation Similarity"
            ]["threshold"]
            response_data["threshold"]["one_class_classification"] = thresholds[
                "Indistinguishability"
            ]["threshold"]
            response_data["threshold"]["duplication_rate"] = thresholds[
                "Duplicated Rate"
            ]["threshold"]
            response_data["threshold"]["identification_risk"] = thresholds[
                "Identification Risk"
            ]["threshold"]
            response_data["threshold"]["linkage_risk"] = thresholds["Linkage Risk"][
                "threshold"
            ]
            response_data["threshold"]["inference_risk"] = thresholds["Inference Risk"][
                "threshold"
            ]
            
        elif dataset_type.lower() == "text":
            response_data["threshold"] = dict()
            response_data["threshold"]["downstream_classification"] = thresholds[
                "Downstream Classification Accuracy Rate"
            ]["threshold"]
            response_data["threshold"]["mmd"] = thresholds["MMD"]["threshold"]
            response_data["threshold"]["one_class_classification"] = thresholds[
                "Indistinguishability"
            ]["threshold"]
            response_data["threshold"]["rouge"] = thresholds["ROUGE"]["threshold"]
            response_data["threshold"]["bert_score"] = thresholds["BERTScore"][
                "threshold"
            ]
        
        return response_data
    except:
        logger.error("report_thres_parser_v1()에서 에러 발생 --> 빈 dict 를 반환합니다.")
        logger.error(traceback.format_exc())
        raise # 위로 올림



def report_thres_parser_v2(end_point:str , azoo_product_id:int , dataset_type:str , eval_json_value:dict):
    try:
        logger.info(f"report_thres_parser_v2() 에 요청이 들어왔습니다. --> origin:{end_point} , azoo_product_id:{azoo_product_id} , dataset_type:{dataset_type}")
        
        if (end_point is None) or \
           (azoo_product_id is None) or \
           (dataset_type is None) or \
           (eval_json_value is None):
            raise Exception("유효하지 않은 값이 포함되어 있습니다.")
        
        response_data = dict()
        response_data["threshold"] = dict()

        thresholds = eval_json_value["thresholds"]
        if len(thresholds.keys()) == 0:
            return None
        
        thres_downstream = thresholds["downstream"]
        thres_safety = thresholds["safety"]
        thres_utility = thresholds["utility"]

        if dataset_type.lower() == 'image':
            response_data["threshold"]["downstream_classification"] = thres_downstream["Accuracy"]["Accuracy Rate"]
            response_data["threshold"]["kid"] = thres_utility["Quality"]["KID"]
            response_data["threshold"]["one_class_classification"] = thres_utility["Indistinguishability"]["OCC Accuracy"]
            response_data["threshold"]["lpips"] = thres_safety["Perceptual Similarity"]["LPIPS"]
            response_data["threshold"]["ssim"] = thres_safety["Structural Similarity"]["SSIM"]
                        
        elif dataset_type.lower() == 'tabular':
            response_data["threshold"]["downstream_classification"] = thres_downstream["Accuracy"]["Accuracy Rate"]
            response_data["threshold"]["mmd"] = thres_utility["Quality"]["MMD"]
            response_data["threshold"]["entropy"] = thres_utility["Diversity"]["Entropy"]
            response_data["threshold"]["two_d_correlation_similarity"] = thres_utility["Quality"]["2D Correlation Similarity"]
            response_data["threshold"]["one_class_classification"] = thres_utility["Indistinguishability"]["OCC Accuracy"]
            response_data["threshold"]["duplication_rate"] = thres_utility["Duplication"]["Duplication Rate"]
            response_data["threshold"]["identification_risk"] = thres_safety["Structural Similarity"]["Identification Risk"]
            response_data["threshold"]["linkage_risk"] = thres_safety["Perceptual Similarity"]["Linkage Risk"]
            response_data["threshold"]["inference_risk"] = thres_safety["Perceptual Similarity"]["Inference Risk"]
            
        elif dataset_type.lower() == "text":
            response_data["threshold"]["downstream_classification"] = thres_downstream["Accuracy"]["Accuracy Rate"]            
            response_data["threshold"]["mmd"] = thres_utility["Quality"]["MMD"]
            response_data["threshold"]["one_class_classification"] = thres_utility["Indistinguishability"]["OCC Accuracy"]
            response_data["threshold"]["rouge"] = thres_safety["Structural Similarity"]["ROUGE"]
            response_data["threshold"]["bert_score"] = thres_safety["Perceptual Similarity"]["BERTScore"]
        
        return response_data
    except:
        logger.error("report_thres_parser_v2()에서 에러 발생 --> 빈 dict 를 반환합니다.")
        logger.error(traceback.format_exc())
        raise # 위로 올림