/import_data/userflow/gn/cm/heilongjiang_v3


#!/bin/sh
spark-submit \
--master yarn-client \
--class com.prehandle.SampleData \
--num-executors 15 \
--driver-memory 8g \
--executor-memory 12g \
--executor-cores 3 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
/import_data/userflow/gn/cm/heilongjiang_v3/20160519/userflow_20160519_018085.tar.bz2 \
/user/zhangyi/location_opendsg/com_prehandle/sample_data


#!/bin/sh
spark-submit \
--master yarn-client \
--class com.prehandle.ExtractData \
--num-executors 15 \
--driver-memory 8g \
--executor-memory 12g \
--executor-cores 3 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
/import_data/userflow/gn/cm/heilongjiang_v3 \
20160523 \
20160619 \
/user/zhangyi/stage2/maptable.txt \
/user/zhangyi/location_opendsg/com_prehandle/extract_data_filepath \
/user/zhangyi/location_opendsg/com_prehandle/extract_data

#!/bin/sh
spark-submit \
--master yarn-client \
--class com.prehandle.DoubleTransfer \
--num-executors 15 \
--driver-memory 8g \
--executor-memory 20g \
--executor-cores 5 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
/user/zhangyi/location_opendsg/com_prehandle/extract_data/2016* \
/user/zhangyi/location_opendsg/com_prehandle/extract_data_1


#!/bin/sh
spark-submit \
--master yarn-client \
--class com.prehandle.FilterConnectDays \
--num-executors 15 \
--driver-memory 8g \
--executor-memory 20g \
--executor-cores 5 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
/user/zhangyi/location_opendsg/com_prehandle/extract_data/2016* \
28 \
/user/zhangyi/location_opendsg/com_prehandle/filter_connect_days


# #!/bin/sh
# spark-submit \
# --master yarn-client \
# --class com.analyze.UserConnectTimes \
# --num-executors 15 \
# --driver-memory 8g \
# --executor-memory 20g \
# --executor-cores 5 \
# --conf spark.yarn.executor.memoryOverhead=4096 \
# /home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
# /user/zhangyi/location_opendsg/com_prehandle/filter_connect_days \
# /user/zhangyi/location_opendsg/com_analyze/user_connect_times
#
#
#
#
# #!/bin/sh
# spark-submit \
# --master yarn-client \
# --class com.analyze.FrequencyTopUser \
# --num-executors 15 \
# --driver-memory 8g \
# --executor-memory 20g \
# --executor-cores 5 \
# --conf spark.yarn.executor.memoryOverhead=4096 \
# /home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
# /user/zhangyi/location_opendsg/com_prehandle/filter_connect_days \
# /user/zhangyi/location_opendsg/com_analyze/user_connect_times \
# 100 \
# /user/zhangyi/location_opendsg/com_analyze/frequency_top_user

#!/bin/sh
spark-submit \
--master yarn-client \
--class com.analyze.UserStayTime \
--num-executors 15 \
--driver-memory 8g \
--executor-memory 20g \
--executor-cores 5 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
/user/zhangyi/location_opendsg/com_prehandle/filter_connect_days \
/user/zhangyi/location_opendsg/com_analyze/user_continue_stay_time \
/user/zhangyi/location_opendsg/com_analyze/user_total_stay_time



#!/bin/sh
spark-submit \
--master yarn-client \
--class com.util.Repart \
--num-executors 10 \
--driver-memory 8g \
--executor-memory 20g \
--executor-cores 5 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
/user/zhangyi/location_opendsg/com_analyze/user_continue_stay_time \
28 \
/user/zhangyi/location_opendsg/com_analyze/user_continue_stay_time_repart


#!/bin/sh
spark-submit \
--master yarn-client \
--class com.analyze.ImportantPlace \
--num-executors 10 \
--driver-memory 8g \
--executor-memory 20g \
--executor-cores 5 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
/user/zhangyi/location_opendsg/com_analyze/user_continue_stay_time_repart \
8 \
32 \
/user/zhangyi/stage2/maptable.txt \
/user/zhangyi/location_opendsg/com_analyze/person_important_place


#!/bin/sh
spark-submit \
--master yarn-client \
--class com.algorithm.LocationKmeas \
--num-executors 10 \
--driver-memory 8g \
--executor-memory 20g \
--executor-cores 5 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
/user/zhangyi/location_opendsg/com_analyze/user_continue_stay_time_repart \
8 \
32 \
/user/zhangyi/stage2/maptable.txt \
/user/zhangyi/location_opendsg/com_analyze/person_important_place


#!/bin/sh
spark-submit \
--master yarn-client \
--class com.report.TrajectoryPair \
--num-executors 10 \
--driver-memory 8g \
--executor-memory 20g \
--executor-cores 5 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
/user/zhangyi/location_opendsg/com_analyze/user_continue_stay_time_repart \
6 \
10 \
/user/zhangyi/stage2/maptable.txt \
100 \
/user/zhangyi/location_opendsg/com_analyze/trajectory_pair_top_zao


#!/bin/sh
spark-submit \
--master yarn-client \
--class com.report.TrajectoryPair \
--num-executors 10 \
--driver-memory 8g \
--executor-memory 20g \
--executor-cores 5 \
--conf spark.yarn.executor.memoryOverhead=4096 \
/home/zhangyi/location_opendsg/location_opendsg_data_handle.jar \
/user/zhangyi/location_opendsg/com_analyze/user_continue_stay_time_repart \
16 \
19 \
/user/zhangyi/stage2/maptable.txt \
100 \
/user/zhangyi/location_opendsg/com_analyze/trajectory_pair_top_wan