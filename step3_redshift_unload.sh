work_dir=/backup/automation/kpi_weekly
log_folder=/backup/automation/kpi_weekly/logs

dummy=`date +"%Y%m%d%H%M%S"`

log_file_name="kpi_redhsift_not_accessed_"$dummy.log
export s3_main="s3://itx-ags-admin/AWS-Account-Metrics-New/Table_Not_Accessed/"

data_export_date=$(date +'%b_%d_%Y_')


if [[ -z "$data_export_date" ]];
  then
	echo -e "--------------------------------------------------------------------------------------------------------------------------------------------------------------"
	echo "Export date value is not available"
	echo -e "Process Failed at `TZ="EST" date`"
	echo -e "--------------------------------------------------------------------------------------------------------------------------------------------------------------"
        exit 1
fi



content=`echo -e 'Hi Team\n\n KPI metrics - redshift table not accessed unload process started. \n\nPlease check the log file '$log_folder'/'$log_file_name' for more details about the progress. \n\nThanks\nCDE IT AWS Admin'`
sub='STARTED : Step 3: KPI metrics - redshift table not accessed unload proces - Started at '`TZ="EST" date`''
sh -c " `echo -e "$content" | mailx -r "Automation_Infra" -s " $sub" "DL-OBIUS-CDE-IT-AWSAdmin@ITS.JNJ.com"`"
status=$?
if [[ $status -eq 0 ]];then
echo "Email Process Completed successfully for the Start of redshift table not accessed unload process"
echo -e "--------------------------------------------------------------------------------------------------------------------------------------------------------------"
else
echo "Email Process Failed"
echo -e "Email Process Failed at `TZ="EST" date`"
echo -e "--------------------------------------------------------------------------------------------------------------------------------------------------------------"
exit 1
echo -e "--------------------------------------------------------------------------------------------------------------------------------------------------------------"
fi

sleep 3

echo "Calling redshift_unload.py script "
python3 /backup/automation/kpi_weekly/step3_redshift_unload.py > $log_folder/$log_file_name
status_python=$?
echo "Got Exit Code $status_python from Python"

chmod 777 $log_folder/$log_file_name


if [[ $status_python -eq 0 ]];then
echo "Python Process Completed successfully"
content=`echo -e 'Hi Team\n\nKPI metrics - redshift table not accessed unload process completed successfully. \n\nPlease check the log file '$log_folder'/'$log_file_name' for more details about the progress.\n\nThanks\nCDE IT AWS Admin'`
sub='SUCCESS : Step 3: KPI metrics - redshift table not accessed unload proces - Completed at '`TZ="EST" date`''
echo -e "Unload validation is below : \n" >> $log_folder/$log_file_name
echo -e "aws s3 ls $s3_main | grep $data_export_date" >>  $log_folder/$log_file_name
aws s3 ls $s3_main --profile itx-ags | grep $data_export_date >> $log_folder/$log_file_name
else
echo "Python Process Failed"
content=`echo -e 'Hi Team\n\nKPI metrics - redshift table not accessed unload process failed . \n\nPlease check the log file '$log_folder'/'$log_file_name' for more details about the progress.\n\nThanks\nCDE IT AWS Admin'`
sub='FAILED : Step 3: KPI metrics - redshift table not accessed unload proces - Failed at '`TZ="EST" date`''
#echo $content
echo -e "Process Failed at `TZ="EST" date`"
#echo -e "--------------------------------------------------------------------------------------------------------------------------------------------------------------"
fi


#sh -c " `echo -e "$content" | mailx -r "DL-OBIUS-CDE-IT-AWSAdmin@ITS.JNJ.com" -s " $sub" "DL-OBIUS-CDE-IT-AWSAdmin@ITS.JNJ.com"`"
sh -c " `echo -e "$content" | mailx -a $log_folder/$log_file_name -r "Automation_Infra" -s " $sub" "DL-OBIUS-CDE-IT-AWSAdmin@ITS.JNJ.com"`"
status=$?
if [[ $status -eq 0 ]];then
echo "Email Process Completed to send final status"
echo -e "Overall Process Completed at `TZ="EST" date`. Please check the log file $log_folder/$log_file_name"
echo -e "--------------------------------------------------------------------------------------------------------------------------------------------------------------"
else
echo "Email Process Failed"
echo -e "Process Failed to send final status"
echo -e "Overall Process Completed at `TZ="EST" date`. Please check the log file $log_folder/$log_file_name"
echo -e "--------------------------------------------------------------------------------------------------------------------------------------------------------------"
exit 1
fi

