
#--------------------------------------------------------------------------------------------
# SHELL PySPARK - PMVTmktChamAtvFaixa.py
#--------------------------------------------------------------------------------------------

# data_arq=$(date "+%Y-%m")
# Data recebida como parâmetro, formato: YYYY-MM
# data_arq=$1
ret_cod=0

#--------------------------------------------------------------------------------------------

#spark-submit --master yarn --queue root.bi.carga --name PMV-TMK_CHAM-ATV-FAIXA --deploy-mode cluster --executor-memory 10G --executor-cores 10 --driver-memory 9G --num-executors 5 /databi/pmv/scripts/src/scripts/PMVTmktChamAtvFaixa.py


spark-submit 
    --name PMV_TMKT_CHAM_ATV_FAIXA
    --master yarn
    --deploy-mode cluster
    --queue root.digital.users
    --executor-memory 20G
    --executor-cores 10
    --total-executor-cores 10
    --principal bibigadm/hdcpx02@INFRA.OI
    --keytab /home/bibigadm/bibigadm_hdcpx02.keytab
    /databi/pmv/scripts/src/scripts/PMVTmktChamAtvFaixa.py
   

# spark-submit --master yarn\
#             --queue root.bi.carga\ 
#             --name PMV-TMK_CHAM-ATV-FAIXA\
#             --deploy-mode cluster\
#             --executor-memory 10G\
#             --executor-cores 10\
#             --driver-memory 9G\
#             --num-executors 5\
#             /databi/pmv/scripts/src/scripts/PMVTmktChamAtvFaixa.py
             
ParamRetCode="${?}" 
if [[ "${ParamRetCode}" -eq "0" ]]; then
	echo "Carga finalizada!"
else
	echo 'Erro ao Executar'
	exit 1
fi

#spark-submit --master yarn --queue root.bi.carga --name PMV-TMK_CHAM-ATV-FAIXA --deploy-mode cluster --executor-memory 10G --executor-cores 10 --driver-memory 9G --num-executors 5 /databi/pmv/scripts/src/scripts/PMVTmktChamAtvFaixa.py