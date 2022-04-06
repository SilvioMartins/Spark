#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql.types import * 
from pyspark.sql.functions import *
import sys, os

reload(sys)
sys.setdefaultencoding('UTF-8')

sc =  SparkContext()
sql = SQLContext(sc)
hc = HiveContext(sc)

# Executando SQL no HIVE
hc.sql(
"""
INSERT OVERWRITE TABLE   bi.tb_i_mkt_det_ma_cham_tmkt_atv_faixa_v2  
    SELECT DISTINCT
        t1.DT_INI_ATIVIDADE_REDE, 
        t1.HR_INI_ATIVIDADE_REDE, 
        t1.QT_DURACAO_COBRAVEL, 
        t1.QT_DURACAO_REAL, 
        t1.FAIXA, 
        t1.CD_PLATAFORMA_ORIG, 
        t1.NU_AREA_TELEFONE_ORIG, 
        t1.NU_LINHA_TELEFONE_ORIG, 
        t1.MACESSO_ORIG, 
        t1.NU_AREA_TELEFONE, 
        t1.NU_LINHA_TELEFONE,
        t1.MACESSO, 
        t1.NO_TIPO_SERV_ATIVIDADE_REDE, 
        t1.NO_CATEG_PONTA_DESTINO, 
        t1.QT_DIAS, 
        t1.DS_HORARIO, 
        t2.Codigo_SAP_Parceiro  AS CD_SAP_PARCEIRO
    FROM 
        bi.tb_i_mkt_det_ma_cham_tmkt_atv_faixa  t1
    LEFT JOIN bi.tb_t_mkt_parceiros t2 
        ON t1.MACESSO_ORIG = t2.Numero_Originador_DDD_Numero
    WHERE
        t2.classificacao = 'Telemarketing' AND t2.enriquecido = 'Não';
"""  
)
