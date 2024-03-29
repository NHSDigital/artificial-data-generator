# Databricks notebook source
# Source: yanai repo (DPS) - path: src/dsp/datasets/schema/hes/hes_ns_ae_schema.py
from pyspark.sql.types import *


def get_hes_ae_schema():
    schema = StructType([
        StructField("FYEAR", StringType(), True),
        StructField("PARTYEAR", IntegerType(), True),
        StructField("PSEUDO_HESID", StringType(), True),
        StructField("AEKEY", LongType(), True),
        StructField("AEKEY_FLAG", IntegerType(), True),
        StructField("AEARRIVALMODE", StringType(), True),
        StructField("AEATTEND_EXC_PLANNED", IntegerType(), True),
        StructField("AEATTENDCAT", StringType(), True),
        StructField("AEATTENDDISP", StringType(), True),
        StructField("AEDEPTTYPE", StringType(), True),
        StructField("AEINCLOCTYPE", StringType(), True),
        StructField("AEPATGROUP", StringType(), True),
        StructField("AEREFSOURCE", StringType(), True),
        StructField("AT_GP_PRACTICE", StringType(), True),
        StructField("AT_RESIDENCE", StringType(), True),
        StructField("AT_TREATMENT", StringType(), True),
        StructField("ARRIVALAGE", IntegerType(), True),
        StructField("ARRIVALAGE_CALC", DoubleType(), True),
        StructField("ARRIVALDATE", DateType(), True),
        StructField("ARRIVALTIME", StringType(), True),
        StructField("CANNET", StringType(), True),
        StructField("CANREG", StringType(), True),
        StructField("CCG_GP_PRACTICE", StringType(), True),
        StructField("CCG_RESIDENCE", StringType(), True),
        StructField("CCG_RESPONSIBILITY", StringType(), True),
        StructField("CCG_RESPONSIBILITY_ORIGIN", StringType(), True),
        StructField("CCG_TREATMENT", StringType(), True),
        StructField("CCG_TREATMENT_ORIGIN", StringType(), True),
        StructField("CR_GP_PRACTICE", StringType(), True),
        StructField("CR_RESIDENCE", StringType(), True),
        StructField("CR_TREATMENT", StringType(), True),
        StructField("CONCLDUR", IntegerType(), True),
        StructField("CONCLTIME", StringType(), True),
        StructField("DEPDUR", IntegerType(), True),
        StructField("DEPTIME", StringType(), True),
        StructField("DIAG2_01", StringType(), True),
        StructField("DIAG2_02", StringType(), True),
        StructField("DIAG2_03", StringType(), True),
        StructField("DIAG2_04", StringType(), True),
        StructField("DIAG2_05", StringType(), True),
        StructField("DIAG2_06", StringType(), True),
        StructField("DIAG2_07", StringType(), True),
        StructField("DIAG2_08", StringType(), True),
        StructField("DIAG2_09", StringType(), True),
        StructField("DIAG2_10", StringType(), True),
        StructField("DIAG2_11", StringType(), True),
        StructField("DIAG2_12", StringType(), True),
        StructField("DIAG3_01", StringType(), True),
        StructField("DIAG3_02", StringType(), True),
        StructField("DIAG3_03", StringType(), True),
        StructField("DIAG3_04", StringType(), True),
        StructField("DIAG3_05", StringType(), True),
        StructField("DIAG3_06", StringType(), True),
        StructField("DIAG3_07", StringType(), True),
        StructField("DIAG3_08", StringType(), True),
        StructField("DIAG3_09", StringType(), True),
        StructField("DIAG3_10", StringType(), True),
        StructField("DIAG3_11", StringType(), True),
        StructField("DIAG3_12", StringType(), True),
        StructField("DIAGA_01", StringType(), True),
        StructField("DIAGA_02", StringType(), True),
        StructField("DIAGA_03", StringType(), True),
        StructField("DIAGA_04", StringType(), True),
        StructField("DIAGA_05", StringType(), True),
        StructField("DIAGA_06", StringType(), True),
        StructField("DIAGA_07", StringType(), True),
        StructField("DIAGA_08", StringType(), True),
        StructField("DIAGA_09", StringType(), True),
        StructField("DIAGA_10", StringType(), True),
        StructField("DIAGA_11", StringType(), True),
        StructField("DIAGA_12", StringType(), True),
        StructField("DIAGS_01", StringType(), True),
        StructField("DIAGS_02", StringType(), True),
        StructField("DIAGS_03", StringType(), True),
        StructField("DIAGS_04", StringType(), True),
        StructField("DIAGS_05", StringType(), True),
        StructField("DIAGS_06", StringType(), True),
        StructField("DIAGS_07", StringType(), True),
        StructField("DIAGS_08", StringType(), True),
        StructField("DIAGS_09", StringType(), True),
        StructField("DIAGS_10", StringType(), True),
        StructField("DIAGS_11", StringType(), True),
        StructField("DIAGS_12", StringType(), True),
        StructField("DIAGSCHEME", StringType(), True),
        StructField("ETHNOS", StringType(), True),
        StructField("EPIKEY", LongType(), True),
        StructField("GORTREAT", StringType(), True),
        StructField("GPPRAC", StringType(), True),
        StructField("INITDUR", IntegerType(), True),
        StructField("INITTIME", StringType(), True),
        StructField("INVEST2_01", StringType(), True),
        StructField("INVEST2_02", StringType(), True),
        StructField("INVEST2_03", StringType(), True),
        StructField("INVEST2_04", StringType(), True),
        StructField("INVEST2_05", StringType(), True),
        StructField("INVEST2_06", StringType(), True),
        StructField("INVEST2_07", StringType(), True),
        StructField("INVEST2_08", StringType(), True),
        StructField("INVEST2_09", StringType(), True),
        StructField("INVEST2_10", StringType(), True),
        StructField("INVEST2_11", StringType(), True),
        StructField("INVEST2_12", StringType(), True),
        StructField("INVESTSCHEME", StringType(), True),
        StructField("PCON", StringType(), True),
        StructField("PCON_ONS", StringType(), True),
        StructField("PCTCODE_HIS", StringType(), True),
        StructField("PCTORIG_HIS", StringType(), True),
        StructField("PCTTREAT", StringType(), True),
        StructField("PROCODE3", StringType(), True),
        StructField("PROCODE5", StringType(), True),
        StructField("PROCODET", StringType(), True),
        StructField("PROCSCHEME", IntegerType(), True),
        StructField("PURCODE", StringType(), True),
        StructField("RANK_ORDER", IntegerType(), True),
        StructField("RESCTY", StringType(), True),
        StructField("RESCTY_ONS", StringType(), True),
        StructField("RESGOR", StringType(), True),
        StructField("RESGOR_ONS", StringType(), True),
        StructField("RESLADST", StringType(), True),
        StructField("RESLADST_ONS", StringType(), True),
        StructField("RESPCT_HIS", StringType(), True),
        StructField("RESSTHA_HIS", StringType(), True),
        StructField("SEX", StringType(), True),
        StructField("LSOA01", StringType(), True),
        StructField("MSOA01", StringType(), True),
        StructField("STHATRET", StringType(), True),
        StructField("SUSHRG", StringType(), True),
        StructField("SUSLDDATE_HIS", StringType(), True),
        StructField("TREAT2_01", StringType(), True),
        StructField("TREAT2_02", StringType(), True),
        StructField("TREAT2_03", StringType(), True),
        StructField("TREAT2_04", StringType(), True),
        StructField("TREAT2_05", StringType(), True),
        StructField("TREAT2_06", StringType(), True),
        StructField("TREAT2_07", StringType(), True),
        StructField("TREAT2_08", StringType(), True),
        StructField("TREAT2_09", StringType(), True),
        StructField("TREAT2_10", StringType(), True),
        StructField("TREAT2_11", StringType(), True),
        StructField("TREAT2_12", StringType(), True),
        StructField("TREAT3_01", StringType(), True),
        StructField("TREAT3_02", StringType(), True),
        StructField("TREAT3_03", StringType(), True),
        StructField("TREAT3_04", StringType(), True),
        StructField("TREAT3_05", StringType(), True),
        StructField("TREAT3_06", StringType(), True),
        StructField("TREAT3_07", StringType(), True),
        StructField("TREAT3_08", StringType(), True),
        StructField("TREAT3_09", StringType(), True),
        StructField("TREAT3_10", StringType(), True),
        StructField("TREAT3_11", StringType(), True),
        StructField("TREAT3_12", StringType(), True),
        StructField("TREATSCHEME", StringType(), True),
        StructField("TRETDUR", IntegerType(), True),
        StructField("TRETTIME", StringType(), True),
        StructField("LSOA11", StringType(), True),
        StructField("MSOA11", StringType(), True),
        StructField("PROVDIST", StringType(), True),
        StructField("PROVDIST_FLAG", StringType(), True),
        StructField("NER_GP_PRACTICE", StringType(), True),
        StructField("NER_RESIDENCE", StringType(), True),
        StructField("NER_TREATMENT", StringType(), True),
        StructField("SITETRET", StringType(), True),
        StructField("SITEDIST", StringType(), True),
        StructField("SITEDIST_FLAG", StringType(), True)
    ])
    return schema