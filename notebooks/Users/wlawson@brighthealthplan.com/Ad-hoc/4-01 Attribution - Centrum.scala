// Databricks notebook source
// MAGIC %sql
// MAGIC SELECT
// MAGIC attribution_type
// MAGIC ,CASE WHEN pl.plan_variant_marketing_name like '%Super%' THEN 'Y' ELSE 'N' End AS super
// MAGIC ,count(distinct member_id) ct
// MAGIC 
// MAGIC FROM dev.enrollment__pcp_attribution A
// MAGIC LEFT JOIN dev.enrollment__base_enrollment e ON a.fk_enrollment_id = e.enrollment_id
// MAGIC LEFT JOIN bdp.plans pl ON e.plan_id = replace(pl.hios_plan_id_standard_component_variant, '-', '') and plan_year = '2021'
// MAGIC WHERE year_month = '2021-04'
// MAGIC AND care_partner_name = 'Centrum'
// MAGIC AND CURRENT_DATE() BETWEEN source_start_date AND COALESCE(source_end_date, '2078-12-31')
// MAGIC AND is_cancelled = false
// MAGIC AND (effectuation_date_utc IS NOT NULL OR e.member_ID LIKE '5%')
// MAGIC 
// MAGIC 
// MAGIC group by 1,2
// MAGIC 
// MAGIC order by super, ct desc

// COMMAND ----------

display(spark.sql("select * from bdp.plans")

// COMMAND ----------

// MAGIC %md
// MAGIC # QA/Test

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dev.enrollment__base_enrollment e 
// MAGIC LEFT JOIN bdp.plans pl ON e.plan_id = replace(pl.hios_plan_id_standard_component_variant, '-', '') and plan_year = '2021'
// MAGIC where member_id = '100280903'

// COMMAND ----------

