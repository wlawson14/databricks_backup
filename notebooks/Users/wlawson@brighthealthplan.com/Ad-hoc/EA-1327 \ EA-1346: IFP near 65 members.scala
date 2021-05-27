// Databricks notebook source
def BrokerDetailSearchTable = spark.read.format("csv").options(Map("header"->"true", "delimiter"->",")).load("dbfs:/FileStore/shared_uploads/wlawson@brighthealthplan.com/ADHOC/BrokerDetailSearchTable_05_10_2021.csv")
BrokerDetailSearchTable.createOrReplaceTempView("BrokerDetailSearchTable")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from BrokerDetailSearchTable

// COMMAND ----------

// MAGIC %sql
// MAGIC REFRESH TABLE dev.person__golden_person_crosswalk;
// MAGIC REFRESH TABLE dev.person__golden_person;
// MAGIC REFRESH TABLE dev.enrollment__pcp_attribution;
// MAGIC REFRESH TABLE dev.enrollment__base_enrollment;
// MAGIC 
// MAGIC SELECT DISTINCT
// MAGIC   COALESCE(LOB, '') AS LOB,
// MAGIC   COALESCE(market_state, '') AS market_state,
// MAGIC   COALESCE(market, '') AS market,
// MAGIC   COALESCE(plan_id, '') AS plan_id,
// MAGIC   COALESCE(plan_variant_marketing_name, '') AS plan_variant_marketing_name,
// MAGIC   COALESCE(member_id, '') AS member_id,
// MAGIC   COALESCE(subscriber_id, '') AS subscriber_id,
// MAGIC   COALESCE(agent_broker_name, '') as agent_broker_name,
// MAGIC   COALESCE(agent_broker_npn, '') as agent_broker_npn,
// MAGIC   COALESCE(`FMO/Agency`, '') as agent_broker_agency,
// MAGIC   COALESCE(source_start_date, '') AS source_start_date,
// MAGIC   COALESCE(source_end_date, '') AS source_end_date,
// MAGIC   COALESCE(source_effectuation_date, '') AS source_effectuation_date,
// MAGIC   COALESCE(grace_period_indicator, '') AS grace_period_indicator,
// MAGIC   COALESCE(grace_period_number, '') AS grace_period_number,
// MAGIC   COALESCE(grace_period_start_date, '') AS grace_period_start_date,
// MAGIC   COALESCE(grace_period_end_date, '') AS grace_period_end_date,
// MAGIC   COALESCE(first_name, '') AS first_name,
// MAGIC   COALESCE(middle_name, '') AS middle_name,
// MAGIC   COALESCE(last_name, '') AS last_name,
// MAGIC   COALESCE(dependent_relationship, '') AS dependent_relationship,
// MAGIC   COALESCE(DOB, '') AS DOB,
// MAGIC   COALESCE(floor(current_age), '') as current_age,
// MAGIC   COALESCE(floor(target_age), '') as `12-31-2022_age`,
// MAGIC   COALESCE(provider_first_name, '') as provider_first_name,
// MAGIC   COALESCE(provider_last_name, '') as provider_last_name,
// MAGIC   COALESCE(attributed_provider_npi, '') as provider_npi,  
// MAGIC   COALESCE(email_address, '') AS email_address,
// MAGIC   COALESCE(phone_number, '') AS phone_number,
// MAGIC   COALESCE(residential_address_line_1, '') AS residential_address_line_1,
// MAGIC   COALESCE(residential_address_line_2, '') AS residential_address_line_2,
// MAGIC   COALESCE(residential_city_name, '') AS residential_city_name,
// MAGIC   COALESCE(residential_state_code, '') AS residential_state_code,
// MAGIC   COALESCE(residential_zipcode, '') AS residential_zipcode,
// MAGIC   COALESCE(residential_county_code, '') AS residential_county_code,
// MAGIC   COALESCE(mailing_address_line_1, '') AS mailing_address_line_1,
// MAGIC   COALESCE(mailing_address_line_2, '') AS mailing_address_line_2,
// MAGIC   COALESCE(mailing_address_city, '') AS mailing_address_city,
// MAGIC   COALESCE(mailing_address_state_code, '') AS mailing_address_state_code,
// MAGIC   COALESCE(mailing_address_zipcode, '') AS mailing_address_zipcode,
// MAGIC   COALESCE(marital_status, '') AS marital_status,
// MAGIC   COALESCE(gender, '') AS gender,
// MAGIC   COALESCE(primary_spoken_language, '') AS primary_spoken_language,
// MAGIC   COALESCE(primary_written_language, '') AS primary_written_language
// MAGIC FROm (
// MAGIC   select distinct
// MAGIC   per.date_of_birth as DOB,
// MAGIC   (datediff(current_date(), per.date_of_birth)/365.25) as current_age,
// MAGIC   (datediff('2022-12-31', per.date_of_birth)/365.25) as target_age, 
// MAGIC   pl.*, e.*, per.*, a.*, b.*
// MAGIC   from dev.enrollment__base_enrollment e
// MAGIC   left join bdp.plans pl ON
// MAGIC     pl.plan_year = '2021' and (e.plan_id = REPLACE(pl.hios_plan_id_standard_component_variant, '-', '') --- ifp
// MAGIC     OR LEFT(REPLACE(pl.hios_plan_id_standard_component_variant, '-', ''), 8) = REPLACE(e.plan_id, ' ', '')) --- ma
// MAGIC   LEFT JOIN dev.person__golden_person_crosswalk c ON e.fk_person_id = c.person_id
// MAGIC   LEFT JOIN dev.person__golden_person per ON c.golden_person_id = per.golden_person_id
// MAGIC   LEFT JOIN
// MAGIC   (
// MAGIC     SELECT DISTINCT att.*, plt.provider_first_name, plt.provider_last_name
// MAGIC     FROM dev.enrollment__pcp_attribution att
// MAGIC     INNER JOIN --- max yearmo for each enrollment
// MAGIC     (
// MAGIC       SELECT fk_enrollment_id, max(year_month) year_month
// MAGIC       FROM dev.enrollment__pcp_attribution
// MAGIC       GROUP BY fk_enrollment_id
// MAGIC     ) nest ON att.fk_enrollment_id = nest.fk_enrollment_id AND att.year_month = nest.year_month
// MAGIC     
// MAGIC     LEFT JOIN (SELECT plt_npi, first(plt.first_name) as provider_first_name, first(plt.last_name) as provider_last_name from bdp.plt group by plt_npi)
// MAGIC       plt ON plt.plt_npi = att.attributed_provider_npi
// MAGIC       
// MAGIC   ) a ON a.fk_enrollment_id = e.enrollment_id
// MAGIC   LEFT JOIN BrokerDetailSearchTable b ON b.`National Producer Number` = e.agent_broker_npn
// MAGIC   
// MAGIC --   WHERE pl.market_state IN ('AZ', 'CO', 'IL')
// MAGIC   WHERE pl.market_state IN ('FL')
// MAGIC   AND lob = 'IFP'
// MAGIC   AND ( 1 = 1 AND --- active
// MAGIC      cast(current_date() as date) BETWEEN source_start_date AND COALESCE(source_end_date, '2078-12-31')
// MAGIC      AND (source_effectuation_date is not null or member_id like '5%')
// MAGIC      AND is_cancelled = false
// MAGIC      )
// MAGIC -- order by rand()     
// MAGIC )
// MAGIC WHERE target_age >= 64.999
// MAGIC order by
// MAGIC agent_broker_agency desc,
// MAGIC market_state, market
// MAGIC -- order by `include?` desc, target_age asc

// COMMAND ----------

// MAGIC %sql
// MAGIC REFRESH TABLE dev.person__golden_person_crosswalk;
// MAGIC REFRESH TABLE dev.person__golden_person;
// MAGIC REFRESH TABLE dev.enrollment__pcp_attribution;
// MAGIC REFRESH TABLE dev.enrollment__base_enrollment;
// MAGIC 
// MAGIC 
// MAGIC select e.* from dev.enrollment__pcp_attribution a
// MAGIC inner join dev.enrollment__base_enrollment_history e ON a.fk_enrollment_id = e.enrollment_id
// MAGIC -- LEFT JOIN dev.person__golden_person_crosswalk c ON e.fk_person_id = c.person_id
// MAGIC -- LEFT JOIN dev.person__golden_person per ON c.golden_person_id = per.golden_person_id
// MAGIC where e.member_id = '100386153'
// MAGIC order by year_month desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dev.enrollment__base_enrollment

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select first(plan_id, TRUE) from (
// MAGIC select distinct CASE WHEN plan_id = ' ' THEN null else plan_id end as plan_Id
// MAGIC from dev.enrollment__base_enrollment e where member_id like '5%' order by plan_id
// MAGIC -- )

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select
// MAGIC row_number() over (partition by subscriber_id order by subscriber_id) rk
// MAGIC ,* from dev.enrollment__base_enrollment
// MAGIC where member_id like '5%'
// MAGIC order by subscriber_id 

// COMMAND ----------

// MAGIC %sql
// MAGIC select *
// MAGIC 
// MAGIC from bdp.plans pl where plan_year = 2021 --and lob = 'MA'
// MAGIC and market_state = 'FL'

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

val timestamps = dbutils.fs.ls("/mnt/datalake/dev/source/isuipa/online_insight/history/")
val path = timestamps.sortWith(_.path > _.path)(0).path
val Filename = dbutils.fs.ls(path)(0).path
val isuipa = spark.read
                   .format("csv")
                   .option("header", "false")
                   .option("delimiter", "|")
                   .load(Filename)


isuipa.createOrReplaceTempView("isuipa")
var colNames = Seq(
"TradingPartnerId",
"SpoeId",
"TenantId",
"HiosId",
"QhpidLookupKey",
"ApplicationExtractDate",
"ApplicationExtractTime",
"PolicyLastMaintenanceDate",
"PolicyLastMaintenanceTime",
"ApplicationId",
"TransactionTypeCode",
  //add this to enrollment - if = 3 means the member has cancelled, also look if the benefit start and end date are the same, there is a cancellation
"AgentBrokerFirstName",
  //add this to enrollment - critical
"AgentBrokerMiddleName",
  //add this to enrollment - critical
"AgentBrokerLastName",
  //add this to enrollment - critical
"AgentBrokerNpn",
   //add this to enrollment - critical
"AssistorTypeCode",
"IndividualRelationshipCode",
"SubscriberIndicator",
"DateOfDeath",
"ExchangeAssignedSubscriberId",
"ExchangeAssignedMemberId",
"IssuerAssignedMemberId",
"IssuerAssignedSubscriberId",
"QiLastName",
"QiFirstName",
"QiMiddleName",
"QiMaritalStatus",
"ResidentialAddressLine1",
"ResidentialAddressLine2",
"ResidentialCityName",
"ResidentialStateCode",
"ResidentialZIPCode",
"ResidentialCountyCode",
"PhoneNumber",
"SocialSecurityNumber",
"QIBirthDate",
"QIGender",
"TobaccoUseCode",
"RaceType",
"Ethnicity",
"LanguageCodeSpoken",
"LanguageCodeWritten",
"EmailAddress",
"MailingAddressLine1",
"MailingAddressLine2",
"MailingAddressCity",
"MailingAddressStateCode",
"MailingAddressZIPCode",
"CustodialParentLastName",
"CustodialParentFirstName",
"CustodialParentMailingAddressLine1",
"CustodialParentMailingAddressLine2",
"CustodialParentMailingAddressCity",
"CustodialParentStateCode",
"CustodialParentZIPCode",
"ResponsiblePersonLastName",
"ResponsiblePersonFirstName",
"ResponsiblePersonMailingAddressLine1",
"ResponsiblePersonMailingAddressLine2",
"ResponsiblePersonMailingAddressCity",
"ResponsiblePersonStateCode",
"ResponsiblePersonZIPCode",
"BenefitStartDate",
  //add this to enrollment
"BenefitEndDate",
  //add this to enrollment
"IssuerAssignedPolicyNumber",
"QHPIdentifier",
  //add this to enrollment (plan level)
"ConfirmationIndicator",
"Exchange-AssignedPolicyNumber",
"AllocatedAPTCAmount",
"AppliedAPTCAmount",
"AppliedAPTCEffectiveDate",
"AppliedAPTCEndDate",
"CSRAmount",
"CSREffectiveDate",
"CSREndDate",
"TotalPremiumAmount",
"TotalPremiumEffectiveDate",
"TotalPremiumEndDate",
"IndividualPremiumAmount",
"IndividualPremiumEffectiveDate",
"IndividualPremiumEndDate",
"TotalResponsibilityAmount",
"TotalResponsibilityAmountEffectiveDate",
"TotalResponsibilityAmountEndDate",
"EndofYearTerminationIndicator",
"CancellationSource",
"TerminationSource",
"RFU1",
"RFU2",
"RFU3",
"RatingArea",
  //enrollment - critical
"PaidThroughDate",
  //enrollment - critical
"PaymentTransactionID",
"InsuranceApplicationOriginType",
"InsuranceLineofBusiness",
"CICCorrelationKey",
"RFU4",
"RFU5",
"EffectuationEffectiveDateFlag",
  //add this to enrollment (plan level)
"EffectuationEffectiveDate",
  //add this to enrollment (plan level)
"CancelledStatusDateFlag",
  //add this to enrollment (plan level)
"CancelledStatusDate",
  //add this to enrollment (plan level)
"TerminationStatusDateFlag",
  //add this to enrollment (plan level)
"TerminationStatusDate",
  //add this to enrollment (plan level)
"GracePeriodStart",
  //add this to enrollment (plan level)
"GracePeriodEnd",
  //add this to enrollment (plan level)
"GracePeriodIndicator",
  //add this to enrollment (plan level)
"EnrollmentSource",
  //add this to   //add this to enrollment (plan level) - place enrollment originated from
"DateEnrollmentEnteredatEnrollmentSource",
  //add this to enrollment (plan level)
"ProviderNPI",
"SecondaryPhoneNumber",
"CommPrefINV",
"CommPrefLetters",
"CommPrefEOB",
"CommPrefContact",
"CustodialParentEmail",
"CustodialParentPhone",
"ResponsiblePersonEmail",
"ResponsiblePersonPhone",
"OnHoldFlag",
  //enrollment - critical
"IssuerAssignedRecordTraceNumber",
"GracePeriodNumber",
  //add this to enrollment (plan level) 0-2 months of GP
"EffectiveDateDTP348",
"Market",
"UniversalID",
"PersonCode",
"ISA06IssuerID",
"CoverageYear"
);


val subset = isuipa.toDF(colNames = colNames: _*)
  .na.fill("") // remove nulls
subset.createOrReplaceTempView("isuipa_new")
display(subset)

// COMMAND ----------

