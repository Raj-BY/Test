# Databricks notebook source
# MAGIC %run /Users/christopher.schwandt@jda.com/Common_Params_Prd

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

obj_nm = "Account"
src_name = "Salesforce" #Data source name
lst =["NAME","ANNUALREVENUE","BILLINGCOUNTRY","BILLINGSTATE","DESCRIPTION","JDA_MAINTENANCE_REVENUE__C","RETAIL_REVENUE__C","ShippingCountry"]
td = trstd_decrypt(src_name,obj_nm,lst)
AccountDF = td.trustd_df()
AccountDF = AccountDF.filter("IsDeleted = false")
AccountDF.createOrReplaceTempView("Account")

CustomerReferenceBase= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/CustomerReferenceBase")
CustomerReferenceBase.createOrReplaceTempView("CustomerReferenceBase")


CustomerReference= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/CustomerReference")
CustomerReference.createOrReplaceTempView("CustomerReference")


AccountTeam= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/AccountTeam")
AccountTeam.createOrReplaceTempView("AccountTeam")


Pipeline= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/Pipeline")
Pipeline.createOrReplaceTempView("Pipeline")


GPSDF= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/GPS")
GPSDF.createOrReplaceTempView("GPS")


GPSTbl= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/GPSTbl")
GPSTbl.createOrReplaceTempView("GPSTbl")


AccountHistory= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/AccountHistory")
AccountHistory.createOrReplaceTempView("AccountHistory")


AccountHierarchy= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/AccountHierarchy")
AccountHierarchy.createOrReplaceTempView("AccountHierarchy")


CHSRenewals= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/CHSRenewals")
CHSRenewals.createOrReplaceTempView("CHSRenewals")


Entitlements= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/Entitlements")
Entitlements.createOrReplaceTempView("Entitlements")


EntitlementsSite= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/EntitlementsSite")
EntitlementsSite.createOrReplaceTempView("EntitlementsSite")


Version= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/Version")
Version.createOrReplaceTempView("Version")


User= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/User")
User.createOrReplaceTempView("User")


Product= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/Product")
Product.createOrReplaceTempView("Product")


ACV= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/ACV")
ACV.createOrReplaceTempView("ACV")

#Customer
dfCust = spark.read.format("delta").load(datalake_refinedpath+"CommonDimensions/CustomerDelta")
dfCust.createOrReplaceTempView("Customer")


ProductHierarchyDF = spark.read.json(datalake_refinedpath+'CommonDimensions/ProductHierarchy')
ProductHierarchyDF.createOrReplaceTempView("ProductHierarchy")

obj_nm = "Contact" #Table name
src_name = "Salesforce" #Data source name
lst =["FirstName", "LastName", "Account_Name__c"]
td = trstd_decrypt(src_name,obj_nm,lst)
ContactDF = td.trustd_df()
ContactDF.createOrReplaceTempView("Contact")


SupportWebinarData = spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/SupportWebinarData")
SupportWebinarData.createOrReplaceTempView("SupportWebinarData")

FocusDataFinal = spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/FocusDataFinal")
FocusDataFinal.createOrReplaceTempView("FocusDataFinal")

ReferenceItemCategory = spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/ReferenceItemCategory")
ReferenceItemCategory.createOrReplaceTempView("ReferenceItemCategory")


SupportWebinarFinal = spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/SupportWebinarFinal")
SupportWebinarFinal.createOrReplaceTempView("SupportWebinarFinal")

#obj_nm = "Webinar_Data" #Table name
#src_name = "MappingTables/CHS_2.0" #Data source name
#lst =["ParticipantName", "ParticipantEmail"]
#td = trstd_decrypt(src_name,obj_nm,lst)
#WebinarDataDF = td.trustd_df()
#WebinarDataDF.createOrReplaceTempView("SupportWebinarFinal")

obj_nm = "CampaignMember" #Table name
src_name = "Salesforce" #Data source name
CampaignMemberDF = spark.read.format("delta").load(datalake_path+src_name+'/'+obj_nm)
CampaignMemberDF = CampaignMemberDF.select("CampaignId","ContactId")
lst =[]
td = trstd_decrypt_df(CampaignMemberDF,src_name,obj_nm,lst)
CampaignMemberDF = td.trustd_withdf()
CampaignMemberDF.createOrReplaceTempView("CampaignMember")

obj_nm = "Campaign" #Table name
src_name = "Salesforce" #Data source name
CampaignDF = spark.read.format("delta").load(datalake_path+src_name+'/'+obj_nm)
CampaignDF = CampaignDF.select("Type", "Name","StartDate", "ID")
lst =[]
td = trstd_decrypt_df(CampaignDF,src_name,obj_nm,lst)
CampaignDF = td.trustd_withdf()
CampaignDF.createOrReplaceTempView("Campaign")

# COMMAND ----------

QualtricsVOC2020Distributions=spark.read.format("json").load('abfss://prdlakefilesystem@adluse2appdpmprd001.dfs.core.windows.net/jdaitgdatalake/REFINED/UseCases/Survey Metrics/QualtricsVOC2020Distributions')
QualtricsVOC2020Distributions.createOrReplaceTempView("QualtricsVOC2020Distributions")

#[Yesterday 1:19 PM] Jyothi Suragani
#    df= spark.read.format('delta').load("abfss://prdlakefilesystem@adluse2appdpmprd001.dfs.core.windows.net/jdaitgdatalake/REFINED/UseCases/Survey #Metrics/qualtricsdistributions")

QualtricsDistributions=spark.read.format("delta").load('abfss://prdlakefilesystem@adluse2appdpmprd001.dfs.core.windows.net/jdaitgdatalake/REFINED/UseCases/Survey Metrics/qualtricsdistributions')
lst =["SurveyName"]
td = trstd_decrypt_df(QualtricsDistributions,src_name,obj_nm,lst)
QualtricsDistributions.createOrReplaceTempView("QualtricsDistributions")


obj_nm = "ConsultingResponses" #Table name
src_name = "Qualtrics" #Data source name
df = spark.read.format("delta").load(datalake_path+src_name+'/'+obj_nm)
lst =["RecipientFirstName", "RecipientLastName"]
td = trstd_decrypt_df(df, src_name,obj_nm,lst)
ConsultingResponsesDF = td.trustd_withdf()
ConsultingResponsesDF.createOrReplaceTempView("ConsultingResponses")

obj_nm = "VOC2020Responses" #Table name
src_name = "Qualtrics_Archive" #Data source name
df = spark.read.format("delta").load(datalake_path+src_name+'/'+obj_nm)
lst =["RecipientFirstName", "RecipientLastName", "RecipientEmail"]
td = trstd_decrypt_df(df,src_name,obj_nm,lst)
VOC2020ResponsesDF = td.trustd_withdf()
VOC2020ResponsesDF.createOrReplaceTempView("VOC2020Responses")

obj_nm = "ClosedCaseResponses" #Table name
src_name = "Qualtrics" #Data source name
df = spark.read.format("delta").load(datalake_path+src_name+'/'+obj_nm)
lst =["RecipientFirstName", "RecipientLastName"]
td = trstd_decrypt_df(df,src_name,obj_nm,lst)
ClosedCaseResponsesDF = td.trustd_withdf()
ClosedCaseResponsesDF.createOrReplaceTempView("ClosedCaseResponses")


obj_nm = "Rewards" #Table name
src_name = "RFP" #Data source name
df = spark.read.format("delta").load(datalake_path+src_name+'/'+obj_nm)
lst =["Customer","Points"]
td = trstd_decrypt_df(df,src_name,obj_nm,lst)
RewardsDF = td.trustd_withdf()
RewardsDF.createOrReplaceTempView("Rewards")


# COMMAND ----------

obj_nm = "ClosedCaseResponses2021" #Table name
src_name = "Qualtrics" #Data source name
df = spark.read.format("delta").load(datalake_path+src_name+'/'+obj_nm)
lst =["RecipientFirstName", "RecipientLastName"]
td = trstd_decrypt_df(df,src_name,obj_nm,lst)
ClosedCaseResponsesDF = td.trustd_withdf()
ClosedCaseResponsesDF.createOrReplaceTempView("ClosedCaseResponses2021")


#obj_nm = "VocResponses2021" #Table name
#src_name = "Qualtrics" #Data source name
#df = spark.read.format("delta").load(datalake_path+src_name+'/'+obj_nm)
#lst =["RecipientFirstName", "RecipientLastName", "RecipientEmail"]
#td = trstd_decrypt_df(df,src_name,obj_nm,lst)
#VocResponses2021DF = td.trustd_withdf()
#VocResponses2021DF.createOrReplaceTempView("VocResponses2021")

VOC2021Responses=spark.read.format("delta").load('abfss://prdlakefilesystem@adluse2appdpmprd001.dfs.core.windows.net/jdaitgdatalake/TRUSTED/Qualtrics/VocResponses2021')
VOC2021Responses.createOrReplaceTempView("VOC2021Responses")

# COMMAND ----------

#ConsultingSurvey

ConsultingSurvey = spark.sql("""
select cast(cast(EndDate as string) as timestamp) as Response_Recieved_Date, 
                                           ACC.Name as Company_Name, ACC.ID as AccountID,
                                           'Consulting Survey' as Survey_Name, '' as Case_ID, 
                                           concat(RecipientFirstName,  ' ', RecipientLastName) as Contact_Name, 
                                           cast(purchase_influence_level__c as string) as Purchase_Influence_Level, 
                                           'Likely To Recommend' as Primary_Score_Type, 
                                           cast(cast(CR.Q2_3 as string) as int) as Primary_Score, 
                                           CASE WHEN cast(cast(CR.Q2_3 as string) as int)>8 THEN 'Promoter'  WHEN cast(cast(CR.Q2_3 as string) as int)>6 THEN 'Passive'   
                                           ELSE 'Detractor' END AS NPSCategory,
                                           cast(CR.Q2_2 as string) as Primary_Comment
              from (select * from ConsultingResponses 
                                           where datediff(current_date, cast(cast(EndDate as string) as date)) <= 365
                                                                        and Q2_3 is not null) CR
left join CustomerReferenceBase ACC
on cast(CR.Company_identifier__c as string) = ACC.ID
""")

ConsultingSurvey.createOrReplaceTempView("ConsultingSurvey")

# COMMAND ----------

#%sql

#select * from ConsultingSurvey
#where Company_Name like '%Ford%Motor%'
#and Primary_Score is not null

# COMMAND ----------

#%sql

#select count(*) from ConsultingSurvey

# COMMAND ----------

#MidProjectSurvey

MidProjectSurvey = spark.sql("""
select cast(cast(CR.EndDate as string) as timestamp) as Response_Recieved_Date, 
                                           ACC.Name as Company_Name, ACC.ID as AccountID,
                                           'MidProject Survey' as Survey_Name, '' as Case_ID, 
                                           concat(RecipientFirstName,  ' ', RecipientLastName) as Contact_Name, 
                                           cast(purchase_influence_level__c as string) as Purchase_Influence_Level, 
                                           'Overall Satisfaction' as Primary_Score_Type, 
                                           cast(cast(CR.Q2_1 as string) as int) as Primary_Score, 
                                           '' AS NPSCategory,
                                           cast(CR.Q2_2 as string) as Primary_Comment
              from (select * from ConsultingResponses 
                                           where datediff(current_date, cast(cast(EndDate as string) as date)) <= 365
                                                                        and cast(MidProject as string) ='True'
                                                                        and Q2_1 is not null) CR
left join CustomerReferenceBase ACC
on cast(CR.Company_identifier__c as string)  = ACC.ID
""")

MidProjectSurvey.createOrReplaceTempView("MidProjectSurvey")

# COMMAND ----------

#RelationshipSurvey

RelationshipSurvey = spark.sql("""
select cast(cast(RR.EndDate as string) as timestamp) as Response_Recieved_Date, ACC.Name as Company_Name, 
                                           ACC.ID as AccountID,
                                           'Relationship Survey' as Survey_Name, '' as Case_ID, 
                                           concat(RecipientFirstName,  ' ', RecipientLastName) as Contact_Name, 
                                           cast(purchase_influence_level__c as string) as Purchase_Influence_Level, 
                                           'Likely To Recommend' as Primary_Score_Type, 
                                           cast(cast(RR.Q01 as string) as int) as Primary_Score, 
                                           CASE WHEN cast(cast(cast(RR.Q01 as string) as int) as int) >8 THEN 'Promoter'  WHEN cast(cast(cast(RR.Q01 as string) as int) as int) >6 THEN 'Passive'   
                                           ELSE 'Detractor' END AS NPSCategory,
                                           cast(RR.ReasonforLikelihoodtorecommend as string) as Primary_Comment
                             from (select * from VOC2020Responses
                                                          where datediff(current_date, cast(cast(EndDate as string) as date)) <= 365
                                                          and Q01 is not null) RR
left join CustomerReferenceBase ACC
on cast(RR.Company_identifier__c as string) = ACC.ID

union all

select cast(cast(RR.EndDate as string) as timestamp) as Response_Recieved_Date, ACC.Name as Company_Name, 
                                           ACC.ID as AccountID,
                                           'Relationship Survey' as Survey_Name, '' as Case_ID, 
                                           concat(RecipientFirstName,  ' ', RecipientLastName) as Contact_Name, 
                                           cast(purchase_influence_level__c as string) as Purchase_Influence_Level, 
                                           'Likely To Recommend' as Primary_Score_Type, 
                                           cast(cast(RR.Q01 as string) as int) as Primary_Score, 
                                           CASE WHEN cast(cast(cast(RR.Q01 as string) as int) as int) >8 THEN 'Promoter'  WHEN cast(cast(cast(RR.Q01 as string) as int) as int) >6 THEN 'Passive'   
                                           ELSE 'Detractor' END AS NPSCategory,
                                           cast(RR.ReasonforLikelihoodtorecommend as string) as Primary_Comment
                             from (select * from VOC2021Responses
                                                          where /**datediff(current_date, cast(cast(EndDate as string) as date)) <= 365
                                                          and**/ Q01 is not null) RR
left join CustomerReferenceBase ACC
on cast(RR.Company_identifier__c as string) = ACC.ID
""")

RelationshipSurvey.createOrReplaceTempView("RelationshipSurvey")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from RelationshipSurvey where Company_Name like '%Sony%'

# COMMAND ----------

#CSATSurvey

CSATSurvey = spark.sql("""
select cast(cast(CCR.EndDate as string) as timestamp) as Response_Recieved_Date, ACC.Name as Company_Name, 
                                           ACC.ID as AccountID,
                                           'CSAT Survey' as Survey_Name, cast(CaseNumber as string) as Case_ID, 
                                           concat(RecipientFirstName,  ' ', RecipientLastName) as Contact_Name, 
                                           '' as Purchase_Influence_Level, 
                                           'Overall Satisfaction' as Primary_Score_Type, 
                                           cast(cast(CCR.Q2_2 as string) as int) as Primary_Score, 
                                           '' as NPSCategory,
                                           cast(CCR.Q2_3 as string) as Primary_Comment
                             from (select * from ClosedCaseResponses 
                                                                        where datediff(current_date, cast(cast(EndDate as string) as date)) <= 365
                                                                                      and Q2_2 is not null) CCR
left join CustomerReferenceBase ACC
on cast(CCR.Company_identifier__c as string) = ACC.ID

union all

select cast(cast(CCR.EndDate as string) as timestamp) as Response_Recieved_Date, ACC.Name as Company_Name, 
                                           ACC.ID as AccountID,
                                           'CSAT Survey' as Survey_Name, cast(CaseNumber as string) as Case_ID, 
                                           concat(RecipientFirstName,  ' ', RecipientLastName) as Contact_Name, 
                                           '' as Purchase_Influence_Level, 
                                           'Overall Satisfaction' as Primary_Score_Type, 
                                           cast(cast(CCR.Q01 as string) as int) as Primary_Score, 
                                           '' as NPSCategory,
                                           cast(CCR.Q01A as string) as Primary_Comment
                             from (select * from ClosedCaseResponses2021 
                                                                        where datediff(current_date, cast(cast(EndDate as string) as date)) <= 365
                                                                                      and Q01 is not null) CCR
left join CustomerReferenceBase ACC
on cast(CCR.Company_identifier__c as string) = ACC.ID
""")

CSATSurvey.createOrReplaceTempView("CSATSurvey")

# COMMAND ----------

#%sql

#select * from CSATSurvey
#where Company_Name like '%Linfox%'

# COMMAND ----------

#%sql

#select * from CSATSurvey

# COMMAND ----------

#TempSurvey

TempSurvey = spark.sql("""
select SR.*, case when AH.Parent_Account is null then SR.Company_Name else AH.Parent_Account end as Parent_Account 
from
(select * from ConsultingSurvey
union
select * from MidProjectSurvey
union
select * from RelationshipSurvey
union
select * from CSATSurvey) SR
    left join (select * from AccountHierarchy where Type = 'Site')  AH
    on AH.Child_Account = SR.Company_Name
where Primary_Score is not null
""")

TempSurvey.createOrReplaceTempView("TempSurvey")

# COMMAND ----------

obj_nm = "VOC2020Responses" #Table name
src_name = "Qualtrics_Archive" #Data source name
df = spark.read.format("delta").load(datalake_path+src_name+'/'+obj_nm)
lst =["RecipientFirstName", "RecipientLastName", "RecipientEmail"]
td = trstd_decrypt_df(df,src_name,obj_nm,lst)
VOC2020ResponsesDF = td.trustd_withdf()
VOC2020ResponsesDF.createOrReplaceTempView("VOC2020Responses")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select cast(cast(RR.EndDate as string) as timestamp) as Response_Recieved_Date, ACC.Name as Company_Name, 
# MAGIC                                            ACC.ID as AccountID,
# MAGIC                                            'Relationship Survey' as Survey_Name, '' as Case_ID, 
# MAGIC                                            concat(RecipientFirstName,  ' ', RecipientLastName) as Contact_Name, 
# MAGIC                                            cast(purchase_influence_level__c as string) as Purchase_Influence_Level, 
# MAGIC                                            'Likely To Recommend' as Primary_Score_Type, 
# MAGIC                                            cast(cast(RR.Q01 as string) as int) as Primary_Score, 
# MAGIC                                            CASE WHEN cast(cast(cast(RR.Q01 as string) as int) as int) >8 THEN 'Promoter'  WHEN cast(cast(cast(RR.Q01 as string) as int) as int) >6 THEN 'Passive'   
# MAGIC                                            ELSE 'Detractor' END AS NPSCategory,
# MAGIC                                            cast(RR.ReasonforLikelihoodtorecommend as string) as Primary_Comment
# MAGIC                              from (select * from VOC2020Responses
# MAGIC                                                           where datediff(current_date, cast(cast(EndDate as string) as date)) <= 365
# MAGIC                                                           and Q01 is not null) RR
# MAGIC left join CustomerReferenceBase ACC
# MAGIC on cast(RR.Company_identifier__c as string) = ACC.ID

# COMMAND ----------

#SurveyPivot1

SurveyPivot1 = spark.sql("""
SELECT Company_Name, AccountID
	, Survey_Name
	, SUM(cast(ifnull(Detractor, 0) as int))/(SUM(ifnull(Detractor, 0)+ifnull(Passive,0)+ifnull(Promoter,0))) AS Detractor
	, SUM(cast(ifnull(Passive,0) as int))/(SUM(ifnull(Detractor, 0)+ifnull(Passive,0)+ifnull(Promoter,0))) As Passive
	, SUM(cast(ifnull(Promoter,0) as int))/(SUM(ifnull(Detractor, 0)+ifnull(Passive,0)+ifnull(Promoter,0))) AS Promoter  
FROM
 (select Response_Recieved_Date
				, Company_Name
				, Survey_Name
				, Primary_Score_Type
				, NPSCategory 
                , AccountID
			from TempSurvey
		  )base

		  PIVOT
		  (
		   COUNT(Primary_Score_Type)
            FOR NPSCategory IN ('Detractor','Passive','Promoter')
		  )
WHERE Survey_Name IN ('Cloud Survey','Consulting Survey','Relationship Survey')
Group by Company_Name, AccountID , Survey_Name
    
""")
SurveyPivot1.createOrReplaceTempView("SurveyPivot1")

# COMMAND ----------

#%sql

#select * from SurveyPivot1

# COMMAND ----------

#SurveyPivot2

SurveyPivot2 = spark.sql("""
              SELECT AccountID, Company_Name, ROUND(AVG(CAST(Primary_Score AS Float)),2) As AverageofPrimaryScore
              FROM TempSurvey
              WHERE Survey_Name LIKE '%CSAT%'
              GROUP BY Company_Name, AccountID
""")

SurveyPivot2.createOrReplaceTempView("SurveyPivot2")

# COMMAND ----------

#TempSurvey1Sites

TempSurvey1Sites = spark.sql("""
select SR.*, case when AH.Parent_Account is null then SR.Company_Name else AH.Parent_Account end as Parent_Account
                             from SurveyPivot1 SR
              left join (select * from AccountHierarchy where Type = 'Site')  AH
              on AH.Child_Account = SR.Company_Name
              """)

TempSurvey1Sites.createOrReplaceTempView("TempSurvey1Sites")

# COMMAND ----------

#%sql

#select * from TempSurvey1Sites

# COMMAND ----------

#TempSurvey2Sites

TempSurvey2Sites = spark.sql("""
    select SR.*, case when AH.Parent_Account is null then SR.Company_Name else AH.Parent_Account end as Parent_Account
                                 from SurveyPivot2 SR
                   left join (select * from AccountHierarchy where Type = 'Site')  AH
                   on AH.Child_Account = SR.Company_Name
""")

TempSurvey2Sites.createOrReplaceTempView("TempSurvey2Sites")

# COMMAND ----------

#SurResp 

SurResp = spark.sql("""
select AccountID, AccountName, Parent_Account, SurveyName, 
                                           sum(Response_Flag) as Responses, 
                                           sum(Survey_Sent_Flag) as Surveys from
(select distinct SR.*, case when AH.Parent_Account is null 
                                                                        then SR.AccountName 
                                                                        else AH.Parent_Account
                                                                        end as Parent_Account, SurveySent as Survey_Sent_Flag, 
                                                                        ResponseRecd as Response_Flag
                             from (SELECT *,
                                                                 CASE WHEN cast(ResponseRecievedDate as string) = '' 
																		or ifnull(ResponseRecievedDate, '') = ''
                                                                        or upper(ResponseRecievedDate) like '%N%'
																		THEN 0
                                    ELSE 1 END AS ResponseRecd,
                                    1 As SurveySent
                                    FROM 
                      (SELECT * FROM (select QDistributionID as DistributionID, INTType as SurveyName, 'null' as SurveyID, 
                      USR.FirstName as FirstName, USR.LastName as LastName, 
                      QD.ContactEmail as Email, ACC.Account_Name as AccountName, 
                      ACC.AccountID, cast(QD.PostedAt as string) as InvitationSentDate,
                      cast(R.ENDDATE as string) as ResponseRecievedDate, 'null' as SurveyStartDate,
                      'null' as OpenedDate, 'null' as EmailBounced, ContactID as PersonID, 
                      'null' as JCESProjectName, 'null' as JEMPhase, 'null' as GPSNumber, 'null' as CaseID, 
                      'null' as Asset, 'null' as Program, 'null' as Department
                      from QualtricsVOC2020Distributions QD
                      left join VOC2020Responses R
                             on cast(R.RecipientEmail as string) = QD.ContactEmail
                      join Contact USR
                             on QD.ContactID = USR.ID
                      join AccountHierarchy ACC
                             on USR.AccountID = ACC.AccountID 
                             WHERE DATEDIFF (current_date, qd.postedat) <= 365                                             
		union all
select DistributionID, SurveyName, SurveyID, FirstName, LastName, Email, AccountName, AccountID, cast(invitationsentdate as string) as InvitationSentDate,
/**        cast(concat(concat('20', right(replace(left(invitationsentdate, char_length(invitationsentdate)-5), '/', '-') ,2)), concat('-', replace(left(invitationsentdate, char_length(invitationsentdate)-8), '/', '-'))) as string) as InvitationSentDate,**/ ResponseReceivedDate as ResponseRecievedDate, SurveyStartedDate as SurveyStartDate, OpenedDate, EmailBounced, 
         PersonID, JCESProjectName, JEMPhase, GPSNumber, CaseID, Asset, Program, Department from QualtricsDistributions					 
WHERE datediff(current_date, cast(invitationsentdate as date)) <= 365
/**DATEDIFF (current_date, cast(concat(concat('20', right(replace(left(invitationsentdate, char_length(invitationsentdate)-5), '/', '-') ,2)), concat('-', replace(left(invitationsentdate, char_length(invitationsentdate)-8), '/', '-'))) as date)) <= 365       **/                                       
--and EmailBounced in( 'No','N') 
) a

 ) a) SR
              left join (select * from AccountHierarchy where Type = 'Site') AH
              on AH.Child_Account = SR.AccountName
			  ) RESPRATE
group by AccountID, Parent_Account, AccountName, SurveyName
""")

SurResp.createOrReplaceTempView("SurResp")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from QualtricsDistributions where AccountID = '0013900001ifBUQAA2'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from SurResp
# MAGIC where AccountName like '%Linfox%'
# MAGIC --and SurveyName = 'Qualtrics_ClosedCase_Survey'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from QualtricsDistributions
# MAGIC --where CaseID like '%2933019%' 
# MAGIC where upper(AccountName) like '%ABBVIE%'
# MAGIC --and 
# MAGIC --DistributionID = 'EMD_c12Z9ZQXGhAVFI3'
# MAGIC and cast(InvitationSentDate as date) between cast('2020-08-19' as date) and cast('2021-08-20' as date) 
# MAGIC and SurveyName = 'Qualtrics_ClosedCase_Survey'
# MAGIC --where 
# MAGIC --where upper(ResponseReceivedDate) like '%N%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from QualtricsDistributions
# MAGIC where ResponseReceivedDate not in ('', 'NaT', 'nan')
# MAGIC and AccountName like '%Linfox%'
# MAGIC and SurveyName = 'Qualtrics_ClosedCase_Survey'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select invitationsentdate, 
# MAGIC locate(' ', invitationsentdate)
# MAGIC , replace(trim(left(invitationsentdate,8)), '/', '-')
# MAGIC , cast(concat(concat('20', right(replace(left(invitationsentdate, char_length(invitationsentdate)-5), '/', '-') ,2)), concat('-', replace(left(invitationsentdate, char_length(invitationsentdate)-8), '/', '-'))) as string)
# MAGIC , cast(concat(concat('20', right(replace(left(invitationsentdate, char_length(invitationsentdate)-5), '/', '-') ,2)), concat('-', replace(left(invitationsentdate, char_length(invitationsentdate)-8), '/', '-'))) as date) from QualtricsDistributions
# MAGIC where ResponseReceivedDate <> ''
# MAGIC and AccountName like '%Linfox%'
# MAGIC and SurveyName = 'Qualtrics_ClosedCase_Survey'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select DistributionID, SurveyName, SurveyID, FirstName, LastName, Email, AccountName, AccountID, 
# MAGIC         cast(concat(concat('20', right(replace(left(invitationsentdate, char_length(invitationsentdate)-5), '/', '-') ,2)), concat('-', replace(left(invitationsentdate, char_length(invitationsentdate)-8), '/', '-'))) as string) as InvitationSentDate, ResponseReceivedDate, --SurveyStartDate, OpenedDate, EmailBounced, 
# MAGIC          PersonID, JCESProjectName, JEMPhase, GPSNumber, CaseID, Asset, Program, Department from QualtricsDistributions					 
# MAGIC WHERE --DATEDIFF (current_date, cast(concat(concat('20', right(replace(left(invitationsentdate, char_length(invitationsentdate)-5), '/', '-') ,2)), concat('-', --replace(left(invitationsentdate, char_length(invitationsentdate)-8), '/', '-'))) as date)) <= 365                                              
# MAGIC --and EmailBounced in( 'No','N') 
# MAGIC --and 
# MAGIC CaseID in ('02362136', '02360420')

# COMMAND ----------

#%sql

#select distinct SurveyName from SurResp where Parent_Account like '%Linfox%'



# COMMAND ----------

#%sql

#select RecipientEmail from VOC2020Responses limit 10

# COMMAND ----------

#%sql

#select * from QualtricsDistributions 		 
#WHERE DATEDIFF (current_date, cast(concat(concat('20', right(replace(left(invitationsentdate, char_length(invitationsentdate)-5), '/', '-') ,2)), concat('-', #replace(left(invitationsentdate, char_length(invitationsentdate)-8), '/', '-'))) as date)) <= 365                                              
#and EmailBounced in( 'No','N') 

# COMMAND ----------

#%sql

#select QD.* from QualtricsVOC2020Distributions  QD
#join Contact USR
#                             on QD.ContactID = USR.ID
#where AccountID = '0013900001dBLmcAAG'
#limit 10

# COMMAND ----------

#SurResp1

SurResp1 = spark.sql("""
              SELECT *
              FROM SurResp
              WHERE SurveyName in ('Qualtrics_ClosedCase_Survey','Relationship 2020','Qualtrics_Consulting_Survey')
""")

SurResp1.createOrReplaceTempView("SurResp1")

# COMMAND ----------

#SurRespSites

SurRespSites = spark.sql("""
              SELECT distinct SR.*, case when AH.Parent_Account is null 
                                                                                      then SR.AccountName 
                                                                                      else AH.Parent_Account
                                                                                      end as Parent_Account2
              FROM SurResp SR
              LEFT JOIN (select * from AccountHierarchy where Type = 'Site')  AH
                             ON AH.Child_Account = SR.AccountName

""")

SurRespSites.createOrReplaceTempView("SurRespSites")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from SurResp where Parent_Account like '%MGD MWS Sp. z o.o. Sp.k.%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from SurRespSites where Parent_Account like '%Linfox%'

# COMMAND ----------

#%sql

#select * from SurRespSites where Parent_Account like '%Panasonic%Corp%'

# COMMAND ----------

#SurResp1Sites

SurResp1Sites = spark.sql("""
              SELECT Parent_Account, SUM(CAST(Responses As Float))/SUM(Surveys) As SumofResponseRate
              FROM (select * from SurRespSites WHERE SurveyName like '%Relationship%') a
              GROUP BY Parent_Account
""")

SurResp1Sites.createOrReplaceTempView("SurResp1Sites")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from SurRespSites
# MAGIC where Parent_Account2 = 'MGD MWS Sp. z o.o. Sp.k.'

# COMMAND ----------

#%sql

#select * from SurResp1Sites where Parent_Account2 like '%Ford%Motor%' limit 10

# COMMAND ----------

#SurveyMetricSites

SurveyMetricSites = spark.sql("""              
SELECT Parent_Account, NPS, OSAT, Cloud, Consulting, Response
                             , Test1, Test2
                             , NPSScore+CloudScore+ConsultingScore As Test3
                             , CASE WHEN Test1+Test2+NPSScore+CloudScore+ConsultingScore < 0 THEN 0 ELSE Test1+Test2+NPSScore+CloudScore+ConsultingScore END As Overall
              FROM
              (
                             SELECT *
                                           , CASE WHEN NPS<0 OR OSAT<=6 OR Cloud<0 OR Consulting<0 THEN 0 ELSE 8 END As Test1
                                           , CASE WHEN Response IS NULL THEN 0
                                                                        WHEN Response = 0 THEN -4
                                                                        ELSE 0 END As Test2
                                           , CASE WHEN NPS>0 THEN 4 ELSE 0 END As NPSScore
                                           , CASE WHEN Cloud>0 THEN 4 ELSE 0 END As CloudScore
                                           , CASE WHEN Consulting>0 THEN 4 ELSE 0 END As ConsultingScore
                             FROM
                             (
                                           SELECT DISTINCT base.Parent_Account2 as Parent_Account
                                                          , max(CASE WHEN survey.Survey_Name LIKE '%Relationship%Survey%' THEN Promoter-Detractor END) As NPS
                                                          , MAX(AverageofPrimaryScore) As OSAT
                                                          , MAX(CASE WHEN survey.Survey_Name LIKE '%Cloud%Survey%' THEN Promoter-Detractor END) As Cloud
                                                          , MAX(CASE WHEN survey.Survey_Name LIKE '%Consulting%Survey%' THEN Promoter-Detractor END) As Consulting
                                                          , MAX(SumofResponseRate) As Response
                                           FROM 
                                           (
                                                          SELECT DISTINCT Parent_Account as Parent_Account2 FROM 
                                                          (
                                                                    select distinct Parent_Account from TempSurvey1Sites
                                                                    union
                                                                    select distinct Parent_Account from TempSurvey2Sites

                                                                    --SELECT * FROM SurRespSites
                                                                       
                                                          ) a
                                                          
                                           ) base
              
                                           LEFT JOIN TempSurvey1Sites survey
                                           ON base.Parent_Account2 = survey.Parent_Account
                
                                           LEFT JOIN TempSurvey2Sites csat
                                           ON base.Parent_Account2 = csat.Parent_Account
                
                                           LEFT JOIN SurResp1Sites response
                                           ON base.Parent_Account2 = response.Parent_Account
                                           GROUP BY base.Parent_Account2
                                           )A
                             )a
""")

SurveyMetricSites.createOrReplaceTempView("SurveyMetricSites")

# COMMAND ----------

#%sql

#select * from SurveyMetricSites

# COMMAND ----------

#0203ReferenceDataBase
ReferenceDataBase = spark.sql("""
select to_date(date, 'MM/dd/yyyy') as Date
		, customer
		, RewardItem
		, Recipient
		, Notes
from REWARDS
       where RewardItem <> 'POINTS EXPIRATION'
	   and RewardItem not like '%Adjus%'
""")
ReferenceDataBase.createOrReplaceTempView("ReferenceDataBase")

# Reference Data Table
# Table Name [020301 Reference Data]

ReferenceData = spark.sql("""
	SELECT distinct *
		, CASE WHEN DATEDIFF(current_date, cast(Date as date)) between 0 and 365 THEN 1 ELSE 0 END As DateFilter
	FROM ReferenceDataBase 
where rewarditem not in ('FOCUS')
""")
ReferenceData.createOrReplaceTempView("ReferenceData")

# Reference Data Table
# Table Name [020301 Reference Data_Sites]

ReferenceData_Sites = spark.sql("""
select REF.*, A.ID  as AccountID
		from ReferenceData REF
	left join (select * from AccountHierarchy where Type = 'Site')  AH
	on AH.Child_Account = REF.Customer
    left join CustomerReferenceBase A
    on Ref.Customer = A.Name
""")
ReferenceData_Sites.createOrReplaceTempView("ReferenceData_Sites")



# COMMAND ----------

# Reference Data Pivot
# Table Name [020302 Reference Data Pivot_Sites]

ReferenceDataPivot_Sites = spark.sql("""
SELECT a.Parent_Account
    , AccountID
    , SUM(null) as null
    , SUM(Site_Visit) as Site_Visit
    , SUM(Other) as Other
    
 FROM
	(
	  SELECT distinct case when AH.Parent_Account is null 
						then Customer 
						else AH.Parent_Account
						end as Parent_Account
              , Customer
              , Customer As Customer_Cnt
              , Date
              , RewardItem
              , case when RewardItem like '%Site%Visit%' 
                          or RewardItem like '%site%visit%'
                          or RewardItem like '%site%Visit%'
                          or RewardItem like '%Site%visit%' then 'Site_Visit' else 'Other' end as RewardCategory
	  , CASE WHEN DATEDIFF(current_date, rawdata.Date)<=365 THEN 1 ELSE 0 END As DateFilter
      , AH.TYPE
      , AH.AccountID 
	 
	 FROM ReferenceData rawdata
	 LEFT JOIN (select * from AccountHierarchy where Type in ('Site'))  AH
		ON AH.Account_Name = rawdata.Customer
        
	)a
	PIVOT
	(
	  COUNT(Customer_Cnt)
	  FOR RewardCategory IN ('Site_Visit',
                              'Other')
  )
 WHERE DateFilter = 1  

GROUP BY Parent_Account, AccountID
""")
ReferenceDataPivot_Sites.createOrReplaceTempView("ReferenceDataPivot_Sites")

# ReferenceDataPivot_Sites.na.fill(0)
# ReferenceDataPivot_Sites.createOrReplaceTempView("ReferenceDataPivot_Sites")



# COMMAND ----------

#FocusData

FocusData = spark.sql("""
select distinct 
    FOC.CompanyName
  , FOC.Event
  , FOC.ContactName
  , FOC.Email
  , case when AH.Parent_Account is null then FOC.CompanyName else AH.Parent_Account end as Parent_Account 
from (select * from FocusDataFinal where CompanyName not like '%#N/A%') FOC
	left join (select * from AccountHierarchy where Type = 'Site')  AH
      on upper(AH.Child_Account) = upper(FOC.CompanyName)
where (FOC.Event like '%2017%' or FOC.Event like '%2018%' or FOC.Event like '%2019%' or FOC.Event like '%2020%' or FOC.Event like '%2021%')
     and FOC.Event not like '%ICON %'
""")
FocusData.createOrReplaceTempView("FocusData")



# COMMAND ----------

# this is developed as a part for CustomerEngagementMetric for the Append Query. 

FocusMetric_Append = spark.sql("""
select  CompanyName 
--AccountID, CompanyName 
      , sum(Focus) as Focus
      --, Focus
      , (case when sum(Focus) <= approx_percentile(sum(Focus),0.5) OVER (PARTITION BY  SNo) then 2.5  else 5 end)/2  FocusMetric  
from (
        select 1 as  SNo 
              , upper(CompanyName) as CompanyName
              , AccountID
              , count (*) as  Focus  
              from (
                      select distinct FOC.CompanyName 
                                    , FOC.Event
                                    , FOC.ContactName 
                                    , FOC.Email 
                                    , AH.AccountID
                                    , case when AH.Parent_Account is null then FOC.CompanyName  
                                    else AH.Parent_Account
                                    end as Parent_Account 
                      from (
                              select * from  FocusDataFinal  where  CompanyName  not like '%#N/A%'
                             ) FOC
                        left join (select * from  AccountHierarchy  where Type = 'Site')  AH
                              on upper(AH.Child_Account) = upper(FOC.CompanyName)
                     
                    ) A
          where (Event like '%2017%' or Event like '%2018%' or Event like '%2019%' or Event like '%2020%' or Event like '%2021%')
          group by  upper(CompanyName), AccountID 
    ) a
    group by CompanyName, SNo
""")
FocusMetric_Append.createOrReplaceTempView("FocusMetric_Append")


SupportWebinar_Append = spark.sql("""
select AccountName
       , Webinar
       , (case when Webinar <= approx_percentile(Webinar,0.5) OVER (PARTITION BY  SNo) then 2.5  else 5 end)/2  WebinarMetric  
from (select 1 as SNo
            , AccountName
            , sum(cast(Flag as int)) as Webinar
        from (select * from 
                    (select Company as AccountName, case when EventStartDate  >= "2018-01-01" then '1' else 0 end as Flag from SupportWebinarFinal
                    ) where Flag = '1') a
		group by AccountName
      ) a 
""")
SupportWebinar_Append.createOrReplaceTempView("SupportWebinar_Append")

# COMMAND ----------

#%sql

#select * from FocusMetric_Append
#where upper(CompanyName) like '%BRIDGESTONE BANDAG - SC CLOUD%'

# COMMAND ----------

#CustomerEngagementMetric


CustomerEngagementMetric = spark.sql("""              
SELECT distinct ID, Name, CloudCustomer, NPSScore, Detractor, SurveyResponse, PromoterScoreVOC, PromoterScoreConsulting
                             , PromoterScoreCloud, SurveyMetric, ReferenceMetric
                             , SurveyMetric+ReferenceMetric As CustomerEngagment
                             , SiteVisits, OtherActivities
              FROM
              (
                             SELECT ID, Name
                                           , CloudCustomer, NPS*100 as NPSScore
                                           , CASE WHEN Test1 IS NULL THEN 8 ELSE Test1 End As Detractor
                                           , CASE WHEN Test2 IS NULL THEN 0 ELSE Test2 End As SurveyResponse
                                           , CASE WHEN NPS IS NOT NULL AND NPS > 0 THEN 4 ELSE 0 END As PromoterScoreVOC
                                           , CASE WHEN Consulting IS NOT NULL AND Consulting>0 THEN 4 ELSE 0 END As PromoterScoreConsulting
                                           , CASE WHEN Cloud IS NOT NULL AND Cloud > 0 THEN 4 ELSE 0 END As PromoterScoreCloud
                                           , CASE WHEN Overall IS NULL THEN 8 ELSE Overall END As SurveyMetric
                                          , CASE WHEN Site_Visit>0 THEN 20
                                                 WHEN Other >1 THEN 20
                                                  WHEN ifnull(Site_Visit,0) = 0 and Other = 1 THEN 15
                                                ELSE 0 END As ReferenceMetric
                                          , CASE WHEN Site_Visit>0 THEN 20 ELSE 0 End As SiteVisits
                                          , CASE WHEN Other > 1 THEN 20 
                                                  WHEN ifnull(Site_Visit,0) = 0 and Other = 1 THEN 15
                                              ELSE 0 END As OtherActivities
                                            , Case when FOC.FocusMetric  is null then 0 else FOC.FocusMetric  end as  FocusMetric 
                                            , case when WEB.WebinarMetric  is null then 0 else WEB.WebinarMetric  end as  WebinarMetric 
                             FROM (select ID, Name, CloudCustomer from CustomerReferenceBase
                                                                        WHERE Type = 'Client') cust
                             LEFT JOIN SurveyMetricSites surveymetric
                             ON cust.Name = surveymetric.Parent_Account

                             LEFT JOIN ReferenceDataPivot_Sites ref
                             ON cust.Name = ref.Parent_Account
                                                     
                              LEFT JOIN FocusMetric_Append FOC
                              ON Upper(Cust.Name)  = upper(FOC.CompanyName) 

                              LEFT JOIN SupportWebinar_Append WEB
                              ON Cust.Name  = WEB.AccountName
                                    )A

""")

CustomerEngagementMetric.createOrReplaceTempView("CustomerEngagementMetric")

# COMMAND ----------

#%sql

#select * from CustomerEngagementMetric

# COMMAND ----------

#saving the "0025_Support Webinar (5) " data to refined zone as a json file (physical file) 

SupportWebinar5 = spark.sql("""
      select REF.ID as AccountId
	, REF.NAME as AccountName
	, WEB.ProgramName as CampaignType
	, WEB.EventName as CompaignName
	, WEB.ParticipantName as CompaignMemberName
	, WEB.EventStartDate as CompaignStartDate  

from SupportWebinarFinal WEB
inner join CustomerReferenceBase REF
on WEB.Company = Ref.Name
""")
SupportWebinar5.createOrReplaceTempView ("SupportWebinar5")



# COMMAND ----------

# this data was already build in Azure on the Old subcription, where the same has been replciated to the New and current Subcription, Script source Srinivas Chukkala
SupportWebinar_OldSubcription = spark.sql("""
Select c.AccountId as AccountID
       , a.Name as AccountName
       , cdf.Type as CompaignType
       , cdf.Name as CampaignName
       , c.Name as CompaignMemberName
       , date_format(cdf.StartDate,'y-MM-dd') as CompaignStartDate
from CampaignMember cmDF
join campaign cdf
  on cmDF.CampaignId = cdf.Id
join Contact c
  on cmDF.ContactId = c.Id
join  (Select * from Account a where (Type ='Client' or (Jpower_No__c is not null and Type='Site'))) a
  on c.AccountId = a.Id
where (cdf.Type = 'Webinar') and (datediff(current_date,cdf.StartDate) <= 730)
""")
SupportWebinar_OldSubcription.createOrReplaceTempView ("SupportWebinar_OldSubcription")



#saving the "0025_Support Webinar (2) " data to refined zone as a json file (physical file) 
SupportWebinar2 = spark.sql("""
select *
from SupportWebinar_OldSubcription 
union all 
select *  from SupportWebinar5
""")
SupportWebinar2.createOrReplaceTempView ("SupportWebinar2")



# COMMAND ----------

#saving the "0025_Support_Webinar_Metric" data to refined zone as a json file (physical file) 

SupportWebinarMetric = spark.sql("""
select Distinct Company , Sum(WebinarMetric) as WebinarMetric
from 
(select distinct Company, Sum(flag) as WebinarMetric 
from (
      Select AccountName as Company,
      case when CompaignStartDate  >= "2018-01-01" then '1' else 0 end as Flag
      from SupportWebinar2
      where datediff(current_date, cast(CompaignStartDate as date)) <= 365
      ) A
where flag = '1'
Group by Company
)group by Company

""")
SupportWebinarMetric.createOrReplaceTempView("SupportWebinarMetric")


# COMMAND ----------

#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/TempSurvey",True)
#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/ReferenceData_Sites",True)
#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/SurRespSites",True)
#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/CustomerEngagementMetric",True)
#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/SupportWebinar5",True)
#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/SupportWebinar2",True)
#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/SupportWebinarMetric",True)
#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/FocusData",True)
#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/FocusMetric_Append",True)

# COMMAND ----------

TempSurvey.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/TempSurvey")

# COMMAND ----------

deltaPath = datalake_refinedpath +"Solutions/CHS/TempSurvey"
TempSurvey = "CREATE TABLE IF NOT EXISTS %s.CHS_SurveyResults USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
spark.sql(TempSurvey)

# COMMAND ----------

#%sql

#select * from corporateanalyticsedw_dev.CHS_SurveyResults
#where Survey_Name = 'CSAT Survey'

# COMMAND ----------

ReferenceData_Sites.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/ReferenceData_SitesTest")

deltaPath = datalake_refinedpath +"Solutions/CHS/ReferenceData_SitesTest"
ReferenceData_Sites = "CREATE TABLE IF NOT EXISTS %s.CHS_ReferenceDataTest USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
spark.sql(ReferenceData_Sites)

# COMMAND ----------

SurRespSites.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/SurRespSites")

deltaPath = datalake_refinedpath +"Solutions/CHS/SurRespSites"
SurRespSites = "CREATE TABLE IF NOT EXISTS %s.CHS_SurResponse USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
spark.sql(SurRespSites)

# COMMAND ----------

CustomerEngagementMetric.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/CustomerEngagementMetricTest")

deltaPath = datalake_refinedpath +"Solutions/CHS/CustomerEngagementMetricTest"
CustomerEngagementMetric = "CREATE TABLE IF NOT EXISTS %s.CHS_CustomerEngagementMetricTest USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
spark.sql(CustomerEngagementMetric)

# COMMAND ----------

# saving the dataset 
try:
  SupportWebinar5.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/SupportWebinar5")
except Exception as e:
  dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/SupportWebinar5",True)
  SupportWebinar5.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/SupportWebinar5")

deltaPath = datalake_refinedpath +"Solutions/CHS/SupportWebinar5"
SupportWebinar5 = "CREATE TABLE IF NOT EXISTS %s.CHS_SupportWebinar5 USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
spark.sql(SupportWebinar5)

# COMMAND ----------

# saving the dataset 
SupportWebinar2.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/SupportWebinar2")

deltaPath = datalake_refinedpath +"Solutions/CHS/SupportWebinar2"
SupportWebinar2 = "CREATE TABLE IF NOT EXISTS %s.CHS_SupportWebinar2 USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
spark.sql(SupportWebinar2)

# COMMAND ----------


# saving the dataset 
SupportWebinarMetric.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/SupportWebinarMetric")

deltaPath = datalake_refinedpath +"Solutions/CHS/SupportWebinarMetric"
SupportWebinarMetric = "CREATE TABLE IF NOT EXISTS %s.CHS_SupportWebinarMetric USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
spark.sql(SupportWebinarMetric)

# COMMAND ----------

#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/FocusMetric_Append",True)

# COMMAND ----------

#FocusData
FocusData.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/FocusData")

deltaPath = datalake_refinedpath +"Solutions/CHS/FocusData"
FocusData = "CREATE TABLE IF NOT EXISTS %s.CHS_FocusData USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
spark.sql(FocusData)

# COMMAND ----------

#FocusMetric_Append

FocusMetric_Append.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/FocusMetric_Append")

deltaPath = datalake_refinedpath +"Solutions/CHS/FocusMetric_Append"
FocusMetric_Append = "CREATE TABLE IF NOT EXISTS %s.CHS_FocusMetric USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
spark.sql(FocusMetric_Append)

# COMMAND ----------

#saving the "0025_Support Webinar " data to refined zone as a json file (physical file) 

SupportWebinarFinal = spark.sql("Select * from SupportWebinarFinal")
SupportWebinarFinal.createOrReplaceTempView("SupportWebinarFinal")


# saving the dataset 
#SupportWebinarFinal.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/SupportWebinarFinal")

deltaPath = datalake_refinedpath +"Solutions/CHS/SupportWebinarFinal"
SupportWebinarFinal = "CREATE TABLE IF NOT EXISTS %s.CHS_SupportWebinarFinal USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
spark.sql(SupportWebinarFinal)

# COMMAND ----------

CustomerEngagementMetricTest= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/CustomerEngagementMetricTest")
CustomerEngagementMetricTest.createOrReplaceTempView("CustomerEngagementMetricTest")

FocusMetricNew= spark.read.format("delta").load(datalake_refinedpath+"/Solutions/CHS/FocusMetric_Append")
FocusMetricNew.createOrReplaceTempView("FocusMetricNew")

SupportWebinarMetric= spark.read.format("delta").load(datalake_refinedpath +"Solutions/CHS/SupportWebinarMetric")
SupportWebinarMetric.createOrReplaceTempView("SupportWebinarMetric")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from CustomerEngagementMetricTest
# MAGIC where upper(Name) like '%ROHLIG%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from FocusMetricNew
# MAGIC where upper(CompanyName) like '%BRIDGESTONE BANDAG - SC CLOUD%'--'%ROHLIG SUUS LOGISTICS SA%'
# MAGIC --group by CompanyName
# MAGIC --having count(CompanyName) > 1 

# COMMAND ----------

CustomerEngagementFinal = spark.sql("""

select *, case when ReferenceMetric = 20 then 10 else (case when ReferenceMetric = 15 then 5 else 0 end) end as ReferenceMetricNew
        , case when FM.FocusMetric is null then 0 else FM.FocusMetric end as FocusMetricNew
        , case when WEB.WebinarMetric is null then 0 else WEB.WebinarMetric end as WebinarMetricNew
        , SurveyMetric 
          + case when ReferenceMetric = 20 
                  then 10 else 
                  (case when ReferenceMetric = 15 then 5 else 0 end) end 
          + case when FM.FocusMetric is null then 0 else FM.FocusMetric end        
          + case when WEB.WebinarMetric is null then 0 else WEB.WebinarMetric end 
          as CustomerEngagementNew
        from CustomerEngagementMetricTest CE
        left join FocusMetricNew FM
          on CE.Name = FM.CompanyName
        left join SupportWebinarMetric WEB
          on CE.Name = WEB.Company

""")

CustomerEngagementFinal.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/CustomerEngagementFinal")


# COMMAND ----------

#%sql

#select * from AccountHierarchy where Parent_Account like '%Panasonic%Corp%'

# COMMAND ----------

#%sql

#select * from corporateanalyticsedw_dev.CHS_CustomerEngagementMetric 
#where Name like '%Post%Office%'

# COMMAND ----------

#%sql

#select * from corporateanalyticsedw_dev.CHS_SurveyResults
#where Company_Name = 'Ford Motor Private Limited'
#and Survey_Name = 'Relationship Survey'

# COMMAND ----------

#%sql

#select * from corporateanalyticsedw_dev.CHS_SurveyResults
#where Company_Name = 'Ford Motor Private Limited'
#and Survey_Name = 'Consulting Survey'

# COMMAND ----------

#%sql

#select * from corporateanalyticsedw_dev.CHS_SurResponse 
#where AccountName like '%Panasonic%'

# COMMAND ----------

#SLA_Batch_Misses

#SLA_Batch_Misses.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(datalake_refinedpath+"Solutions/CHS/SLA_Batch_Misses")

#deltaPath = datalake_refinedpath +"Solution/CHS/SLA_Batch_Misses"
#SLA_Batch_Misses = "CREATE TABLE IF NOT EXISTS %s.CHS_SLA_Batch_Misses USING DELTA LOCATION \'%s\'"%(delta_dbname,deltaPath)
#spark.sql(SLA_Batch_Misses)

# COMMAND ----------

#dbutils.fs.rm(datalake_refinedpath+"Solution/CHS/GPSTbl",True)

# COMMAND ----------

#dbutils.fs.rm(datalake_refinedpath+"Solutions/CHS/CoreComplement",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from corporateanalyticsedw_dev.CHS_SurResponse
# MAGIC where AccountID = '0013900001ifBUQAA2'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from corporateanalyticsedw_dev.CHS_SurveyResults 
# MAGIC where Survey_Name in ('CSAT Survey', 'MidProject Survey', 'Consulting Survey')