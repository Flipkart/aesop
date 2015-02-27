package com.flipkart.aesop.avro.schemagenerator.test.main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.aesop.avro.schemagenerator.main.SchemaGenerator;
import com.flipkart.aesop.avro.schemagenerator.mysql.DataSourceConfig;

/**
 * <code>SchemaGeneratorTest</code> test for SchemaGenerator class.
 * @author yogesh.dahiya
 */

public class SchemaGeneratorTest
{
	SchemaGenerator schemaGenerator;

	/**
	 * Setup schema generator.
	 * @throws Exception the exception
	 */
	public void setupSchemaGenerator() throws Exception
	{

		List<DataSourceConfig> dataSourceConfigs = new ArrayList<DataSourceConfig>();
		Map<String, List<String>> tablesInclusionListMap = new HashMap<String, List<String>>();
		Map<String, List<String>> tablesExclusionListMap = new HashMap<String, List<String>>();

		DataSourceConfig dataSourceConfig = new DataSourceConfig();

		dataSourceConfig.setDbName("payment");
		dataSourceConfig.setHostName("localhost");
		dataSourceConfig.setUserName("root");
		dataSourceConfig.setPassword("");

		dataSourceConfigs.add(dataSourceConfig);
		List<String> dbExclusionTableList =
		        Arrays.asList("ABBucket,ABBucketResponse,ABFeature,ABFeatureResponse,AccountRequest,AuditTrail,BankBins,Banks,BlackListData,BrowserRedirect,CTypeFilter,CTypeFilter_copy,CardAuxInfo,CardBins,CardBinsPriority,CardInfo,CardPrefix,CardSaveBillingAddress,CardSaveCheck,CardUserProfile,ClientConfig,ClientPGParam,ClusterData,Country,Country_copy,CronReportContents,CronReportMaster,CronReportNotification,CssAuthUsers,CssPreferences,DownTimeInfo,DownTimeMsg,FDSMetrics,FDSVerificationData,FlipkartAccountAutoLoginMapping,FraudStatus,JaffaAudit,MccSectorMapping,MerchantBankDetails,MerchantBusinessContacts,MerchantBusinessDetails,MerchantCategoryCodes,MerchantConfig,MerchantFee,MerchantFraudConfig,MerchantKeys,MerchantOffers,MerchantPGMapping,MerchantPGParam,MerchantRefundConfig,MerchantRelationshipContact,MerchantSession,MerchantViewData,MerchantWhiteListURL,MigrationRecord,NotificationConfig,NotificationSetting,NotifiedUsersInviteStatus,OTPServiceResponse,OfferAudit,OfferDetails,PGBankDetails,PGConfig,PGConfigNew,PGContacts,PGCounter,PGFee,PGFilter,PGMaping,PGMapingNew,PGMappingRules,PGProperty,PasswordRecovery,PayZippyMailNotifyStatus,PayZippyUserAudit,PayZippyUserBillingAddress,PayZippyUserCards,PayZippyUserProfile,PaymentConfig,PaymentRequest,PayzippyAccountEmailidMapping,PayzippyContactUs,PgBankMap,PrefInfo,PropertyList,PushNotificationAudit,Rate,RateCard,RateCardDetails,Refund,RetryRecommendation,Rule,RuleDSL,RuleDefinition,RuleImport,RulePackage,SIGNAL_LIST,SectorBaseRateMapping,SignalThresholdData,SignupOfferDetails,SignupOfferStatus,SignupOffersData,SignupOffersTransaction,TIME_LIST,ThrottlingAudit,ThrottlingConfig,TransactionsNew,URLConfig,UserAddress,UserAddressMap,UserData,UserLoginFailAudit,UserRememberMeConfig,UserSession,UserSessionFingerPrint,UserSessionTransaction,VerifiedIds,WalletHistory,WalletHistoryReport,Wallets,WhiteListCountries,audit_log,commonPassword,cssVersion,fds_alert_action,fds_alert_action_reason,fds_alert_verification,fds_result,fds_result_metrics,fraud_detector,product_type_weight,schema_version,testBin,urlvalidator"
		                .split(","));
		tablesExclusionListMap.put("payment", dbExclusionTableList);

		this.schemaGenerator = new SchemaGenerator(dataSourceConfigs, tablesInclusionListMap, tablesExclusionListMap);

	}

	/**
	 * Test suite.
	 * @throws Exception the exception
	 */
	public void testSuite() throws Exception
	{
		System.out.println("Test Result for single table : ");
		testForSingleTable();
		System.out.println("Test Result for all tables : ");
		testForAllTables();
	}

	/**
	 * Test for single table.
	 * @throws Exception
	 */
	public void testForSingleTable() throws Exception
	{
		System.out.println(schemaGenerator.generateSchema("payment", "TransactionAuditDetail"));

	}

	/**
	 * Test for all tables.
	 * @throws Exception the exception
	 */
	public void testForAllTables() throws Exception
	{
		Map<String, String> tableNameToSchema = schemaGenerator.generateSchemaForAllTables("payment");
		for (String tableName : tableNameToSchema.keySet())
		{
			System.out.println("\n=====" + tableName + "=====\n");
			System.out.println(tableNameToSchema.get(tableName));
			System.out.println("\n=====End=====\n");
		}
	}
}
