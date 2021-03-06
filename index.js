
/****

********SunEdison*********

AUTHOR : Anurag Bhardwaj

****/

var pjson = require('./package.json');
var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
	region = "us-east-1";
	console.log("AWS Lambda Redshift Database Loader using default region " + region);
}

//Requiring aws-sdk. 
var aws = require('aws-sdk');
aws.config.update({
	region : region
});

//Requiring S3 module. 
var s3 = new aws.S3({
	apiVersion : '2006-03-01',
	region : region
});
//Requiring dynamoDB module. 
var dynamoDB = new aws.DynamoDB({
	apiVersion : '2012-08-10',
	region : region
});

//Requiring SNS module. 
var sns = new aws.SNS({
	apiVersion : '2010-03-31',
	region : region
});

require('./constants');
var kmsCrypto = require('./kmsCrypto');
kmsCrypto.setRegion(region);
var common = require('./common');
var async = require('async');
var uuid = require('node-uuid');
var pg = require('pg');
var upgrade = require('./upgrades');
var zlib = require('zlib');

//Connection string to connect to Redshift with username and password. 
var conString = "postgresql://abhardwaj:Master12@sunedisondatawarehouse.cgnr3c8sn1sz.us-west-2.redshift.amazonaws.com:5439/sunedison";

//Query string to insert data into Redshift. 
var queryTextInsertRequest = 'INSERT INTO suned_redshift (suned_cust_id, quote_system_size, quote_ef_cost_per_watt, quote_year1_production, cust_pre_payment, quote_master_lease_pay_esc_rate, quote_rebate, quote_hipbi_year1_value, quote_hipbi_tenure, quote_hipbi_annual_derate, quote_state_tax_rate, quote_current_utility_cost, quote_post_solar_utility_cost, quote_proposal_id, quote_call_version_id, quote_auth_code, system_module_id, system_module_quantity, system_inverter_id, system_inverter_quantity, system_mounting_type, contract_calcmap_current_date, contract_installer_client_name, contract_calcmap_dealer_name, contract_calcmap_howner_0_first_name, contract_calcmap_howner_0_last_name, contract_calcmap_howner_1_first_name, contract_calcmap_howner_1_last_name, contract_product_type, contract_calcmap_n_of_howners, contract_calcmap_howner_0_address, contract_calcmap_howner_0_city, contract_calcmap_howner_0_state, contract_calcmap_howner_0_zipcode, contract_calcmap_howner_0_phone, contract_calcmap_howner_0_email, contract_calcmap_howner_1_address, contract_calcmap_howner_1_city, contract_calcmap_howner_1_state, contract_calcmap_howner_1_zipcode, contract_calcmap_howner_1_phone, contract_calcmap_howner_1_email, contract_calcmap_howner_2_address, contract_calcmap_howner_2_city, contract_calcmap_howner_2_state, contract_calcmap_howner_2_zipcode, contract_calcmap_howner_2_phone, contract_calcmap_howner_2_email, contract_installer_client_phone, contract_production_0_col2, contract_calcmap_lifetime_kwh, array_number, module_type, module_quantity, shading, tilt, azimuth, orientation, monthly_production_values, degradation_rates) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60)';

var queryTextInsertResonse = 'INSERT INTO response (sunedcustid, pricingquoteid, customerleasepayments, downpayment, leaseterm, estimatedannualoutput, uniquefinancialrunid, terminationvalues, suned_timestamp, financialmodelversion, callversionid, guaranteedannualoutput) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)';

//Query string to fetch data from Redshift. 
var queryFetchRequest = 'SELECT * from suned_redshift where suned_cust_id = $1 order by array_number asc';

var queryFetchResponse = 'SELECT * from response where SunEdCustId = $1 and PricingQuoteId = $2';

// Main function for AWS Lambda
exports.handler = function(event, context) {

    // Get the object from the event and show its content type
    var bucket = event.Records[0].s3.bucket.name;

    var keyRequest = event.Records[0].s3.object.key.request;
    var keyResponse = event.Records[0].s3.object.key.response;

    s3.getObject({Bucket: bucket, Key: keyRequest}, function(err, dataRequest) {
        if (err) {
            console.log("Error getting object " + keyRequest + " from bucket " + bucket +
                ". Make sure they exist and your bucket is in the same region as this function.");
            context.fail ("Error getting file: " + err);      
        } else {
            console.log('CONTENT TYPE: Request : ', dataRequest.ContentType);
            var inbound_payload_request = JSON.parse(dataRequest.Body);
        	//insertData(inbound_payload_request);
            accessResponse(inbound_payload_request);
        }
    });

    var accessResponse = function(inbound_payload_request){
        s3.getObject({Bucket: bucket, Key: keyResponse}, function(err, dataResponse) {
            if (err) {
                console.log("Error getting object " + keyResponse + " from bucket " + bucket +
                    ". Make sure they exist and your bucket is in the same region as this function.");
                context.fail ("Error getting file: " + err);      
            } 
            else {
                console.log('CONTENT TYPE: Request : ', dataResponse.ContentType);
                var inbound_payload_response = JSON.parse(dataResponse.Body);
                insertData(inbound_payload_request, inbound_payload_response);
                //console.log(inbound_payload_request.Quote.SystemSize + " SPACE IN BETWEEN " + inbound_payload_response.SunEdCustId);
            }
        });

        //Method to insert data into Redshift. 
    	var insertData = function(inbound_payload_request, inbound_payload_response){

    		//Local variables definition to get data from JSON file. 
    		var SunEdCustId = inbound_payload_request.SunEdCustId;
            var SystemSize = inbound_payload_request.Quote.SystemSize;
            var EFCostPerWatt = inbound_payload_request.Quote.EFCostPerWatt;
            var Year1Production = inbound_payload_request.Quote.Year1Production;
            var CustomerPrepayment = inbound_payload_request.Quote.CustomerPrepayment;
            var MasterLeasePaymentEscalationRate = inbound_payload_request.Quote.MasterLeasePaymentEscalationRate;
            var Rebate = inbound_payload_request.Quote.Rebate;
            var HIPBIYear1Value = inbound_payload_request.Quote.HIPBIYear1Value;
            var HIPBITenure = inbound_payload_request.Quote.HIPBITenure;
            var HIPBIAnnualDerate = inbound_payload_request.Quote.HIPBIAnnualDerate;
            var StateTaxRate = inbound_payload_request.Quote.StateTaxRate;
            var CurrentUtilityCost = inbound_payload_request.Quote.CurrentUtilityCost;
            var PostSolarUtilityCost = inbound_payload_request.Quote.PostSolarUtilityCost;
            var ProposalID = inbound_payload_request.Quote.ProposalID;
            var CallVersionID = inbound_payload_request.Quote.CallVersionID;
            var AuthorizationCode = inbound_payload_request.Quote.AuthorizationCode;
            var ModuleId = inbound_payload_request.System.ModuleId;
            var ModuleQuantity = inbound_payload_request.System.ModuleQuantity;
            var InverterId = inbound_payload_request.System.InverterId;
            var InverterQuantity = inbound_payload_request.System.InverterQuantity;
            var MountingType = inbound_payload_request.System.MountingType;
     
            var currentDate = inbound_payload_request.Contract["calcMap.currentDate"];
            var installerClientName = inbound_payload_request.Contract["installer.client.name"];
            var dealerName = inbound_payload_request.Contract["calcMap.dealerName"];
            var homeownerList_0_firstName = inbound_payload_request.Contract["calcMap.homeownerList.0.firstName"];
            var homeownerList_0_lastName = inbound_payload_request.Contract["calcMap.homeownerList.0.lastName"];
            var homeownerList_1_firstName = inbound_payload_request.Contract["calcMap.homeownerList.1.firstName"];
            var homeownerList_1_lastName = inbound_payload_request.Contract["calcMap.homeownerList.1.lastName"];
            var product_type = inbound_payload_request.Contract["product_type"];
            var numberOfHomeowners = inbound_payload_request.Contract["calcMap.numberOfHomeowners"];

            var homeownerList_0_address = inbound_payload_request.Contract["calcMap.homeownerList.0.address"];
            var homeownerList_0_city = inbound_payload_request.Contract["calcMap.homeownerList.0.city"];
            var homeownerList_0_state = inbound_payload_request.Contract["calcMap.homeownerList.0.state"];
            var homeownerList_0_zipcode = inbound_payload_request.Contract["calcMap.homeownerList.0.zipCode"];
            var homeownerList_0_phone = inbound_payload_request.Contract["calcMap.homeownerList.0.phone"];
            var homeownerList_0_email = inbound_payload_request.Contract["calcMap.homeownerList.0.email"];

            var homeownerList_1_address = inbound_payload_request.Contract["calcMap.homeownerList.1.address"];
            var homeownerList_1_city = inbound_payload_request.Contract["calcMap.homeownerList.1.city"];
            var homeownerList_1_state = inbound_payload_request.Contract["calcMap.homeownerList.1.state"];
            var homeownerList_1_zipcode = inbound_payload_request.Contract["calcMap.homeownerList.1.zipCode"];
            var homeownerList_1_phone = inbound_payload_request.Contract["calcMap.homeownerList.1.phone"];
            var homeownerList_1_email = inbound_payload_request.Contract["calcMap.homeownerList.1.email"];

            var homeownerList_2_address = inbound_payload_request.Contract["calcMap.homeownerList.2.address"];
            var homeownerList_2_city = inbound_payload_request.Contract["calcMap.homeownerList.2.city"];
            var homeownerList_2_state = inbound_payload_request.Contract["calcMap.homeownerList.2.state"];
            var homeownerList_2_zipcode = inbound_payload_request.Contract["calcMap.homeownerList.2.zipCode"];
            var homeownerList_2_phone = inbound_payload_request.Contract["calcMap.homeownerList.2.phone"];
            var homeownerList_2_email = inbound_payload_request.Contract["calcMap.homeownerList.2.email"];

            var installerClientPhone = inbound_payload_request.Contract["installer.client.phone"];
            var productionList_0_col2 = inbound_payload_request.Contract["productionList.0.col2"];
            var lifeTimeKwh = inbound_payload_request.Contract["calcMap.lifetimekWh"];	

    		var suned_id = parseInt(SunEdCustId, 10);

            var SunEdCustIdResponse = inbound_payload_response.SunEdCustId;
            var PricingQuoteId = inbound_payload_response.PricingQuoteId;
            var customerLeasePaymentsArray = [];
            var DownPayment = inbound_payload_response.DownPayment;
            var LeaseTerm = inbound_payload_response.LeaseTerm; 
            var estimatedAnnualOutputArray = [];
            var EstimatedAnnualOutput = inbound_payload_response.EstimatedAnnualOutput;
            var UniqueFinancialRunId = inbound_payload_response.UniqueFinancialRunId;
            var terminationValuesArray = [];
            var Suned_Timestamp = inbound_payload_response.Timestamp;
            var FinancialModelVersion = inbound_payload_response.FinancialModelVersion;
            var CallVersionId = inbound_payload_response.CallVersionId; 
            var GuaranteedAnnualOutput = inbound_payload_response.GuaranteedAnnualOutput;   

            for(var i=0;i<LeaseTerm;i++){
                customerLeasePaymentsArray.push(inbound_payload_response.CustomerLeasePayments[i]);
            }

            for(var j=0;j<LeaseTerm;j++){
                terminationValuesArray.push(inbound_payload_response.TerminationValues[j]);
            } 

            var NewEstimatedAnnualOutput = EstimatedAnnualOutput.substring(1,EstimatedAnnualOutput.length-1);
            var NewGuaranteedAnnualOutput = GuaranteedAnnualOutput.substring(1,GuaranteedAnnualOutput.length-1)

    		//Establishing connection to Redshift using postgres. 
    		pg.connect(conString, function(err,client){
    			if(err){
    				return console.log("Connection error. ", err);
    			}

    			console.log("Connection Established under fetch");

    			//Querying redshift. 
    			client.query(queryFetchRequest, [SunEdCustId], function(err,result){
    				if(err){
    					console.log("Error returning query", err);
    					context.done("Fatal Error");
    				}
    				console.log("Number of rows: ", result.rows.length);
    				console.log("Number of rows from JSON: " + inbound_payload_request.Array.length);

    				//Algorithm to check redundancy and add unique data into redshift. 
    				for(var m=0;m<inbound_payload_request.Array.length;m++){
                      	
                      	//Insert all the data from JSON file if no data exists in Redshift. 
                      	if(result.rows.length == 0){
                            console.log("No records in Redshift");
                            insertIntoRedshift(m);
                      	}

                      	//Check for duplicacy and insert rows to redshift. 
                      	else{
                      		for(var k=0;k<result.rows.length;k++){
    	                    	if(result.rows[k].suned_cust_id == SunEdCustId && result.rows[k].array_number == inbound_payload_request.Array[m].ArrayNumber){
    	                        	console.log("Duplicate Row Exists.");
    	                        	break;           
                            	}
    	                    	else if(k == result.rows.length-1){
    	                        	insertIntoRedshiftRequest(m);
    	                    	} 
    	                	}
                      	}   	
                	}
    			});

                client.query(queryFetchResponse, [SunEdCustIdResponse,PricingQuoteId], function(err,result){
                    if(err){
                        console.log("Error returning query", err);
                        context.done("Fatal Error");
                    }
                    console.log("Number of rows: ", result.rows.length);
                    console.log("Number of Arrays for CustomerLeasePayments in JSON: " + inbound_payload_response.CustomerLeasePayments.length);

                    if(result.rows.length == 0){
                        insertIntoRedshiftResponse();
                    }
                    else{
                        console.log("Data already exisits for received customer ID.");
                        
                    }
                
                });
    		});

    		//Method to run insert query to push data into redshift. 
    		var insertIntoRedshiftRequest = function(m){
    			pg.connect(conString, function(err,client){
            		if(err){
            			return console.log("Connection Error.", err);
           			}
           			console.log("Connection Established.");
           			client.query(queryTextInsertRequest, [suned_id, SystemSize, EFCostPerWatt, Year1Production, CustomerPrepayment, MasterLeasePaymentEscalationRate, Rebate, HIPBIYear1Value, HIPBITenure, HIPBIAnnualDerate, StateTaxRate, CurrentUtilityCost, PostSolarUtilityCost, ProposalID, CallVersionID, AuthorizationCode, ModuleId, ModuleQuantity, InverterId, InverterQuantity, MountingType, currentDate, installerClientName, dealerName, homeownerList_0_firstName, homeownerList_0_lastName, homeownerList_1_firstName, homeownerList_1_lastName, product_type, numberOfHomeowners, homeownerList_0_address, homeownerList_0_city, homeownerList_0_state, homeownerList_0_zipcode, homeownerList_0_phone, homeownerList_0_email, homeownerList_1_address, homeownerList_1_city, homeownerList_1_state, homeownerList_1_zipcode, homeownerList_1_phone, homeownerList_1_email, homeownerList_2_address,homeownerList_2_city, homeownerList_2_state, homeownerList_2_zipcode, homeownerList_2_phone, homeownerList_2_email, installerClientPhone, productionList_0_col2, lifeTimeKwh, inbound_payload_request.Array[m].ArrayNumber, inbound_payload_request.Array[m].ModuleType, inbound_payload_request.Array[m].ModuleQuantity, inbound_payload_request.Array[m].Shading, inbound_payload_request.Array[m].Tilt, inbound_payload_request.Array[m].Azimuth, inbound_payload_request.Array[m].Orientation, inbound_payload_request.Array[m].monthlyProductionValues, inbound_payload_request.Array[m].DegradationRate], function(err,result){
                   		if(err){
                       		return console.log('Error returning query', err);
                   		}
                   		console.log('Row inserted from Request JSON. Go and check on Redshift: ' + result);
                   		return client;
            		});
    				
            	});	
    		}

            var insertIntoRedshiftResponse = function(){
                pg.connect(conString, function(err,client){
                if(err){
                    return console.log("Connection Error.", err);
                }
                console.log("Connection Established.");
                client.query(queryTextInsertResonse, [SunEdCustIdResponse, PricingQuoteId, customerLeasePaymentsArray.toString(), DownPayment, LeaseTerm, NewEstimatedAnnualOutput, UniqueFinancialRunId, terminationValuesArray.toString(), Suned_Timestamp, FinancialModelVersion, CallVersionId, NewGuaranteedAnnualOutput], function(err,result){
                    if(err){
                        return console.log('Error returning query', err);
                    }
                    console.log('Row inserted from Response JSON. Go and check on Redshift: ' + result);
                    return client;
                    end();
                    pg.end();  
                });   
            });
            }			
    	}
    }
};
