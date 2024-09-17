import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

class ConvertToDict(beam.DoFn):
    def process(self, element):
        import json
        yield json.loads(element)

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    schema = """
    age:STRING, appboy ID:STRING, campaign_Source:STRING, city:STRING, dob:TIMESTAMP, gender:STRING, language:STRING, phone:STRING, 
    source:STRING, state:STRING, address:STRING, age:STRING, age_group:STRING, annual_revenue:STRING, apartment_or_suite_number:STRING, 
    appraised_improvement_value:STRING, arts_bundle:STRING, auto_year:STRING, automotive:STRING, average_time_spent_minutes:STRING, 
    avid_reader:STRING, benefits:STRING, birthday:STRING, boating:STRING, book_buyer:STRING, browser:STRING, browser_language:STRING, 
    browser_version:STRING, campaign:STRING, campaign_name:STRING, cancer_type:STRING, car_accident:STRING, car_insurance:STRING, 
    car_owner:STRING, child_gender:STRING, child_name:STRING, children:STRING, clickid:STRING, collectibles:STRING, connection_type:STRING, 
    cooking_and_wine:STRING, country:STRING, country_code:STRING, coverage_type:STRING, credit_card_user:STRING, credit_score:STRING, 
    custom_event:STRING, days_visited:STRING, deed_type:STRING, delivery_validation:STRING, description:STRING, device:STRING, 
    device_brand:STRING, device_browser:STRING, device_model:STRING, device_name:STRING, device_os:STRING, device_platform:STRING, 
    disabled:STRING, doctor_treatment:STRING, donator_to_charity_or_causes:STRING, education:STRING, email:STRING, emailLastSeen:TIMESTAMP, 
    email_opt_out:STRING, ethnicity:STRING, everflow_id:STRING, fax:STRING, first_name:STRING, first_visited_time:STRING, first_visited_url:STRING,
    fitness:STRING, food_wine_and_cooking:STRING, fuel_type:STRING, gaming_and_gambling:STRING, has_attorney:STRING, has_cancer:STRING, 
    has_children:STRING, health_plan_type:STRING, hi_tech:STRING, home_garden:STRING, home_improvement:STRING, home_market_value:STRING, 
    home_owner:BOOLEAN, home_value:STRING, homeowner:STRING, household_income:STRING, household_size:STRING, id:INTEGER, in_app_purchase_total:INTEGER, 
    incident_date:STRING, incomeRange:STRING, industry:STRING, injury_type:STRING, insurance_type:STRING, interest_rate_type:STRING, investing:STRING, 
    ip_address:STRING, is_duplicate:STRING, is_test_lead:BOOLEAN, isp:STRING, jornaya_cert:STRING, keyword_searched:STRING, landing_page:STRING, 
    last_name:STRING, last_visited_time:STRING, latestActivityDate:TIMESTAMP, lead_id:STRING, lead_source:STRING, leads_payload_id:STRING, 
    lender_name:STRING, length_of_residence:STRING, luxury_vehicle_owner:STRING, marital_status:STRING, married:STRING, medicare:STRING, 
    medium:STRING, men_apparel:STRING, mfg:STRING, middle_name:STRING, mileage:STRING, mobile:STRING, mobile_home:STRING, model:STRING, 
    mortgage_amount:STRING, mortgage_owner_type:STRING, mortgage_refi_type:STRING, mortgage_type:STRING, motorcycling:STRING, number_of_IDFAs:INTEGER, 
    number_of_IDFVs:INTEGER, number_of_children:STRING, number_of_google_ad_ids:INTEGER, number_of_push_tokens:INTEGER, number_of_roku_ad_ids:INTEGER, 
    number_of_windows_ad_ids:INTEGER, oid:STRING, os:STRING, os_version:STRING, outdoors:STRING, owner_occupancy:STRING, parenting_and_children:STRING, 
    payload:STRING, percentage_asian:STRING, percentage_black:STRING, percentage_hispanic:STRING, percentage_white:STRING, pet_lover:STRING, 
    phone_Type:STRING, politics:STRING, prepaid:STRING, product:STRING, product_name:STRING, property_type:STRING, purchasing_power_score:STRING, 
    random_bucket:INTEGER, recording:STRING, reference_no:STRING, referral:STRING, referral_url:STRING, referrer:STRING, region:STRING, 
    rent_or_own:STRING, revenue:INTEGER, ringba_publisher_id:STRING, ringba_publisher_name:STRING, salutation:STRING, second_auto_class:STRING, 
    second_auto_fuel_type:STRING, second_auto_make:STRING, second_auto_model:STRING, second_auto_style:STRING, second_auto_vin:STRING, second_auto_year:STRING, 
    secondary_email:STRING, self_improvement:STRING, senior_in_household:STRING, session_count:INTEGER, sex_abused:STRING, single_parent:STRING, 
    skype_id:STRING, sms_consent:BOOLEAN, sports:STRING, status:STRING, street_address:STRING, street_name:STRING, street_type:STRING, 
    suv_owner:STRING, tax_amount:STRING, telco:STRING, terms_agreed:STRING, title_company:STRING, travel:STRING, trusted_form_url:STRING, 
    twitter:STRING, unit:STRING, us_veteran:STRING, user_agent:STRING, user_id:STRING, value_shopper:STRING, vehicle_make:STRING, 
    vehicle_type:STRING, vin:STRING, visitor_score:STRING, wealth_score:STRING, website:STRING, were_you_at_fault:STRING, women_apparel:STRING, 
    zip:INTEGER, zip_code:INTEGER
    """

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription='projects/arctic-column-432314-p8/subscriptions/customer-profiles-subscription')
            | 'ConvertToDict' >> beam.ParDo(ConvertToDict())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table='arctic-column-432314-p8:customer_profiles.customer_data',
                schema=schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()
