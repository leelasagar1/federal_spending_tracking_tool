-- CREATE TABLE DisasterEvents (
--   disaster_event_id SERIAL PRIMARY KEY,
--   disaster_event VARCHAR(255) NOT NULL,
--   disaster_year VARCHAR(20) NOT NULL
-- );

-- CREATE TABLE PublicLaws (
--   public_law_id SERIAL PRIMARY KEY,
--   public_law_number VARCHAR(50) NOT NULL,
--   public_law_date DATE NOT NULL
-- );


CREATE TABLE disaster (
    disaster_id INT PRIMARY KEY,
    disaster_name VARCHAR(255),
    disaster_year VARCHAR(9)
);

CREATE TABLE States (
  state_code CHAR(2) PRIMARY KEY,
  state_name VARCHAR(100) NOT NULL
);

CREATE TABLE grantee (
    grantee_id INT PRIMARY KEY,
    grantee_name VARCHAR(255),
    state_code VARCHAR(100),
	FOREIGN KEY (state_code) REFERENCES States(state_code)
);

CREATE TABLE granthistory (
    grant_history_id INT PRIMARY KEY,
    disaster_id INT,
    grantee_id INT,
    grant_award DECIMAL(15,2),
    report_date DATE,
    FOREIGN KEY (disaster_id) REFERENCES disaster(disaster_id),
    FOREIGN KEY (grantee_id) REFERENCES grantee(grantee_id)
);

CREATE TABLE publiclaw (
    pl_number VARCHAR(20) PRIMARY KEY,
    pl_date DATE
);

CREATE TABLE disasterpubliclaw (
    disaster_id INT,
    pl_number VARCHAR(20),
    PRIMARY KEY (disaster_id, pl_number),
    FOREIGN KEY (disaster_id) REFERENCES disaster(disaster_id),
    FOREIGN KEY (pl_number) REFERENCES publiclaw(pl_number)
);


CREATE TABLE CFDAPrograms (
  cfda_number VARCHAR(7) PRIMARY KEY,
  cfda_title VARCHAR(255) NOT NULL,
  cfda_objectives TEXT
);

CREATE TABLE Agencies (
  agency_id INTEGER ,
  agency_name VARCHAR(255) NOT NULL,
  agency_code VARCHAR(50) PRIMARY KEY,
  toptier_agency_id INTEGER,
  agency_slug VARCHAR(100)
);

CREATE TABLE SubAgency (
  sub_agency_code INTEGER PRIMARY KEY,
  sub_agency_name VARCHAR(100) NOT NULL
);

CREATE TABLE Recipients (
  recipient_id VARCHAR(50) PRIMARY KEY,
  recipient_name VARCHAR(255) NOT NULL,
  recipient_state_code CHAR(2) REFERENCES States(state_code)
);




CREATE TABLE Grants (
  grant_id VARCHAR(50) PRIMARY KEY,
  generated_internal_id VARCHAR(50),
  cfda_number VARCHAR(7) REFERENCES CFDAPrograms(cfda_number),
  place_of_performance_state_code CHAR(2) REFERENCES States(state_code),
  recipient_state_code CHAR(2) REFERENCES States(state_code),
  awarding_agency_code VARCHAR(50) REFERENCES Agencies(agency_code),
  funding_agency_code VARCHAR(50) REFERENCES Agencies(agency_code),
  awarding_sub_agency_code INTEGER REFERENCES SubAgency(sub_agency_code),
  funding_sub_agency_code INTEGER REFERENCES SubAgency(sub_agency_code),
  total_funding DECIMAL(18,2),
  total_account_outlay DECIMAL(18,2),
  total_account_obligation DECIMAL(18,2),
  total_outlay DECIMAL(18,2),
  total_obligated DECIMAL(18,2),
  description TEXT,
  award_type VARCHAR(100),
  def_codes VARCHAR(50),
  public_law_number VARCHAR(20),
  covid_19_obligations DECIMAL(18,2),
  covid_19_outlays DECIMAL(18,2),
  infrastructure_obligations DECIMAL(18,2),
  infrastructure_outlays DECIMAL(18,2),
  start_date DATE,
  end_date DATE,
  funding_office_name VARCHAR(255),
  awarding_office_name VARCHAR(255)
);

-- CREATE TABLE GrantHistory (
--   unique_key SERIAL PRIMARY KEY,
--   report_date DATE NOT NULL,
--   state_code CHAR(2) REFERENCES States(state_code),
--   grantee VARCHAR(50),
--   grant_award DECIMAL(18,2),
--   disaster_event_id INTEGER REFERENCES DisasterEvents(disaster_event_id),
--   public_law_id INTEGER REFERENCES PublicLaws(public_law_id)
-- );

CREATE TABLE Funding (
  funding_id SERIAL PRIMARY KEY,
  grant_id VARCHAR(50) REFERENCES Grants(grant_id),
  transaction_obligated_amount DECIMAL(18,2),
  gross_outlay_amount DECIMAL(18,2),
  federal_account VARCHAR(50),
  account_title VARCHAR(255),
  object_class VARCHAR(50),
  object_class_name VARCHAR(255),
  program_activity_code VARCHAR(50),
  program_activity_name VARCHAR(255),
  reporting_fiscal_year INTEGER,
  reporting_fiscal_quarter SMALLINT,
  reporting_fiscal_month SMALLINT,
  is_quarterly_submission BOOLEAN
);

CREATE TABLE Subawards (
  subaward_id SERIAL PRIMARY KEY,
  grant_id VARCHAR(50) REFERENCES Grants(grant_id),
  subaward_number VARCHAR(50),
  description TEXT,
  action_date DATE,
  amount DECIMAL(18,2),
  recipient_id VARCHAR(50) REFERENCES Recipients(recipient_id)
);

CREATE TABLE Transactions (
  transaction_id SERIAL PRIMARY KEY,
  grant_id VARCHAR(50) REFERENCES Grants(grant_id),
  type VARCHAR(50),
  type_description VARCHAR(255),
  action_date DATE,
  action_type VARCHAR(50),
  action_type_description VARCHAR(255),
  modification_number VARCHAR(50),
  description TEXT,
  federal_action_obligation DECIMAL(18,2),
  face_value_loan_guarantee DECIMAL(18,2),
  original_loan_subsidy_cost DECIMAL(18,2),
  cfda_number VARCHAR(7) REFERENCES CFDAPrograms(cfda_number)
);

CREATE TABLE FederalAwards (
  award_id SERIAL PRIMARY KEY,
  grant_id VARCHAR(50) REFERENCES Grants(grant_id),
  recipient_id VARCHAR(50) REFERENCES Recipients(recipient_id),
  award_amount DECIMAL(18,2),
  total_outlays DECIMAL(18,2),
  description TEXT,
  award_type VARCHAR(100),
  covid_19_obligations DECIMAL(18,2),
  covid_19_outlays DECIMAL(18,2),
  infrastructure_obligations DECIMAL(18,2),
  infrastructure_outlays DECIMAL(18,2),
  start_date DATE,
  end_date DATE

);

CREATE TABLE GrantFinancialReports (
  report_id SERIAL PRIMARY KEY,
  grant_id VARCHAR(50) REFERENCES Grants(grant_id),
  state_code CHAR(2) REFERENCES States(state_code),
  grant_award DECIMAL(18,2),
  balance DECIMAL(18,2),
  disaster_year INTEGER,
  grant_age_in_months SMALLINT,
  expected_spending_percentage DECIMAL(5,2),
  drawn_percentage DECIMAL(5,2),
  spending_status VARCHAR(50),
  amount_behind_pace DECIMAL(18,2),
  report_date DATE
);

CREATE TABLE Activities (
  activity_title VARCHAR,
  activity_id SERIAL PRIMARY KEY,
  activity_number VARCHAR(50),
  activity_type VARCHAR(100),
  activity_status VARCHAR(50),
  project_number VARCHAR(50),
  project_title VARCHAR(100),
  projected_start_date DATE,
  projected_end_date DATE,
  national_objective VARCHAR(50),
  responsible_organization VARCHAR(100),
  activity_description TEXT,
  progress_narrative TEXT,
  benifit_type VARCHAR(50),
  completed_activity_actual_end_date DATE
);

CREATE TABLE AmountDescriptions (
  amount_desc_id SERIAL PRIMARY KEY,
  description TEXT,
  time_period_amount DECIMAL(18,2),
  time_period VARCHAR(50),
  to_date_amount DECIMAL(18,2)
);


CREATE TABLE PerformanceReports (
  performance_id SERIAL PRIMARY KEY,
  activity_id INTEGER REFERENCES Activities(activity_id),
  amount_desc_id INTEGER REFERENCES AmountDescriptions(amount_desc_id),
  grant_id VARCHAR(50) REFERENCES Grants(grant_id),
  to_date_amount DECIMAL(18,2),
  time_period_amount DECIMAL(18,2)
);

truncate table disaster cascade;
truncate table grantee cascade;
truncate table granthistory cascade;
truncate table publiclaw cascade;
truncate table disasterpubliclaw cascade;


select * from activities;
select * from performancereports
select * from amountdescriptions

select * from PerformanceReports 
select * from disaster
select * from granthistory


ALTER TABLE grantfinancialreports
ADD COLUMN report_date DATE;

ALTER table activities
add column  completed_activity_actual_end_date DATE


select * from agencies
select * from federalawards
select * from recipients where recipient_id ='4e80d4c6-329c-27c3-5033-0ac2275b4cf4-C'

select * from grantee
select * from granthistory
select * from disaster
select * from publiclaw
select * from disasterpubliclaw

select * from grantfinancialreports

-- truncate table agencies cascade
-- truncate table federalawards cascade

select * from cfdaprograms

select * from activities

select * from performancereports;
select * from activities
select * from amountdescriptions

truncate table performancereports;
truncate table activities cascade;
truncate table amountdescriptions cascade;

truncate granthistory cascade;
truncate disaster cascade;
truncate grantee cascade;
truncate publiclaw cascade;
truncate disasterpubliclaw cascade;



select * from granthistory

select * from transactions