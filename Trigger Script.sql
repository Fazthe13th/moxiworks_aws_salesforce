--+++ Updated Date Trigger Function for (moxiworkscompany, moxiworksagents, moxiworkscompany_delta, moxiworksagents_delta) table +++--

------moxiworksagents------
create or replace function moxiworks.f_moxiworksagents_updated_date() 
RETURNS trigger AS
$BODY$
begin
new.updated_date := current_timestamp::timestamp(0) without time zone;
RETURN NEW;
END;
$BODY$
  LANGUAGE plpgsql;
  
create trigger t_moxiworksagents_updated_date
before update on moxiworks.moxiworksagents
for each row
execute procedure moxiworks.f_moxiworksagents_updated_date();
------moxiworksagents------


------moxiworksagents_delta------
create or replace function moxiworks.f_moxiworksagents_delta_updated_date() 
RETURNS trigger AS
$BODY$
begin
new.updated_date := current_timestamp::timestamp(0) without time zone;
RETURN NEW;
END;
$BODY$
  LANGUAGE plpgsql;
  
create trigger t_moxiworksagents_delta_updated_date
before update on moxiworks.moxiworksagents_delta
for each row
execute procedure moxiworks.f_moxiworksagents_delta_updated_date();
------moxiworksagents_delta------


------moxiworkscompany------
create or replace function moxiworks.f_moxiworkscompany_updated_date() 
RETURNS trigger AS
$BODY$
begin
new.updated_date := current_timestamp::timestamp(0) without time zone;
RETURN NEW;
END;
$BODY$
  LANGUAGE plpgsql;
  
create trigger t_moxiworkscompany_updated_date
before update on moxiworks.moxiworkscompany
for each row
execute procedure moxiworks.f_moxiworkscompany_updated_date();
------moxiworkscompany------


------moxiworkscompany_delta------
create or replace function moxiworks.f_moxiworkscompany_delta_updated_date() 
RETURNS trigger AS
$BODY$
begin
new.updated_date := current_timestamp::timestamp(0) without time zone;
RETURN NEW;
END;
$BODY$
  LANGUAGE plpgsql;
  
create trigger t_moxiworkscompany_delta_updated_date
before update on moxiworks.moxiworkscompany_delta
for each row
execute procedure moxiworks.f_moxiworkscompany_delta_updated_date();
------moxiworkscompany_delta------

--+++ Updated Date Trigger Function for (moxiworkscompany, moxiworksagents, moxiworkscompany_delta, moxiworksagents_delta) table End +++--

--+++ Audit Table Trigger function for (moxiworkscompany_delta & moxiworksagents_delta) table +++--

CREATE OR REPLACE FUNCTION moxiworks.f_moxiworksagents_delta_audit() RETURNS TRIGGER AS
$BODY$
BEGIN
    IF (TG_OP = 'INSERT') THEN
	INSERT INTO
        moxiworks.moxiworksagents_audit
        VALUES(new.uuid, new.first_name, new.last_name, new.company_legal_name, new.company_uuid, 
			new.access_lvl, new.primary_email, new.direct_phone, new.mobile_phone, 
			new.other_phone, new.title, new.title_2, new.title_3, new.at_least_1_mls_association, new.hub, new.dms, 
			new.moxiengage, new.moxipresent, new.moxiwebsites, new.moxitalent, new.active, new.status, new.created_date, 
			new.updated_date);
		RETURN new;
	ELSIF (TG_OP = 'UPDATE') THEN
	INSERT INTO
        moxiworks.moxiworksagents_audit
        VALUES(new.uuid, new.first_name, new.last_name, new.company_legal_name, new.company_uuid, 
			new.access_lvl, new.primary_email, new.direct_phone, new.mobile_phone, 
			new.other_phone, new.title, new.title_2, new.title_3, new.at_least_1_mls_association, new.hub, new.dms, 
			new.moxiengage, new.moxipresent, new.moxiwebsites, new.moxitalent, new.active, new.status, new.created_date, 
			new.updated_date);
		RETURN new;
	ELSE 
		RETURN null;
	END IF;
END;
$BODY$
language plpgsql;


CREATE TRIGGER t_moxiworksagents_delta_audit
     AFTER INSERT OR UPDATE ON moxiworks.moxiworksagents_delta
     FOR EACH ROW
     EXECUTE PROCEDURE moxiworks.f_moxiworksagents_delta_audit();
	 
	 
CREATE OR REPLACE FUNCTION moxiworks.f_moxiworkscompany_delta_audit() RETURNS TRIGGER AS
$BODY$
BEGIN
    IF (TG_OP = 'INSERT') THEN
	INSERT INTO
        moxiworks.moxiworkscompany_audit
        VALUES(new.uuid, new.company_legal_name, new.address_line_1, new.address_line_2, new.city, new.state, 
				new.zip_code, new.phone, new.website, new.hub, new.dms, new.engage, new.present, new.moxiwebsites, 
				new.moxitalents, new.moxiinsights, new.show_in_product_marketing, new.ayl, new.ayl_emails, 
				new.ays, new.ays_emails, new.status, new.created_date, new.updated_date, new.sfdc_comapny_id
				);
		RETURN new;
	ELSIF (TG_OP = 'UPDATE') THEN
	INSERT INTO
        moxiworks.moxiworkscompany_audit
        VALUES(new.uuid, new.company_legal_name, new.address_line_1, new.address_line_2, new.city, new.state, 
				new.zip_code, new.phone, new.website, new.hub, new.dms, new.engage, new.present, new.moxiwebsites, 
				new.moxitalents, new.moxiinsights, new.show_in_product_marketing, new.ayl, new.ayl_emails, 
				new.ays, new.ays_emails, new.status, new.created_date, new.updated_date, new.sfdc_comapny_id
				);
		RETURN new;
	ELSE 
		RETURN null;
	END IF;
END;
$BODY$
language plpgsql;


CREATE TRIGGER t_moxiworkscompany_delta_audit
     AFTER INSERT OR UPDATE ON moxiworkscompany_delta
     FOR EACH ROW
     EXECUTE PROCEDURE moxiworks.f_moxiworkscompany_delta_audit();
	 
--+++ Audit Table Trigger function for (moxiworkscompany_delta & moxiworksagents_delta) table End +++--
