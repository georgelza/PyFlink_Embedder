
SET 'execution.checkpointing.interval'   = '30s';

SET 'table.exec.sink.upsert-materialize' = 'NONE';

################################################################################################################################################

SET 'pipeline.name' = 'Persist into Paimon: Address table: Children addresses';

INSERT INTO c_paimon.outbound.address
  (parcel_id, street_1, street_2, neighbourhood, town, county, province, country, postal_code, country_code)
SELECT 
   address.parcel_id      AS parcel_id 
  ,address.street_1       AS street_1
  ,address.street_2       AS street_2
  ,address.neighbourhood  AS neighbourhood
  ,address.town           AS town
  ,address.county         AS county
  ,address.province       AS province
  ,address.country        AS country
  ,address.postal_code    AS postal_code
  ,address.country_code   AS country_code
FROM kafka_catalog.inbound.children
WHERE address.parcel_id IS NOT NULL;

################################################################################################################################################

SET 'pipeline.name' = 'Persist into Paimon: Address table: Adult addresses';

INSERT INTO c_paimon.outbound.address
  (parcel_id, street_1, street_2, neighbourhood, town, county, province, country, postal_code, country_code)
SELECT 
   address.parcel_id      AS parcel_id 
  ,address.street_1       AS street_1
  ,address.street_2       AS street_2
  ,address.neighbourhood  AS neighbourhood
  ,address.town           AS town
  ,address.county         AS county
  ,address.province       AS province
  ,address.country        AS country
  ,address.postal_code    AS postal_code
  ,address.country_code   AS country_code
FROM kafka_catalog.inbound.adults
WHERE address.parcel_id IS NOT NULL;

################################################################################################################################################

SET 'pipeline.name' = 'Persist into Paimon: Accounts table: Unnested accounts';

INSERT INTO c_paimon.outbound.accounts
  SELECT
      `nationalid`                  AS `nationalid`
      ,ac.`fspiAgentAccountId`      AS `fspiAgentAccountId`
      ,ac.`accountId`               AS `accountId`
      ,ac.`fspiId`                  AS `fspiId`
      ,ac.`fspiAgentId`             AS `fspiAgentId`
      ,ac.`accountType`             AS `accountType`
      ,ac.`memberName`              AS `memberName`
      ,ac.`cardHolder`              AS `cardHolder`
      ,ac.`cardNumber`              AS `cardNumber`
      ,ac.`expDate`                 AS `expDate`
      ,ac.`cardNetwork`             AS `cardNetwork`
      ,ac.`issuingBank`             AS `issuingBank`
  FROM kafka_catalog.inbound.adults
  CROSS JOIN UNNEST(`account`) AS ac;

  ################################################################################################################################################


