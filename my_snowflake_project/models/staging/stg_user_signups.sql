-- This file is a "model." It's just a SELECT statement.
-- dbt will turn this into a view or table named "stg_user_signups"
SELECT    
    first_name,    
    last_name,    
    email,    -- This is the business logic you were asked for: 
    CASE
        WHEN country IN ('USA', 'United States of America') THEN 'United States'
        ELSE country
    END AS country,
    signup_time
FROM
    -- This is the magic "ref" function. 
    -- It tells dbt to read from another model.
    -- Wait... we don't have a model, we have a RAW table. So we use "source".
    -- Let's tell dbt about our raw table.
    -- In a file named models/schema.yml:
    /*
    version: 2
    sources:
      - name: python_feed
        tables:
          - name: RAW_USER_SIGNUPS
    */
    -- Now, back to our SQL file:
    {{ source('python_feed', 'RAW_USER_SIGNUPS') }}
 