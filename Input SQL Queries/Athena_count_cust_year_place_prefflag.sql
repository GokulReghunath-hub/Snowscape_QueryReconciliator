select C_BIRTH_COUNTRY,C_BIRTH_YEAR,C_PREFERRED_CUST_FLAG,COUNT(*) AS CUST_COUNT
from athena_db.customer
group by 1,2,3;