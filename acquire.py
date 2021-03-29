import pandas as pd
from env import host, user, password

def acquire_zillow():
    db = 'zillow'
    df = pd.read_sql('''SELECT * FROM properties_2017 AS properties2017 LEFT JOIN (SELECT parcelid, max(logerror), max(transactiondate) AS transactiondate FROM predictions_2017 GROUP BY parcelid ORDER BY parcelid) AS predictions2017 USING(parcelid) LEFT JOIN airconditioningtype USING(airconditioningtypeid) LEFT JOIN architecturalstyletype USING(architecturalstyletypeid) LEFT JOIN buildingclasstype USING(buildingclasstypeid) LEFT JOIN heatingorsystemtype USING(heatingorsystemtypeid) LEFT JOIN propertylandusetype USING(propertylandusetypeid) LEFT JOIN storytype USING(storytypeid) LEFT JOIN typeconstructiontype USING(typeconstructiontypeid) LEFT JOIN unique_properties USING(parcelid) WHERE transactiondate LIKE '2017%' AND longitude IS NOT NULL AND latitude IS NOT NULL''', f'mysql+pymysql://{user}:{password}@{host}/{db}')
    return df