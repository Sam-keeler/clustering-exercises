import pandas as pd
from env import host, user, password

def acquire_zillow():
    db = 'zillow'
    df = pd.read_sql('''SELECT * FROM properties_2017 AS properties2017 LEFT JOIN (SELECT parcelid, max(logerror), max(transactiondate) AS transactiondate FROM predictions_2017 GROUP BY parcelid ORDER BY parcelid) AS predictions2017 USING(parcelid) LEFT JOIN airconditioningtype USING(airconditioningtypeid) LEFT JOIN architecturalstyletype USING(architecturalstyletypeid) LEFT JOIN buildingclasstype USING(buildingclasstypeid) LEFT JOIN heatingorsystemtype USING(heatingorsystemtypeid) LEFT JOIN propertylandusetype USING(propertylandusetypeid) LEFT JOIN storytype USING(storytypeid) LEFT JOIN typeconstructiontype USING(typeconstructiontypeid) LEFT JOIN unique_properties USING(parcelid) WHERE transactiondate LIKE '2017%' AND longitude IS NOT NULL AND latitude IS NOT NULL''', f'mysql+pymysql://{user}:{password}@{host}/{db}')
    return df

def handle_missing_values(df, prop_required_column = .5, prop_required_row = .70):
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df

def prep_zillow(df):
    df = df[(df['propertylandusetypeid'] == 261) | (df['propertylandusetypeid'] == 262) | (df['propertylandusetypeid'] == 263) | (df['propertylandusetypeid'] == 273) | (df['propertylandusetypeid'] == 275) | (df['propertylandusetypeid'] == 276) | (df['propertylandusetypeid'] == 279)]
    df = df[(df.bedroomcnt > 0) & (df.bathroomcnt > 0) & ((df.unitcnt<=1)|df.unitcnt.isnull())\
        & (df.calculatedfinishedsquarefeet>350)]
    df.drop(columns = ['finishedsquarefeet12', 'propertycountylandusecode', 'propertyzoningdesc',
        'regionidcity', 'assessmentyear', 'id', 'fullbathcnt'], inplace = True)
    df.fips.replace({6037: 'Los Angeles', 6059: 'Orange', 6111: 'Ventura'}, inplace = True)
    df.rename(columns = {'fips': 'county'}, inplace = True)
    df.unitcnt.fillna(1, inplace = True)
    df.heatingorsystemdesc.fillna('None', inplace = True)
    df.lotsizesquarefeet.fillna(7313, inplace = True)
    df.buildingqualitytypeid.fillna(6.0, inplace = True)
    df = df[df.taxvaluedollarcnt < 5_000_000]
    df = df[df.calculatedfinishedsquarefeet < 8000]
    return df

def min_max_scaler(train, valid, test):
    num_vars = list(train.select_dtypes('number').columns)
    scaler = MinMaxScaler(copy=True, feature_range=(0,1))
    train[num_vars] = scaler.fit_transform(train[num_vars])
    valid[num_vars] = scaler.transform(valid[num_vars])
    test[num_vars] = scaler.transform(test[num_vars])
    return scaler, train, valid, test