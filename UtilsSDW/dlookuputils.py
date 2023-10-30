

def clean_lookup_generic(lookuptable):
    print("Cleaning lookup...")
    #table.replace('NULL', np.NaN, inplace=True) # TODO - risky, sto ako stvarno postoji string null
    lookuptable.replace('NULL', None, inplace=True)
    lookuptable = lookuptable.drop(['DlookupID','LookupId'], axis=1, errors='ignore')
    lookuptable['tField']=lookuptable['tField'].str.replace('\r','').str.strip() #edge case 
    lookuptable['tField']=lookuptable['tField'].str.replace('\n','').str.strip()

    #TODO - dodati da ostavi orig kol ako nema tocke
    return lookuptable.astype(str).replace("'",'', regex=True)

def clean_lookup_split_tablename(lookuptable):
    lookuptable['sField']=lookuptable['sField'].apply(lambda x: x.split('.')[1] if '.' in str(x) else str(x))
    lookuptable['tTable']=lookuptable['tTable'].apply(lambda x: x.split('.')[1] if '.' in str(x) else str(x))
    return lookuptable

def clean_lookup_lowercase(lookuptable):
    lookuptable['sField'] = lookuptable['sField'].str.lower()
    lookuptable['tField'] = lookuptable['tField'].str.lower()
    return lookuptable

def enrich_dlookup_columninfo(df_lookup, target_meta):
    target_meta['COLUMN_NAME'] = target_meta['COLUMN_NAME'].str.lower()

    if target_meta is not None:
        if len(target_meta)>0:
            return df_lookup.merge(target_meta, left_on=['tTable','tField'], right_on=['TABLE_NAME', 'COLUMN_NAME'], how='left')

def enrich_dlookup_columnfks(df_lookup, target_meta_fk):
    target_meta_fk['FK_Column'] = target_meta_fk['FK_Column'].str.lower()
    if target_meta_fk is not None:
        if len(target_meta_fk)>0:
            return df_lookup.merge(target_meta_fk, left_on=['tTable','tField'], right_on=['FK_Table', 'FK_Column'], how='left')


def create_dlookup_final(lookupmain, lookuproy):
    #TODO - to accept X dlookups and impose cleaning for each dlookup, not only for specific lookupmain
    from pandas import concat
    lookupmain['tSchema']=lookupmain['tTable'].apply(lambda x: x.split('.')[0] if '.' in str(x) else str(x))
    lookupmain['tTable']=lookupmain['tTable'].apply(lambda x: x.split('.')[1] if '.' in str(x) else str(x))
    if 'sSchema' not in lookupmain.columns:
        lookupmain['sSchema']=lookupmain['sTable'].apply(lambda x: x.split('.')[0] if '.' in str(x) else str(x))   
    lookupmain['sTable']=lookupmain['sTable'].apply(lambda x: x.split('.')[1] if '.' in str(x) else str(x))

    #df_lookup_roy['tTable']=df_lookup_roy['tSchema']+'.'+df_lookup_roy['tTable']

    lookupmain = lookupmain.loc[:,['SubjectCode','DiseaseCode','HealthTopicCode','sDatabase','tDatabase','sSchema', 'tSchema', 'sTable','tTable','sField','tField','sValue','tValue']]
    lookuproy = lookuproy.loc[:,['SubjectCode','DiseaseCode','HealthTopicCode','sDatabase','tDatabase','sSchema','tSchema', 'sTable','tTable','sField','tField','sValue','tValue']]
    return concat([lookupmain,lookuproy])