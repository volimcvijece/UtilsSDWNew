

def create_dlookup_final(lookupmain, lookuproy):
    """
    Takes 2 current forms of dlookup, cleans them to conform to the common structure and makes an union
    """
    from pandas import concat
    lookupmain['tSchema']=lookupmain['tTable'].apply(lambda x: x.split('.')[0] if '.' in str(x) else str(x))
    lookupmain['tTable']=lookupmain['tTable'].apply(lambda x: x.split('.')[1] if '.' in str(x) else str(x))
    lookupmain['sTable']=lookupmain['sTable'].apply(lambda x: x.split('.')[1] if '.' in str(x) else str(x))

    #df_lookup_roy['tTable']=df_lookup_roy['tSchema']+'.'+df_lookup_roy['tTable']

    lookupmain = lookupmain.loc[:,['SubjectCode','DiseaseCode','HealthTopicCode','tDatabase', 'tSchema', 'sTable','tTable','sField','tField','sValue','tValue']]
    lookuproy = lookuproy.loc[:,['SubjectCode','DiseaseCode','HealthTopicCode','tDatabase','tSchema', 'sTable','tTable','sField','tField','sValue','tValue']]
    return concat([lookupmain,lookuproy])

def clean_lookup_generic(lookuptable):
    print("Cleaning lookup...")
    #table.replace('NULL', np.NaN, inplace=True) # TODO - risky, sto ako stvarno postoji string null
    lookuptable.replace('NULL', None, inplace=True)
    lookuptable = lookuptable.drop(['DlookupID','LookupId'], axis=1, errors='ignore')
    lookuptable['tField']=lookuptable['tField'].str.replace('\r','').str.strip() #edge case 
    lookuptable['tField']=lookuptable['tField'].str.replace('\n','').str.strip()

    #TODO - dodati da ostavi orig kol ako nema tocke

    #TEMP - NULL data type in svalue as NULL
    lookuptable.loc[(lookuptable['sValue'].isna()) & ~(lookuptable['tValue'].isna())& ~(lookuptable['sField'].isna()),'Value'] = 'NULL'

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



    # CREATE FIELD MAPPING FROM DLOOKUP
def get_column_mapping_dict(df_lookup):
    from numpy import nan
    #TODO - sto ako 1 kolona mapira vise njih????? Stari conundrom Npr 
    ####BITNO! ako mapira za vise kolona a sfield je key, izgubit cemo ostale!
    #Treba obrnuti za sad na tfield! i
    #TODO dodati logiku za dodati novi red ili za sad isti ali odvojeno sa ";"!!!!
    """
    1. Create column dictionary mapping from dlookup table as TARGET_FIELD:SOURCE FIELD
    (Target field is a key bcs 1 source field can map to multiple keys) TODO - add that logic for cleaning
    2. Change the created dictionary
    2.a Key RecordId > NationalRecordId (dlookup is mapping it to RecordId bcs of the migration testing)
    2.b Add new mappings
        - DateUsedForSTatistics > TimeCode - TODO - check that one
        - RecordType > SubjectCode (don't forget to check REF changes as well!) - subject code from the EPI table!
        - Status - Status (REF VALUE CHANGES NEW/UPDATE -> ADD/UPDATE) #TODO / dim for new status? Tessy has cv [statuses]

    """
    DICT_COLUMN_RENAMING = dict(zip(df_lookup['tField'].str.strip(),df_lookup['sField'].str.strip())) #helper external col mapping "just in case"
    
    #stav 1 - sa ovim bi sigurno maknuli sve tehnicke kolone ali bi maknuli i GRESKE, npr
    #tamo di je sField prazan a postoji kao za SHIG Pathogen
    #DICT_COLUMN_RENAMING = {key:val for key, val in DICT_COLUMN_RENAMING.items() if val not in (None, 'None', 'nan', 'NULL',nan)}
    #alternativa: hardkodeamo i stavimo sve!


    if 'RecordId' in DICT_COLUMN_RENAMING: DICT_COLUMN_RENAMING['NationalRecordId'] = DICT_COLUMN_RENAMING.pop('RecordId') #2.a
    

    #TODO - tablica ne mora imati recordid i subjectocde
    #tj, legitimno je da dlookup nema ako nema INFO SCHEMA, tj META
    #zato ovo CROSS CHECKATI SA METOM!
    if 'SubjectCode' in DICT_COLUMN_RENAMING: 
        if DICT_COLUMN_RENAMING['SubjectCode'] == 'Subject' or DICT_COLUMN_RENAMING['SubjectCode'] in (None,'None'):
            DICT_COLUMN_RENAMING['SubjectCode'] = 'RecordType'
    if 'SubjectCode' not in DICT_COLUMN_RENAMING: DICT_COLUMN_RENAMING['SubjectCode'] = 'RecordType'

    #Status is METADATA ONLY field, not in dlooku[]
    if 'Status' not in DICT_COLUMN_RENAMING: DICT_COLUMN_RENAMING['Status'] = 'Status'
    
    #AgeClassificationCode is calculated, do not need, delete
    #TODO - how to check later in the flow (curr harcode in this dlookup func)?
    if 'AgeClassificationCode' in DICT_COLUMN_RENAMING: del DICT_COLUMN_RENAMING['AgeClassificationCode']
    
    #OPREZ! TimeCode vs DateUsed....
    #TODO - los workaround - ako imamo timecode, renmeaj
    #ako je timecode:timecode, izbrisi, ne zanima nas jer su calculated
    #ako je dateusedforstatistics tfield, renameaj ga
    if 'TimeCode' in DICT_COLUMN_RENAMING: del DICT_COLUMN_RENAMING['TimeCode']
    #NIJE DOBRO JER STVARA FALSE POSITIVES
    #WARNING! DLOOKUP doesn't have EPI Var DW >  TimeCode


    if 'DateUsedForStatistics' in DICT_COLUMN_RENAMING: DICT_COLUMN_RENAMING['DateUsedForStatistics'] = 'DateUsedForStatistics'
    if 'DateUsedForStatistics' not in DICT_COLUMN_RENAMING: DICT_COLUMN_RENAMING['DateUsedForStatistics'] = 'DateUsedForStatistics'

    #HARDCODE EXCLUSION (for columns we are sure of not to map)
    #BUG - dangerous to write None as string - cognitive load to check None and 'None'!
    #check for None, 'None' and np.nan
    if 'RecordTypeVersion' not in DICT_COLUMN_RENAMING.values(): DICT_COLUMN_RENAMING['None'] = 'RecordTypeVersion'

    #Opasno! DiseaseCode moze biti i nesto drugo, npr Pathogen za VHFOTH!
    if 'DiseaseCode' in DICT_COLUMN_RENAMING:
        if DICT_COLUMN_RENAMING['DiseaseCode'] in (None,'None'):
            DICT_COLUMN_RENAMING['DiseaseCode']='Subject'
        elif DICT_COLUMN_RENAMING['DiseaseCode']=='Subject':
            pass
        elif DICT_COLUMN_RENAMING['DiseaseCode'] not in (None,'None'):
            if 'Subject' in DICT_COLUMN_RENAMING.values():
                keys=[k for k,v in DICT_COLUMN_RENAMING.items() if v=='Subject']
                if len(keys)==1:
                    key=keys[0]
                    DICT_COLUMN_RENAMING[key]=None
                    #print("brisemo viska unos ", key)
                    #del DICT_COLUMN_RENAMING[key]
            else:
                if 'None' in DICT_COLUMN_RENAMING:
                    DICT_COLUMN_RENAMING['None']+=';Subject'
                else:
                    DICT_COLUMN_RENAMING['None']='Subject'
    if 'DiseaseCode' not in DICT_COLUMN_RENAMING: DICT_COLUMN_RENAMING['DiseaseCode']='Subject'


    #TODO - oprez, hardode Subject->Disease code

    #TODOOPREZ! Hardcode pathogens

    return DICT_COLUMN_RENAMING

def column_mapping_dict_clean(dictcolrenaming):
    """
    Simple lowercase
    """
    #import re
    dict_col_renaming_test_lowercase={}
    for k, v in dictcolrenaming.items():
        if v is None:
            dict_col_renaming_test_lowercase[k.lower()]=None
        #elif ',' in v:
        #    dict_col_renaming_test_lowercase[k.lower()]=re.sub('[1-9]', '', v.lower()).split(',')
        else:
            #dict_col_renaming_test_lowercase[k.lower()]=re.sub('[1-9]', '', v.lower())
            #IZBACILI SMO OVDJE radi COVID SPECIFIC KOLONA KOJE *MORAJU* IMATI BROJKE JER SE PONAVLJAJU
            #I TE EDGE CASEOVE CISCNEJA DODALI "ako nemamo druge" kao else u tt.enrichtessy_get_epivardw_from_tessyfields
            dict_col_renaming_test_lowercase[k.lower()]=v.lower()
    return dict_col_renaming_test_lowercase