def get_reference_table(database_name, ref_table_name):
    query = f"SELECT * FROM {database_name}.ref.{ref_table_name}"
    return query

#TODO - subject code or disease code?
def get_reference_table_tosubject(database_name, ref_table_name, subjectcode):
    #WHERE DiseaseCode ili SubjectCode?
    query = f"SELECT * FROM {database_name}.ref.{ref_table_name} WHERE SubjectCode = '{subjectcode}'"
    return query


def get_mdvariable_info(table_name):
    query = f"""
        SELECT m1.[VariableGuid]
            ,[VariableName]
            ,m2.VariableType
            ,[DbDW]
            ,[SchemaDW]
            ,[TableDW]
            ,[VariableDW]
            ,m1.[ValidFrom]
            ,m1.[ValidTo]
            ,r.RefTable
            ,r.RefValueName
            ,r.RefValueCode
        FROM [EpiPulseCasesMetadata].[dbo].[mdVariable] m1
        JOIN [EpiPulseCasesMetadata].[dbo].mdVariableType m2 ON m1.VariableTypeId = m2.VariableTypeId
        JOIN [EpiPulseCasesMetadata].[dbo].mdRefValue r ON m1.VariableGuid = r.VariableGuid
        WHERE 1=1
        AND m1.TableDW = '{table_name}'
        --AND m1.VariableDW ='SerotypeCode'
    """
    return query
