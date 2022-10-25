import pandas as pd
import os
import arcpy

# Set this variable to true to export a pattern stop csv 
create_csv = True

def createtripslist(fpath):
    # Read trips and route csv files
    trips = pd.read_csv(fpath + "/trips.txt")
    routes = pd.read_csv(fpath + "/routes.txt")
    # Find each unique shape_id, route_id, and direction in trips. Return first service id, trip id, and block id for that instance. 
    shapeids = trips.groupby(['shape_id','route_id','direction_id']).first().reset_index()
    # Join by route_id to Routes to get agency, route_short_name, route_long_name, route_desc, route_type
    route_info = shapeids.join(routes.set_index('route_id'), on='route_id')
    route_info.rename(columns={'service_id':'sample_service_id','trip_id':'sample_trip_id','trip_headsign':'sample_headsign'},inplace=True)
    return route_info

def to_dict2(df, indexfield):
    return df.groupby(indexfield).first().to_dict('index')

def patternstopslist2(fpath, trips_df): # For each unique shapeid, make a list of stops that serve it. Use a dictionary lookup
    # Read stops and stop_times csv files
    stops = pd.read_csv(fpath + "/stops.txt")
    stop_times = pd.read_csv(fpath + "/stop_times.txt")
    # convert csv files to dictionaries
    stopslookup = to_dict2(stops,'stop_id')
    routeslookup = to_dict2(trips_df[['sample_trip_id','route_id','direction_id']],'sample_trip_id')
    # Create new list for dataframe construction
    patternstops = []
    # New list of routes
    routes = trips_df['sample_trip_id'].unique()
    for rt in routes:
        rows = stop_times.loc[stop_times['trip_id'] == rt].to_dict('records')
        for row in rows:
            # Add stop information
            row.update(stopslookup[row['stop_id']])
            # Add route information
            row.update(routeslookup[row['trip_id']])
        # Append to list of dictionaries
        patternstops.extend(rows)
    return pd.DataFrame(patternstops)

def createpatternstopsdf(fpath):
    # Create unique trips list
    routeslist = createtripslist(fpath)
    # Create stops dataframe
    stopdf = patternstopslist2(fpath, routeslist)
    return stopdf

def getfields(df): # Get fields from dataframe.
    field_translation = {'object':'TEXT','int64':'LONG','float64':'DOUBLE'}
    datatypes = [str(x) for x in df.dtypes.tolist()]
    field_desc = []
    for i, name in enumerate(df.columns):
        otype = field_translation.get(datatypes[i])
        if otype == None: # Set to text if type not found
            otype = 'TEXT'
        field_desc.append([name, otype])
    return field_desc

def addconflictingfieldtypes(fc_path, df): # If two columns share a name but conflicting data types, add a second new column
    datatranslation = {'String': 'TEXT', 'Integer':'LONG','Double': 'DOUBLE'}
    newfields = getfields(df)
    existingcols = [(f.name,datatranslation.get(f.type)) for f in arcpy.ListFields(fc_path)]
    renamed = {}
    fieldstoadd = []
    for i, new in enumerate(newfields):
        for e in existingcols:
            # If name is the same but the data types are different
            if new[0] == e[0] and new[1] != e[1]:
                print('Conflicting data types between ', new, ' and ', e)
                renamed[new[0]] = new[0] + '_2'
                newname = [new[0] + '_2',new[1]]
                print('Copying data into new column: ', newname)
                fieldstoadd.append(newname)
    if len(fieldstoadd) > 0:
        arcpy.management.AddFields(fc_path, fieldstoadd)
        print('Added the following due to data type conflict ', fieldstoadd)
        return df.rename(columns=renamed)
    else:
        return df

def addfcfields(fc_path, df):
    newfields = getfields(df)
    if arcpy.Exists(fc_path):
        existingcols = [f.name for f in arcpy.ListFields(fc_path)]
        # Check that new fields don't already exist
        fieldstoadd = [n for n in newfields if n[0] not in existingcols]
        # Add new fields 
        if len(fieldstoadd) > 0:
            arcpy.management.AddFields(fc_path, fieldstoadd)
        return addconflictingfieldtypes(fc_path,df)




def write_patternstop_fc(stopdf, newfc):
    # See if any fields need to be added to the feature class
    newstopdf = addfcfields(newfc, stopdf)
    # Get the list of fields to write
    fcfields = [f.name for f in arcpy.ListFields(newfc)]
    matchedfields = [g for g in fcfields if g in newstopdf.columns]
    # Write each dataframe row to the feature class as a new point
    with arcpy.da.InsertCursor(newfc,matchedfields + ['SHAPE@XY']) as cur:
        notinserted = []
        for stop in newstopdf.to_dict('records'):
            row_to_insert = [stop[x] for x in matchedfields]
            row_to_insert.append((stop['stop_lon'],stop['stop_lat']))
            try:
                cur.insertRow(row_to_insert)
            except Exception:
                notinserted.append(row_to_insert)
    print('Number of stops: ', len(newstopdf), ' Number of rows not inserted: ', len(notinserted))

def make_patternstop_fc(file_dir,gdb,name):
    # Make a new feature class if it doesn't already exist
    new_fc = gdb + r'\\' + name
    if arcpy.Exists(new_fc)==False:
        arcpy.CreateFeatureclass_management(gdb, name, "POINT",spatial_reference = 4326)
        print('Pattern stops feature class created')
    # Get list of gtfs folders provided
    gtfs_paths = [os.path.join(file_dir,f) for f in os.listdir(file_dir)]
    # Make a dataframe for each file and append to list
    agency_dfs = []
    for agency in gtfs_paths:
        agency_dfs.append(createpatternstopsdf(agency))
        print(agency, ' data successfully processed')
    # Write the contents of each dataframe to the feature class
    for i, agency in enumerate(gtfs_paths):
        print('For GTFS file: ', agency)
        write_patternstop_fc(agency_dfs[i],new_fc)

if __name__ == "__main__":
    loc = r"C:\Users\1280530\GIS\GTFS to Feature Class\02_OtherData\gtfs_2022_06\zipFiles"
    newgdb = r'C:\Users\1280530\GIS\GTFS to Feature Class\Points_Near.gdb'
    name = 'bus_patternstops_202206'
    make_patternstop_fc(loc, newgdb, name)
        