import pandas as pd
import os
import arcpy

def createtripslist(fpath):
    # Read trips and route csv files
    trips = pd.read_csv(fpath + r"\\trips.txt")
    routes = pd.read_csv(fpath + r"\\routes.txt")
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
    # Edit the list below to specify what route data to include
    keeproutedata = ['sample_trip_id','shape_id','route_id','direction_id','sample_service_id','route_type','route_desc']
    routeslookup = to_dict2(trips_df[keeproutedata],'sample_trip_id')
    # Create new list for dataframe construction
    patternstops = []
    # New list of routes
    routes = trips_df['sample_trip_id'].unique()
    for rt in routes:
        # Filter stop_times dataframe to only the specified route (sample trip)
        rows = stop_times.loc[stop_times['trip_id'] == rt].to_dict('records')
        for row in rows:
            # Add stop information
            row.update(stopslookup[row['stop_id']])
            # Add route information
            row.update(routeslookup[row['trip_id']])
        # Append to list of dictionaries
        patternstops.extend(rows)
    # Make dataframe. Rename trip_id to sample_trip_id
    df = pd.DataFrame(patternstops)
    df.rename(columns={'trip_id':'sample_trip_id'},inplace=True)
    return df

def createpatternstopsdf(file_dir,csv_export=False):
    # Get list of gtfs folders provided
    borough = [f for f in os.listdir(file_dir)]
    gtfs_paths = [os.path.join(file_dir,f) for f in borough]
    # Process the dataframes one by one
    agency_dfs = []
    for i, agency in enumerate(gtfs_paths):
        # Create unique trips list
        routeslist = createtripslist(agency)
        # Create stops dataframe
        stopdf = patternstopslist2(agency, routeslist)
        # Create a source 
        stopdf['source'] = borough[i]
        agency_dfs.append(stopdf)
        print(agency, ' data successfully processed')
    allpatternstops = pd.concat(agency_dfs)
    if csv_export==True:
        allpatternstops.to_csv(file_dir + r'\pattern_stops_merged_' + os.path.split(file_dir)[1] + r'.csv', index=False)
    return allpatternstops

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

def addfcfields(fc_path, df):
    newfields = getfields(df)
    existingcols = [f.name for f in arcpy.ListFields(fc_path)]
    # Check that new fields don't already exist
    fieldstoadd = [n for n in newfields if n[0] not in existingcols]
    # Add new fields 
    if len(fieldstoadd) > 0:
        print('New fields added')
        arcpy.management.AddFields(fc_path, fieldstoadd)

def write_patternstop_data(stopdf, newfc):
    # See if any fields need to be added to the feature class
    addfcfields(newfc, stopdf)
    # Get the list of fields to write
    fcfields = [f.name for f in arcpy.ListFields(newfc)]
    matchedfields = [g for g in fcfields if g in stopdf.columns]
    # Write each dataframe row to the feature class as a new point
    with arcpy.da.InsertCursor(newfc,matchedfields + ['SHAPE@XY']) as cur:
        notinserted = []
        for stop in stopdf.to_dict('records'):
            # Add row data
            row_to_insert = [stop[x] for x in matchedfields]
            # Add latlong data
            row_to_insert.append((stop['stop_lon'],stop['stop_lat']))
            try:
                cur.insertRow(row_to_insert)
            except Exception:
                notinserted.append(row_to_insert)
    print('Number of stops: ', len(stopdf), ' Number of rows not inserted: ', len(notinserted))

def make_patternstop_fc(file_dir,gdb,name,csv_export=False,trackingfield=None):
    # Make a new feature class if it doesn't already exist
    new_fc = gdb + r'\\' + name
    if arcpy.Exists(new_fc)==False:
        arcpy.CreateFeatureclass_management(gdb, name, "POINT",spatial_reference = 4326)
        print('New pattern stops feature class created')
    # Make the new pattern stops dataframe
    boroughs_df = createpatternstopsdf(file_dir,csv_export)
    # Write the contents of each dataframe to the feature class
    if trackingfield == None:
        write_patternstop_data(boroughs_df,new_fc,)
    else:
        borough = boroughs_df[trackingfield].unique().tolist()
        for bor in borough:
            oneborough = boroughs_df.loc[boroughs_df[trackingfield]==bor]
            print('For GTFS file: ', bor)
            write_patternstop_data(oneborough,new_fc)

# Ask whether to run script
while True:
    make = input(r'Excecute script and create feature class? (y/n): ')
    if make not in ['y','Y','n','N']:
        print(r'Incorrect input. Please type either y or n')
        continue
    else:
        if make == 'y' or make == 'Y':
            run_script = True
        else:
            run_script = False
        break

# Ask whether to export a csv of stops
while True:
    create = input(r'Export a csv file of stops? (y/n): ')
    if create not in ['y','Y','n','N']:
        print(r'Incorrect input. Please type either y or n')
        continue
    else:
        if create == 'y' or create == 'Y':
            create_csv = True
        else:
            create_csv = False
        break

if __name__ == "__main__" and run_script == True:
    loc_in = input(r'Input directory of GTFS folders:')
    loc = loc_in.strip('"\'')
    newgdb_in = input(r'Input out ArcGIS geodatabase directory: ')
    newgdb = newgdb_in.strip('"\'')
    name_in = input(r'Input name of the new pattern stops feature class: ')
    name = name_in.strip('"\'')
    make_patternstop_fc(loc, newgdb, name, create_csv, trackingfield='source')

# Sample file paths
 #   loc = r"C:\Users\1280530\GIS\GTFS to Feature Class\02_OtherData\gtfs_2022_09\zipFiles"
 #   newgdb = r"C:\Users\1280530\GIS\GTFS to Feature Class\Points_Near.gdb"
 #   name = 'bus_patternstops_202209'
