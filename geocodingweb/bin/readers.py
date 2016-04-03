#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
# -----------------------------------------------------------------------------
#
# This file is the copyrighted property of Tableau Software and is protected
# by registered patents and other applicable U.S. and international laws and
# regulations.
#
# Unlicensed use of the contents of this file is prohibited. Please refer to
# the NOTICES.txt file for further details.
#
# -----------------------------------------------------------------------------

.. module:: readers.py
   :platform: Unix
   :synopsis: Readers connect to data and return individual records.

.. moduleauthor:: mkenny

Readers instances are responsible for creating a connection to a backing datasource,
and returning an iterable that yields a single record of data from that datasource.

Readers emit a single record via the __iter__() method. A single record in this
context is represented as a python dictionary, with a geojson-like structure.

String values should be emitted as Unicode objects. Numerical values may be emitted
using an appropriate Python data type.

Implementations of readers should inherit from ReaderBaseClass, which defines
the expected interface.

An example feature:

{'geometry':
     {'type': 'GeometryCollection',
      'geometries': [
          {u'type': u'Point', u'coordinates': [-30.2698671622524, -7.69885576115782]},
          {u'type': u'MultiPolygon', u'coordinates': [
              [[[-37.1712953836544, 2.19899087569888], [-37.9445646521589, -17.5967023980146],
                [-24.3350255264808, -18.8339332276217], [-22.0152177209676, 1.27106775349357],
                [-37.1712953836544, 2.19899087569888]]]]}]},
 'type': 'Feature',
 'properties': {
     'none': u'One Hundred - 10 (None)',
     'fr_fr': None,
     'pt_br': u'Cem- 10',
     'zh_cn': u'一百年- 10',
     'ko_kr': u'백- 10',
     'iso3': None,
     'map_code': 0,
     'id': 110,
     'ja_jp': u'ワンハンドレッド- 10',
     'synonyms': [u'Synonym 110', u'Synonym 110-2', u'同義語'],
     'iso2': None,
     'fips': None,
     'parent_id': 100,
     'de_de': u'Einhundert-10',
     'es_es': None,
     'class': 1,
     'en_us': None}
}
"""
__author__ = 'mkenny'
import abc
import csv
import itertools
import os
import psycopg2
import simplejson
import sys
from collections import Counter
from psycopg2.extras import RealDictCursor
from psycopg2 import ProgrammingError
from shapely import wkt, geometry, speedups


class ReaderBaseClass(object):
    """An abstract base class for a Reader interface."""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, **kwargs):
        """Assign parameters extracted from a configuration file
        to parameters on the instance."""
        pass

    @abc.abstractmethod
    def __del__(self, exc_type=None, exc_val=None, ext_tb=None):
        """Cleanup code performed when the instance is garbage collected.
        Note: will be called twice if a context manager is used."""
        pass

    def __enter__(self):
        """Setup code called when instantiated via context manager."""
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        """Cleanup code called when instantiated via context manager."""
        return self.__del__(exc_type, exc_val, ext_tb)

    @abc.abstractmethod
    def __iter__(self):
        """Return a generator object yielding individual records."""
        pass


class ReaderLocalCSVs(ReaderBaseClass):
    """
    Reader class for a set of extracted local csv files.

    Data output are formatted as a GeoJSON Feature Object, whose Geometry Object
    is of type Geometry Collection. This allows for a single feature to store both
    Point and Polygon geometries.

    :param in_dir: A pathway to a directory containing local csv files.
    """

    def __init__(self, in_dir):
        # path to CSV directory
        self.in_dir = in_dir
        self.file_structure = {
            'AreaCode': {'class_id': 101, 'files': ['AreaCode.csv', 'LocalDataAreaCode.csv']},
            'County': {'class_id': 2, 'files': ['CountySynonyms.csv', 'LocalDataCounty.csv', 'County.csv']},
            'ZipCode': {'class_id': 100, 'files': ['ZipCode.csv', 'LocalDataZipCode.csv']},
            'CMSA': {'class_id': 102, 'files': ['CMSA.csv', 'CMSASynonyms.csv', 'LocalDataCMSA.csv']},
            'State': {'class_id': 1, 'files': ['State.csv', 'StateSynonyms.csv', 'LocalDataState.csv']},
            'City': {'class_id': 10, 'files': ['City.csv', 'CitySynonyms.csv', 'LocalDataCity.csv']},
            'Congress': {'class_id': 103, 'files': ['Congress.csv', 'CongressSynonyms.csv', 'LocalDataCongress.csv']},
            'Country': {'class_id': 0, 'files': ['Country.csv', 'CountrySynonyms.csv', 'LocalDataCountry.csv']},
        }
        self._cursor = self._create_cursor(in_dir)
        speedups.enable()

    def __del__(self, exc_type=None, exc_val=None, ext_tb=None):
        """Cleanup code performed when the instance is garbage collected.
        Note: will be called twice if a context manager is used."""
        pass

    def _validate_files(self, in_dir):
        """
        Check that we have all the required files for processing

        :param in_dir: Path to local CSV directory.
        :return:  True if all required files are present. Exception if not.
        """
        # Create a list of files in self.file_structure.
        required_files = [v['files'] for k, v in self.file_structure.iteritems()]
        required_files_flattened = [file_name for file_list in required_files for file_name in file_list]

        # Create a list of files in user-provided directory.
        files_in_dir = os.listdir(in_dir)

        # Check if all required files are present in given directory using set intersection.
        if set(required_files_flattened).issubset(set(files_in_dir)):
            return True
        else:
            raise Exception('Error: Missing Expected CSV file.')

    def _create_cursor(self, in_dir):
        """
        Create a generator object yielding individual records.

        :param in_dir: Path to local CSV directory.
        :return: Returns an iterable (using itertools.chain) that emits assembled features.
        """
        # Override param `field_size_limit` to allow large values for a field.
        # Seen in GeoJSON polygon geoms. Note: using sys.maxsize fails on 64-bit python
        bit32_sys_maxsize = int(2**31 - 1)
        csv.field_size_limit(bit32_sys_maxsize)
        geocoding_generators = []
        if self._validate_files(in_dir):
            for class_name, class_attributes in self.file_structure.iteritems():
                generator = self._get_class_generator(in_dir, class_name, class_attributes)
                geocoding_generators.append(generator)

            # Create combined generator
            return itertools.chain.from_iterable(geocoding_generators)

    def _records_for_class_to_geojson(self, class_name, csv_records):
        """
        Given a class name and dict containing rows from CSV files related to that class
        return a generator which yields a single combined row as GeoJSON.

        :param class_name: The name of the class for which this generated will emit records for.
        :param csv_records: A dictionary of CSV records parsed from user-provided directory.
        """
        # Store our geojson output for the class
        geojson_output = {}

        for record in csv_records['named']:
            out_record = {
                'type': 'feature',
                'properties': {
                    'map_code': int(record['MapCode']),
                    'parent_id': record['ParentID'],
                    'id': int(record['ID']),
                    'class': self.file_structure[class_name]['class_id'],
                    'fips': None, 'iso2': None, 'iso3': None,
                    'de_de': None, 'en_us': None, 'es_es': None,
                    'fr_fr': None, 'ja_jp': None, 'ko_kr': None,
                    'pt_br': None, 'zh_cn': None, 'none': None,
                    'synonyms': None
                }
            }

            # Convert parent_id to int. Use a python `None` if an empty string is encountered.
            if out_record['properties']['parent_id']:
                try:
                    out_record['properties']['parent_id'] = int(out_record['properties']['parent_id'])
                except ValueError as e:
                    out_record['properties']['parent_id'] = None

            if 'Name' in record.keys():
                # For certain geocoding classes, the `none` values
                # is represented in the CSVs as `Name`.
                out_record['properties']['none'] = record['Name'].decode('utf-8')

            # The Country.csv file has extra name fields.
            if 'FIPS' in record.keys():
                out_record['properties']['fips'] = record['FIPS'].decode('utf-8')
                out_record['properties']['iso2'] = record['ISO3166_2'].decode('utf-8')
                out_record['properties']['iso3'] = record['ISO3166_3'].decode('utf-8')

            # assign this record back to staging dict.
            geojson_output[record['compound_key']] = out_record

        for record in csv_records['local']:
            out_record = {'type': 'GeometryCollection', 'geometries': []}
            # Geometry is the name of field containing multipolygon wkt
            if 'Geometry' in record and record['Geometry'] != 'None':
                polygon = geometry.mapping(wkt.loads(record['Geometry']))
                out_record['geometries'].append(polygon)
            point = {'type': 'Point', 'coordinates': [float(record['Longitude']), float(record['Latitude'])]}
            out_record['geometries'].append(point)
            # append out_record to geojson_ouput dict
            geojson_output[record['compound_key']]['geometry'] = out_record

        if csv_records['synonyms']:
            for record in csv_records['synonyms']:
                # Begin adding properties.
                rec_props = geojson_output[record['compound_key']]['properties']
                if record['IsDisplayName'] == '1':
                    locale = record['Locale'].lower()
                    rec_props[locale] = record['Name'].decode('utf-8')
                else:
                    # We have a synonym
                    if rec_props['synonyms'] is None:
                        rec_props['synonyms'] = []
                    rec_props['synonyms'].append(record['Name'].decode('utf-8'))

        # Create the generator
        for record in geojson_output.itervalues():
            yield record

    def _get_class_generator(self, in_dir, class_name, class_attributes):
        """
        Return a generator returning geojson records
        for a specific geocoding class.

        :param in_dir: A string representing the path to CSVs.
        :param class_name: A desired class name to assemble records for.
        :param class_attributes: A dictionary containing the class' numerical code (e.g. 0,1,2,10, etc.) and file names.
        :return: Returns a generator emitting assembled records for that class.
        """
        # Data structure to store records from each file of the class
        records_from_files = {
            'named': None,
            'local': None,
            'synonyms': None
        }

        # Iterate through each file adding contents to records_from_files
        for file_name in class_attributes['files']:
            file_path = os.path.join(in_dir, file_name)
            with open(file_path) as in_file:
                # Determine file 'type'
                if 'Synonyms' in file_name:
                    file_type = 'synonyms'
                elif 'Local' in file_name:
                    file_type = 'local'
                else:
                    file_type = 'named'

                # Create a csv reader and a list to store extracted records.
                reader = csv.DictReader(in_file, delimiter='|', quoting=csv.QUOTE_NONE)
                records_from_files[file_type] = []

                try:
                    for record in reader:
                        # Create a single attribute representing the compound primary key.
                        if file_type == 'named':
                            record['compound_key'] = record['ID'] + '_' + record['MapCode']
                        else:
                            record['compound_key'] = record['ParentID'] + '_' + record['MapCode']
                        records_from_files[file_type].append(record)
                except Exception as e:
                    print e

        #print records_from_files
        # This returns a generator for that class.
        return self._records_for_class_to_geojson(class_name, records_from_files)

    def __iter__(self):
        """
        Accepts a generator object yielding individual records.
        This will be a concatenation of generators, one for each
        geocoding class (e.g. Country, State, etc).
        """
        for row in self._cursor:
            yield row

def generate_select_statements(staging_prefix=None):
    """
    Return a dictionary of SELECT statements to be executed against the postgres database.
    If a staging_prefix is provided, re-format the table names to reflect the staging prefix.
    :param staging_prefix: Optional parameter used to generate staging table names.
    :return: A dictionary of sql statements to be executed against the postgres database.
    """
    queries = {
        'features': ["SELECT id, parent_id, class, map_code FROM ", "features"],
        'names': ["SELECT de_de, es_es, fr_fr, ja_jp, ko_kr, pt_br, zh_cn, none, id, map_code, fips, iso2, iso3 FROM ", "names"],
        'synonyms': ["SELECT name, map_code, id FROM ", "synonyms"],
        'points': ["SELECT id, map_code, ST_AsGeoJSON(the_geom) as pt_geom FROM ", "points"],
        'polygons': ["SELECT id, map_code, ST_AsGeoJSON(the_geom) as pl_geom FROM ", "polygons"]
    }

    # If a staging prefix was given, alter table names to reflect staging tables.
    if staging_prefix:
        for query_key in queries.iterkeys():
            staging_table_name = '_'.join(['staging', staging_prefix, queries[query_key][1]])
            queries[query_key] = queries[query_key][0] + staging_table_name
    else:
        for query_key in queries.iterkeys():
            queries[query_key] = queries[query_key][0] + queries[query_key][1]

    return queries

class ReaderPostgres(ReaderBaseClass):
    """
    Reader class implementation executing a single query via psycopg2.
    Defaults to querying out the contents of geocoding database.
    Specifically, the __iter__() method will yield a single dictionary
    output from an instance of the psycopg2.extras.RealDictCursor class.

    Data output are formatted as a GeoJSON Feature Object, whose Geometry Object
    is of type Geometry Collection. This allows for a single feature to store both
    Point and Polygon geometries.

    :param query: String representing a sql query. Defaults to extract all records.
    :param staging_prefixes: A list of comma-separated strings representing staging table prefix. Defaults to None.
    :param database: Database name. Defaults to `pg_geocoding`.
    :param user: DB user to connect as. Defaults to `test_user`.
    :param password: Password for DB user. Defaults to None.
    :param host: Host name, defaults to `pgsqlgis-repos`.
    :param port: Port number, defaults to 5432.
    """

    def __init__(self, query=None, staging_prefixes=None, database='pg_geocoding', user='test_user',
                 password=None, host='pgsqlgis-repos', port=5432, **kwargs):

        self.conn_params = {
            'database': database,
            'host': host,
            'port': port}
        # Append user and password if provided
        # Else i think this uses your system user. TODO: Check on that.
        if user:
            self.conn_params['user'] = user
        if password:
            self.conn_params['password'] = password
        # Use user provided query if given.
        self._conn_handler = self.open_connection(self.conn_params)

        # if staging prefixes is included, split into a list delimited by commas.
        # Else set to an empty list.
        if staging_prefixes:
            self.staging_prefixes = [prefix.strip() for prefix in staging_prefixes.split(',')]
        else:
            self.staging_prefixes = []

        # Entry Point for creation of merged set of staging/production records, if needed.
        self._production_data = self.query_datasets(self._conn_handler)
        self._staging_datasets = [self.query_datasets(self._conn_handler, prefix) for prefix in self.staging_prefixes]
        self._output_records_iterable = self._generate_output_iterable(self._production_data, self._staging_datasets)

    @classmethod
    def open_connection(self, conn_params):
        """
        Create an open psycopg2 connection. See psycopg2 docs for connection params.
        http://initd.org/psycopg/docs/module.html#psycopg2.connect

        :param conn_params: a dictionary of connection parameters to be passed to psycopg2.connect()
        :return: Returns a psycopg2 connection object.
        """
        conn = psycopg2.connect(database=conn_params['database'], user=conn_params['user'], password=conn_params['password'],
         host=conn_params['host'], port=conn_params['port'])
        return conn

    @classmethod
    def execute_query(self, db_conn, query, query_params=None):
        """
        Return a cursor with result set from self.query. See psycopg2 docs for query param syntax.
        http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries

        :param db_conn: A Psycopg2 connection object.
        :param query: A string representing a query to be executed.
        :param query_params: A tuple or dictionary of parameters to be passed to the sql query. See link in description.

        :return: Returns a cursor with the query results.
        """
        # Create cursor with Unicode support and Execute Query
        cur = db_conn.cursor()
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
        cur.execute(query, query_params)
        return cur

    @classmethod
    def validate_keys(self, in_data, table_name, keys=(0, 1)):
        """
        Given a Psycopg2 dict result set, ensure all combinations of keys are
        unique (DISTINCT).
        :param in_data: An iterable of records to check.
        :param table_name: The table being checked.
        :return: Returns data input if keys are validated.
        """
        # Check that keys are present for a given record, create list of compound primary keys.
        compound_keys = []
        for row in in_data:
            # Check that key values exist.
            print row
            for key in keys:
                print "======================================"
                print key
                print "======================================"
                if row[key] is None or row[key] == '':
                    raise ValueError("Record in table '%s' has an un-populated key field: %s" % (table_name, keys))
            # Add compound key to list of keys.
            compound_key = '_'.join(str(row[key]) for key in keys)
            compound_keys.append(compound_key)

        key_count = Counter(compound_keys)
        # Get the most count of the most common key (it's the form of a tuple (value, cnt))
        if key_count.most_common(1)[0][1] != 1:
            key_count_list = key_count.items()
            duplicate_keys = [key[0] for key in key_count_list if key[1] > 1]
            raise ValueError("Duplicate keys found in %s table of staging "
                             "or production data. Dup Keys are: %s" % (table_name, duplicate_keys))
        return in_data

    @classmethod
    def query_datasets(self, db_conn, staging_prefix=None):
        """
        Given a connection handler to the postgres database, Execute a series of queries to
        return these data (production or staging) as a dict whose keys represent table names
        (e.g. 'point', 'polygon', 'names', 'features', 'synonyms') and whose values represent
        rows from those tables.

        :param db_conn: An open connection to Postgres via Psycopg2.
        :param staging_prefix: Optional staging prefix name, used to query staging tables.
        :return: A dict representing data from data from the various tables representing a geocoding entity.
        """
        # Generate SELECT Statements for tables.
        table_queries = generate_select_statements(staging_prefix)

        # Create Cursors and Extract Data
        table_results = {}
        try:
            for table_name, table_query in table_queries.iteritems():
                table_cursor = self.execute_query(db_conn, table_query)
                # Extract Data, Validate Keys
                table_data = [row for row in table_cursor]
                # Note key validation is not applicable to synonyms,
                # which has valid duplicate id+mapcode combinations.
                # Nor is it applicable to empty tables.
                if table_name != 'synonyms' and len(table_data) > 0:
                    print "======================================"
                    print table_name
                    print "======================================"
                    self.validate_keys(table_data, table_name)
                table_results[table_name] = table_data

        except ProgrammingError as e:
            raise Exception("A Query Failed to Execute: %s" % e.message)

        # At this point we have a dict of data whose keys represent each table
        # (features, names, points, polygons, synonyms) and whose values
        # represent rows from these tables.
        return table_results

    @classmethod
    def dataset_to_hash_map(self, in_dataset):
        """
        Given a dataset formatted as output from self.query_datasets(),
        transform the dict values into a dict whose keys represent the compound
        primary identifier for a geocoding feature <id_mapcode>.
        :param in_dataset: Dictionary formatted from self.query_datasets()
        :return: A dataset with whose values have been transformed into dicts with <id_mapcode> keys
        """
        transformed_data = {}
        for table_name, table_values in in_dataset.iteritems():
            transformed_table = {}
            if table_name == 'synonyms':
                # Since synonyms have valid multiple records of the same key combo
                # Need to append their values to a list.
                for record in table_values:
                    compound_key = str(record['id']) + '_' + str(record['map_code'])
                    # If key exists, append to list; if not, append a new list with first value.
                    if compound_key in transformed_table:
                        transformed_table[compound_key].append(record)
                    else:
                        transformed_table[compound_key] = [record]
            else:
                # Since we've checked key integrity for non-synonym tables in self.validate_keys()
                # Assume a key combo exists in a table only once.
                for record in table_values:
                    compound_key = str(record['id']) + '_' + str(record['map_code'])
                    transformed_table[compound_key] = record
            # Assign data back out.
            transformed_data[table_name] = transformed_table

        return transformed_data

    def _apply_staging(self, in_production_hash, in_merge_hash):
        """
        Given a hash of production data and staging data formatted by dataset_to_hash_map(),
        return a set of the staging data applied to the production data.

        :param in_production_hash: A dataset to be used as the target of the merge operation.
        :param in_merge_hash: A dataset to be used as the source of records for the merge operation.
        :return: Dict of table names whose contents is the merged from staging to production.
        """

        # NOTE: I am choosing not to handle cases in which duplicate synonyms exist in either
        # production or staging. I think this should be a data test.

        # Update dictionary of production hash with data from merge hash
        # Synonym tables will need to be handled separately.
        for table_name in in_production_hash.iterkeys():
            if table_name != 'synonyms':
                # Note: This assumes `in_merge_hash has at least a dict key for each table`
                in_production_hash[table_name].update(in_merge_hash[table_name])
            else:
                # Synonyms are represented as a list of rows, rather then a single row.
                # Need to append to list rather then overwrite existing entries.
                for synonym_id, synonym_value in in_merge_hash[table_name].iteritems():
                    # Update
                    if synonym_id in in_production_hash[table_name]:
                        production_synonyms = in_production_hash[table_name][synonym_id]
                        in_production_hash[table_name][synonym_id] = production_synonyms + synonym_value
                    # Insert
                    else:
                        in_production_hash[table_name][synonym_id] = synonym_value
        # return merged dataset
        return in_production_hash

    def _records_to_geojson(self, in_data_hash):
        """
        Given a dict of Psycopg2 RealDictCursor rows for geocoding tables,
        assemble these into a single GeoJSON-like feature.
        :param in_data_hash:
        :return: A list of assembled GeoJSON-like dicts.
        """
        records_as_geojson = {}

        for table_name, table_data in in_data_hash.iteritems():
            for row_id, row_data in table_data.iteritems():
                # Check for presence of record insert template if none exists.
                if row_id not in records_as_geojson:
                    # Add new record
                    records_as_geojson[row_id] = {
                    'geometry': {'type': 'GeometryCollection',
                                 'geometries': []},
                    'type': 'Feature',
                    'properties': {
                        'none': None,
                        'fr_fr': None,
                        'pt_br': None,
                        'zh_cn': None,
                        'ko_kr': None,
                        'iso3': None,
                        'map_code': None,
                        'id': None,
                        'ja_jp': None,
                        'synonyms': [],
                        'iso2': None,
                        'fips': None,
                        'parent_id': None,
                        'de_de': None,
                        'es_es': None,
                        'class': None,
                        'en_us': None}
                    }
                # Push row into GeoJSON Template.
                if table_name in ['points', 'polygons']:
                    # Place geoms into geometry collection dict
                    records_as_geojson[row_id]['properties']['id'] = row_data.pop('id')
                    records_as_geojson[row_id]['properties']['map_code'] = row_data.pop('map_code')

                    # At this point, on the single geom object (pt_geom or pl_geom) to be loaded.
                    for geom in row_data.itervalues():
                        geom_obj = simplejson.loads(geom)
                        records_as_geojson[row_id]['geometry']['geometries'].append(geom_obj)

                elif table_name in ['synonyms']:
                    if len(row_data) > 0:
                        # create a list of synonyms values
                        synonyms_values = [row['name'] for row in row_data]
                        # Place synonyms into list.
                        records_as_geojson[row_id]['properties']['synonyms'] = synonyms_values
                        records_as_geojson[row_id]['properties']['id'] = row_data[0]['id']
                        records_as_geojson[row_id]['properties']['map_code'] = row_data[0]['map_code']
                else:
                    # Add keys into properties dict
                    records_as_geojson[row_id]['properties'].update(row_data)

        return records_as_geojson.values()

    def _generate_output_iterable(self, production_data, staging_datasets=None):
        """
        Given a dict of Psycopg2 RealDictCursor rows for production (required) and staging tables (optional),
        emit GeoJSON-like dicts for production data with staging data applied (if applicable).

        :param production_data: A dict containing geocoding table names that can be aggregated into GeoJSON-like features.
        :param staging_datasets: A list of dicts that can be applied to the production data after conversion to GeoJSON-like format.
        :return: An iterable of merged data (if applicable).
        """
        # Convert All Datasets into "Hashed Representation"
        production_data_as_hash = self.dataset_to_hash_map(production_data)

        for staging_dataset in staging_datasets:
            staging_dataset_as_hash = self.dataset_to_hash_map(staging_dataset)
            # Merge Dataset
            production_data_as_hash = self._apply_staging(production_data_as_hash, staging_dataset_as_hash)

        # Convert Merged Datasets into GeoJSON
        data_as_geojson = self._records_to_geojson(production_data_as_hash)

        # Return merged datasets
        return data_as_geojson

    def __del__(self, exc_type=None, exc_val=None, exc_tb=None):
        """Close the db connection. Note: Will be Called Twice if a Context Manager is used."""
        if self._conn_handler:
            self._conn_handler.close()
        if exc_type is not None:
            # Exception occurred
            return False  # Will raise the exception
        return True  # Everything's okay

    def __iter__(self):
        """Yield a single record back to the caller as a dict
        formatted as a GeoJSON Feature."""
        for row in self._output_records_iterable:
            yield row
