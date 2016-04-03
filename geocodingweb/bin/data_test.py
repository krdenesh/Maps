#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. module:: test_local_csvs.py
   :platform: Unix
   :synopsis: Tests to be applied to the set of local CSV files and Postgres backing store.

.. moduleauthor:: Maps Backend Team

These tests are designed to be executed against
1. "Local CSV Files" generated using the `extract_geocoding.py` tool.
2. A postgres backing store, currently `pg_geocoding`.

`nose.cfg` notes:

- data_type: csv/postgres

- Block to configure: `[user_csv_settings]`
- Configuration Parameters:
  - `local_csv_path`: A relative or absolute path to a set of Local CSV files.

- Block to configure: `[user_postgres_settings]`
- Configuration Parameters:
  - `pg_database`: Database name for db connection.
  - `pg_user`: User name for db connection.
  - `pg_password`: Password for pg_connection.
  - `pg_host`: Hostname for pg_connection.
  - `pg_staging_prefixes`: A comma-separated list of one or more "staging schema prefixes".
"""

import ConfigParser
import logging
import os
import sys
import unittest
from tab_geocoding_test import readers
from shapely.geometry import Polygon, shape
from shapely.ops import cascaded_union
from shapely.geos import TopologicalError

class GeoTestBaseClass(unittest.TestCase):

    """
    Base class which populates a list attribute `self.csv_test_data` with
    records from the local csv directory.
    """
    # Uncomment and run with `nosetests --verbose --processes=N
    # _multiprocess_shared_ = True
    def setUp(self):
        """
        Load records from a local CSV output.
        :return:
        """
        self.logger = logging.getLogger(name=__name__)
        self.logger.setLevel( logging.DEBUG )
        hdlr = logging.FileHandler('out.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)

        self.logger.info("Setting up environment for tests...")

        # Reading nose.cfg
        config_path = os.path.join(os.path.split(os.path.split(__file__)[0])[0], 'nose.cfg')
        self.user_config = ConfigParser.RawConfigParser()
        self.user_config.read(config_path)

        self.test_data = self.get_input_data()
        self.assertIsNotNone(self.test_data, "Incorrect data type given: Data type in config should be 'csv'/'postgres'.")

        # Append value of assert's `msg` parameter to end of normal failure output
        # Do not overwrite output.
        self.longMessage = True

        #place holder for input data to be stored with id as keys in the dictionary
        # for faster look ups based on ids
        # it also takes care of testing the uniquness of the compund
        # primary key id and map code
        self.id_indexed_record = {}
        for record in self.test_data:
            if record.has_key('properties'):
                if not self.id_indexed_record.has_key(str(record['properties']['id']) +
                '_' + str(record['properties']['map_code'])):
                    self.id_indexed_record[str(record['properties']['id']) +
                    '_' + str(record['properties']['map_code'])] = record
                else:
                    failure_message = "Record already exists with the compound primary key {0}_{1}".format(str(record['properties']['id']),
                        str(record['properties']['map_code']))
                    self.assertFalse(True, msg=failure_message)

        # IDs identified with Invalid polygon shapes
        self.ids_with_invalid_shapes = []

    def get_input_data(self):
        """
        Helper method to return the input data based on data type
        """
        #loads test data from local csv
        self.data_type = self.user_config.get('nosetests', 'data_type')
        self.logger.debug("Input Data Type -> " + self.data_type)
        if(self.data_type == 'csv'):
            local_csv_conn = readers.ReaderLocalCSVs(
                in_dir=self.user_config.get('user_csv_settings', 'local_csv_path')
            )

            # Create test data lists
            return [r for r in local_csv_conn]

        #loads test data from postgres
        elif(self.data_type == 'postgres'):
            # Check if config has staging parameter.
            staging_prefixes = None
            if self.user_config.has_option('user_postgres_settings', 'pg_staging_prefixes'):
                staging_prefixes = self.user_config.get('user_postgres_settings', 'pg_staging_prefixes')

            pg_geocoding_conn = readers.ReaderPostgres(
                database=self.user_config.get('user_postgres_settings', 'pg_database'),
                user=self.user_config.get('user_postgres_settings', 'pg_user'),
                password=self.user_config.get('user_postgres_settings', 'pg_password'),
                host=self.user_config.get('user_postgres_settings', 'pg_host'),
                staging_prefixes=staging_prefixes
            )
            # Create test data lists
            return pg_geocoding_conn
        else:
            return None

class TestProperties(GeoTestBaseClass):
    """
    Perform tests specific to properties for geocodable entities.
    """

    def test_SingleNameClassesHavePopulatedNoneColumn(self):
        """
        Area Code and Zip Code entries have a populated `none` column.

        NOTE: This test may no longer be required if we change the extractor code to not
        emit a record for the above classes in the event that postgres has a Null value
        or empty string in the `None` column.
        """
        none_isNoneString = []
        none_isEmptyString = []
        for record in self.test_data:
            # Test applies only to specific classes.
            if record['properties']['class'] in (100, 101):

                none_val = record['properties']['none']
                # Check for Python NoneType in `none` column. Funny, right?
                if none_val == 'None':
                    composite_key = self._get_comp_primary_key(record)
                    none_isNoneString.append(composite_key)

                # Check for an empty string.
                if none_val == '':
                    composite_key = self._get_comp_primary_key(record)
                    none_isEmptyString.append(composite_key)

        self.assertEqual(len(none_isNoneString), 0, msg="The following features are expected to have a valid value"
                                                      " in the `none` column, but have a string with a value of None: %s" % none_isNoneString)
        self.assertEqual(len(none_isEmptyString), 0, msg="The following features are expected to have a valid value"
                                                         " in the `none` column, but have an empty string: %s" % none_isEmptyString)

class TestGeometries(GeoTestBaseClass):
    """
    Perform tests specific to geometries for geocodable entities.
    """

    def test_HasWGS84PointGeom(self):
        """
        Test to ensure that every Postgres feature has a point geometry within a WGS84 BBOX.
        """
        world_bbox = Polygon([(-180, 90), (180, 90), (180, -90), (-180, -90)])
        features_no_points = []
        for record in self.test_data:
            # Iterate through a feature's geometries, check for point attr,
            # then check for population of point attr
            has_valid_point = False
            if 'geometry' in record.keys():
                for geom in record['geometry']['geometries']:
                    if geom['type'] == 'Point':
                        # Check point is within WGS84 BBOX
                        shapely_pt = shape(geom)
                        if world_bbox.intersects(shapely_pt):
                            has_valid_point = True
            # if we have no point or an invalid point, flag it.
            if not has_valid_point:
                composite_key = self._get_comp_primary_key(record)
                features_no_points.append(composite_key)

        # Raise error if we don't have an empty error list
        self.assertEqual(len(features_no_points), 0, msg="The following features have invalid point geometries %s" % features_no_points)

    def test_HasNoPointsOnNullIsland(self):
        """
        Test to ensure there is no points on null island
        """
        features_on_null_island = []
        for record in self.test_data:
            # pull down from database
            # Iterate through the points
            # inside iteration, check if it is on null island
            null_island_point = False
            if 'geometry' in record.keys():
                for geom in record['geometry']['geometries']:
                    if geom['type'] == 'Point':
                        # Check point is not on null island
                        # Convert point to shapely geom
                        shapely_pt = shape(geom)
                        # Coordinate values are accessed from the coords object
                        # using 'list'
                        if list(shapely_pt.coords) == [(0.0, 0.0)]:
                            null_island_point = True
            # if there is a point on null island, flag it.
            if null_island_point:
                composite_key = self._get_comp_primary_key(record)
                features_on_null_island.append(composite_key)
        #Raise Error
        self.assertEqual(len(features_on_null_island), 0, msg="The following features have point geometries on null island %s" % features_on_null_island)

    def test_PointInPolygonGeom(self):
        """
        Test that a feature's point geometry is within its polygon geometry.
        NOTE: A feature that does not contain a polygon geometry will be skipped.
        """
        features_point_outside_polygon = []
        for record in self.test_data:
            geoms = record['geometry']['geometries']
            pt_geom = None
            poly_geom = None
            # Check for presence of point and polygon geoms.
            for geom in geoms:
                if geom['type'] == 'Point':
                    pt_geom = shape(geom)
                if geom['type'] in ('MultiPolygon', 'Polygon'):
                    poly_geom = shape(geom)
            if pt_geom and poly_geom:
                # Check if point is within polygon. If not, flag it.
                try:
                    if not pt_geom.within(poly_geom):
                        composite_key = self._get_comp_primary_key(record)
                        features_point_outside_polygon.append(composite_key)
                except Exception as e:
                    continue

        # Raise error if we don't have an empty error list
        self.assertEqual(len(features_point_outside_polygon), 0,
                         msg="The following features have point geoms outside of polygon geoms %s" % features_point_outside_polygon)

    def test_overlappingPolygons(self):
        """
        Test to ensure there are No two polygons of same Class and Mapcode overlay
        """
        #Build a spatial index based on the bounding boxes of the polygons
        from rtree import index
        idx = index.Index()
        count = -1
        #iterating through input data to index
        polygon_shapes = []
        self.logger.info("Building the spatial index based on bounding boxes...")
        for record in self.test_data:
            if 'geometry' in record.keys():
                geoms = record['geometry']['geometries']
                # Check for presence of polygon geoms.
                for geom in geoms:
                    if geom['type'] in ('MultiPolygon', 'Polygon'):
                        class_mapcode_polygons = {'id': record['properties']['id'],
                        'class': record['properties']['class'],
                        'map_code': record['properties']['map_code'],
                        'polygon': shape(geom)}
                        polygon_shapes.append(class_mapcode_polygons)
                        count +=1
                        idx.insert(count, shape(geom).bounds)

        #check for overapping polygons
        features_with_overlapping_polygons = []
        ids_with_topology_error = []
        self.logger.info("Verifying overlapping polygons...")
        chumma_count = 0
        for data in polygon_shapes:
            for key in idx.intersection(data['polygon'].bounds):
                if(data['map_code'] == polygon_shapes[key]['map_code'] and
                    data['class'] == polygon_shapes[key]['class'] and
                    polygon_shapes[key]['polygon'] != data['polygon']):
                    #verifying overlap
                    try:
                        feature_1 = str(polygon_shapes[key]['id']) + '_' + str(polygon_shapes[key]['map_code'])
                        feature_2 = str(data['id']) + '_' + str(data['map_code'])
                        if polygon_shapes[key]['polygon'].overlaps(data['polygon']):

                            if feature_2 + ";" + feature_1 not in features_with_overlapping_polygons:
                                features_with_overlapping_polygons.append(feature_1 + ";" + feature_2)
                    except Exception as e:
                        error_dict = {
                            'composite_keys': feature_1 + ";" + feature_2,
                            'error_string': str(e)
                            }
                        ids_with_topology_error.append(error_dict)
                        continue

        # Print ids that caused topology errors
        if len(ids_with_topology_error) is not 0:
            self._print_ids_with_topology_error("These are a list of IDs that have TopologicalErrors that COULD NOT be checked for overlaps",
                ids_with_topology_error)
        # Raise error if we don't have an empty error list
        self.assertEqual(len(features_with_overlapping_polygons), 0,
                         msg="The following features have overlapping polygons %s" % features_with_overlapping_polygons)

    def test_polygonFallsWithinParent(self):
        """
        Test to ensure a feature’s polygon falls within the polygon of its parent
        """
        features_polygon_falls_within_parent = []
        ids_with_topology_error = []
        for record in self.test_data:
            child_polygon = self._get_Polygon(record)
            # checking is the record is not a country for csv data
            if child_polygon is not None and record['properties']['class'] is not 0:
                index_key = str(record['properties']['parent_id']) + "_" + str(record['properties']['map_code'])
                if self.id_indexed_record.has_key(index_key):
                    parent_polygon = self._get_Polygon(self.id_indexed_record[index_key])
                    if parent_polygon is not None:
                        # verifying if the child polygon intersects with the parent polygon
                        try:
                            if not parent_polygon.contains(child_polygon):
                                features_polygon_falls_within_parent.append(str(record['properties']['id']) + "_" + str(record['properties']['map_code']))
                        except Exception as e:
                            error_dict = {
                            'composite_key': str(record['properties']['id']) + "_" + str(record['properties']['map_code']),
                            'error_string': str(e)
                            }
                            ids_with_topology_error.append(error_dict)
                            continue
                    else:
                        pass
                else:
                    self.logger.info("Record's parent ID does not have a corresponding ID mapping -> %s" %record)

        # Print ids that caused topology errors
        if len(ids_with_topology_error) is not 0:
            self._print_ids_with_topology_error("These are a list of IDs that have TopologicalErrors that COULD NOT be checked for contains",
                ids_with_topology_error)
        # Raise error if we don't have an empty error list
        self.assertEqual(len(features_polygon_falls_within_parent), 0,
                         msg="The following features have polygon that does not fall within its parent %s" % features_polygon_falls_within_parent)

    def test_invalidShape(self):
        """
        Test to verify if there is an invalid shape.
        a. 2 vertex polygon_shapes
        b. self intersecting polygon_shapes
        c. minimum area
        """
        print "=======================sadfasfsdfafd===================="
        features_invalid_shape = []
        for record in self.test_data:
            polygon = self._get_Polygon(record)
            if polygon is not None:
                # is_invalid considers OpenGIS Implementation Specification for Geographic information - Simple feature access
                # http://toblerity.org/shapely/manual.html#id22
                if not polygon.is_valid:
                    features_invalid_shape.append(self._get_comp_primary_key(record))

        # Raise error if we don't have an empty error list
        self.assertEqual(len(features_invalid_shape), 0,
                         msg="The following features have invalid polygon shape %s" % features_invalid_shape)

    def _get_Polygon(self, record):
        """
        Private method to get the shape object from the imput data
        """
        if 'geometry' in record.keys():
            geoms = record['geometry']['geometries']
            for geom in geoms:
                if geom['type'] in ('MultiPolygon', 'Polygon'):
                    return shape(geom)
                else:
                    return None
        else:
            return None

    def _get_comp_primary_key(self, record):
        """
        Private method to get id_map_code from the imput data
        """
        return str(record['properties']['id']) + "_" + str(record['properties']['map_code'])

    def _print_ids_with_topology_error(self, message, records):
        """
        private helper method to print the list of records
        """
        self.logger.info("==========================================")
        self.logger.info(message + ": ")
        self.logger.info(records)
        self.logger.info("==========================================")