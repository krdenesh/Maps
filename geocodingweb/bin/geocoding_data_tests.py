import ConfigParser
import logging
import os
import sys
import readers
import json
from shapely.geometry import Polygon, shape
from shapely.ops import cascaded_union
from shapely.geos import TopologicalError

class GeoCodingTest():

    """
    Base class which populates a list attribute `self.csv_test_data` with
    records from the local csv directory.
    """
    # Uncomment and run with `nosetests --verbose --processes=N
    # _multiprocess_shared_ = True
    def __init__(self, params):
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

        for key, value in params.iteritems():
            if key == "input_type":
                self.user_config = ConfigParser.RawConfigParser()
                self.user_config.add_section('nosetests')
                self.user_config.set('nosetests', 'data_type',value)
                if value == 'csv':
                    self.user_config.add_section('user_csv_settings')
                    self.user_config.set('user_csv_settings', 'local_csv_path', params['path_to_csv'])
                else:
                    self.user_config.add_section('user_postgres_settings')
                    self.user_config.set('user_postgres_settings', 'pg_host', params['host'])
                    self.user_config.set('user_postgres_settings', 'pg_database', params['database'])
                    self.user_config.set('user_postgres_settings', 'pg_user', params['user'])
                    self.user_config.set('user_postgres_settings', 'pg_password', params['password'])
                    self.user_config.set('user_postgres_settings', 'pg_staging_prefixes', params['staging_prefix'])

        self.test_data = self.get_input_data()
        #self.assertIsNotNone(self.test_data, "Incorrect data type given: Data type in config should be 'csv'/'postgres'.")

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
                    print failure_message

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

    def test_invalidShape(self):
        """
        Test to verify if there is an invalid shape.
        a. 2 vertex polygon_shapes
        b. self intersecting polygon_shapes
        c. minimum area
        """
        features_invalid_shape = {}
        for record in self.test_data:
            geoms = record['geometry']['geometries']
            polygon = self._get_Polygon(record)
            if polygon is not None:
                # is_invalid considers OpenGIS Implementation Specification for Geographic information - Simple feature access
                # http://toblerity.org/shapely/manual.html#id22
                if not polygon.is_valid:
                    if record['properties']['id'] not in features_invalid_shape:
                        features_invalid_shape[record['properties']['id']] = record
        json_data = json.dumps(features_invalid_shape)
        return json_data

    def test_PointInPolygon(self):
        """
        Test that a feature's point geometry is within its polygon geometry.
        NOTE: A feature that does not contain a polygon geometry will be skipped.
        """
        features_point_outside_polygon = {}
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
                        features_point_outside_polygon[composite_key] = geoms
                except Exception as e:
                    continue

        json_data = json.dumps(features_point_outside_polygon)
        return json_data

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
                        'polygon': shape(geom),
                        'geoms':geoms}
                        polygon_shapes.append(class_mapcode_polygons)
                        count +=1
                        idx.insert(count, shape(geom).bounds)

        #check for overapping polygons
        features_with_overlapping_polygons = {}
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

                            if not features_with_overlapping_polygons.has_key(feature_2 + ";" + feature_1):
                                print "++++++++++++++++++++++++++"
                                print polygon_shapes[key]['geoms']
                                print data['geoms']
                                print "++++++++++++++++++++++++++"
                                features_with_overlapping_polygons[feature_1 + ";" + feature_2]["type"] = "GeometryCollection"
                                features_with_overlapping_polygons[feature_1 + ";" + feature_2]["geometries"] = polygon_shapes[key]['geoms']
                                #features_with_overlapping_polygons[feature_1 + ";" + feature_2]  = polygon_shapes[key]['geoms']
                                features_with_overlapping_polygons[feature_1 + ";" + feature_2]["geometries"].append(data['geoms'])
                    except Exception as e:
                        error_dict = {
                            'composite_keys': feature_1 + ";" + feature_2,
                            'error_string': str(e)
                            }
                        ids_with_topology_error.append(error_dict)
                        continue

        json_data = json.dumps(features_with_overlapping_polygons)
        return json_data

    def _get_comp_primary_key(self, record):
        """
        Private method to get id_map_code from the imput data
        """
        return str(record['properties']['id']) + "_" + str(record['properties']['map_code'])

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