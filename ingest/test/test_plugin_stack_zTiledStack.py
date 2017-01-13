from __future__ import absolute_import

import os
import unittest
import json
from pkg_resources import resource_filename

from PIL import Image
import numpy as np

#from moto import mock_s3
#import boto3

from ingest.core.config import Configuration


class ZImageTiledStackMixin(object):

    def test_PathProcessor_process(self):
        """Test running the path processor"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())
        assert pp.process(0, 0, 0, 0) == "{}/2124_W09_Sec076_montaged/2124_W09_Sec076_montaged".format(pp.parameters["root_dir"])
        assert pp.process(0, 0, 1, 0) == "{}/2124_W09_Sec076_montaged/2124_W09_Sec076_montaged".format(pp.parameters["root_dir"])
        assert pp.process(0, 0, 2, 0) == "{}/2126_W09_Sec078_montaged/2126_W09_Sec078_montaged".format(pp.parameters["root_dir"])

    def test_PathProcessor_process_invalid(self):
        """Test running the path processor with invalid tile indices"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())

        with self.assertRaises(IndexError):
            pp.process(0, 0, 3, 0)

        with self.assertRaises(IndexError):
            pp.process(0, 0, 0, 1)

    def test_TileProcessor_process(self):
        """Test running the tile processor"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())

        tp = self.config.tile_processor_class
        tp.setup(self.config.get_tile_processor_params())

        filename = pp.process(0, 0, 0, 0)
        handle = tp.process(filename, 0, 0, 0, 0)

        # Open handle as image file
        test_img = Image.open(handle)
        test_img = np.array(test_img, dtype="uint8")

        # Open original data
        truth_file = os.path.join(resource_filename("ingest", "test/data/example_z_tiled_stack/"), "2124_W09_Sec076_montaged", "2124_W09_Sec076_montaged_tr1-tc1.png")
        truth_img = Image.open(truth_file)
        truth_img = np.array(truth_img, dtype="uint8")

        # Make sure the same
        np.testing.assert_array_equal(truth_img, test_img)


class TestZImageTiledStackLocal(ZImageTiledStackMixin, unittest.TestCase):

    def test_PathProcessor_setup(self):
        """Test setting up the path processor"""
        pp = self.config.path_processor_class
        pp.setup(self.config.get_path_processor_params())

        assert pp.parameters["root_dir"] == resource_filename("ingest", "test/data/example_z_tiled_stack")
        assert pp.parameters["ingest_job"]["extent"]["y"] == [0, 512]

    def test_TileProcessor_setup(self):
        """Test setting up the tile processor"""
        tp = self.config.tile_processor_class
        tp.setup(self.config.get_tile_processor_params())

        assert tp.parameters["extension"] == "png"
        assert tp.parameters["filesystem"] == "local"
        assert tp.parameters["ingest_job"]["extent"]["y"] == [0, 512]

    @classmethod
    def setUpClass(cls):
        cls.config_file = os.path.join(resource_filename("ingest", "test/data"), "boss-v0.1-zTiledStack.json")

        with open(cls.config_file, 'rt') as example_file:
            cls.example_config_data = json.load(example_file)

        # inject the file path since we don't want to hardcode
        cls.example_config_data["client"]["path_processor"]["params"]["root_dir"] = resource_filename("ingest",
                                                                                                      "test/data/example_z_tiled_stack")

        cls.config = Configuration(cls.example_config_data)
        cls.config.load_plugins()








