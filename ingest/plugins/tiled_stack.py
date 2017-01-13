# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
import six
from PIL import Image
import re
import os
#from ingest.utils.filesystem import DynamicFilesystem
import sys

from ingest.plugins.path import PathProcessor
from ingest.plugins.tile import TileProcessor


class ZindexTiledStackPathProcessor(PathProcessor):
    """Class for simple multiple tiles image stacks that only increment in Z, only supports local filesystem.
       Assumes that all tile images are of the same size (width, height)."""
    def __init__(self):
        """Constructor to add custom class var"""
        PathProcessor.__init__(self)
        self.folder_mapper = dict() # Maps a section# to the folder that contains its images
        self.tile_shape = None # A (width, height) for each small tile in the dataset
        self.min_z_index = sys.maxsize

    def setup(self, parameters):
        """Set the params

        The tile filenames must be of the type: 0351_W02_Sec141_montaged_tr4-tc15.png

        MUST HAVE THE CUSTOM PARAMETERS: "root_dir": "<path_to_stack_root>",
                                         "extension": "<png|tif|jpg>",
                                         "filesystem": "<local>", (TOOD - add support for s3)
                                         "bucket": (if s3 filesystem), (TODO - add support for s3)
                                         "tile_shape": [width, height] (example: [1024, 1024])

        Includes the "ingest_job" section of the config file automatically

        Args:
            parameters (dict): Parameters for the dataset to be processed

        Returns:
            None
        """
        self.parameters = parameters
        assert self.parameters["filesystem"] == "local"

        folder_regex = re.compile('([0-9]+)_.*')

        # Read all directories inside the root dir (each of the form <GolbalSection#>_W<Wafer#>_Sec<LocalSec#>[optional string]).
        # We only care about the GlobalSection# which is the section unmber in the entire dataset
        folders = os.listdir(self.parameters['root_dir'])
        for folder in folders:
            matches = folder_regex.findall(folder)
            if matches is None:
                continue

            z_index = int(matches[0])
            self.folder_mapper[z_index] = os.path.join(self.parameters['root_dir'], folder)
            self.min_z_index = min(self.min_z_index, z_index)

        ## Read a directory (one of the sections) which contains all the tiles of a single section
        #some_z_index = self.folder_mapper.keys()[0]
        #for tile_img_fname in os.listdir(self.folder_mapper[some_z_index]):
        #    if tile_img_fname.endswith(self.parameters["extension"]):
        #        # Open the first file, and get its shape
        #        with open(os.path.join(), 'r') as file_handle:
        #            tile_data = Image.open(file_handle)
        #            self.tile_shape = tile_data.size
        #        break
        
        
        

    def process(self, x_index, y_index, z_index, t_index=None):
        """
        Method to compute the file path for the indicated tile

        Args:
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension (0-based)
            t_index(int): The time index

        Returns:
            (str): A path to the directory where the tiles are located
#A prefix for all the tile filenames (everything but the _tr{}-tc{}.ext)

        """
        if t_index != 0:
            raise IndexError("Z Image Stack only supports non-time series data")

        if z_index >= self.parameters["ingest_job"]["extent"]["z"][1]:
            raise IndexError("Z-index out of range")

        # A closest neighbor search
        actual_z_index = z_index + self.min_z_index
        if actual_z_index not in self.folder_mapper:
            # search for the closest neighbor
            neighbor_z_index = self._find_closest_neighbor(actual_z_index)
            # update the mapping to include the new z_index
            self.folder_mapper[actual_z_index] = self.folder_mapper[neighbor_z_index]

        return self.folder_mapper[actual_z_index]
        #prefix = os.path.join(self.folder_mapper[actual_z_index], os.path.basename(self.folder_mapper[actual_z_index]))
        #return prefix

    def _find_closest_neighbor(self, z_index):
        MAX_DELTA = 5
        for delta in range(MAX_DELTA):
            if z_index - delta in self.folder_mapper:
                return z_index - delta
            elif z_index + delta in self.folder_mapper:
                return z_index + delta
        assert(False)
        return -1

class ZindexTiledStackTileProcessor(TileProcessor):
    """A Tile processor for a single image file identified by z index.
       Assumes that all tile images are of the same size (width, height)."""

    def __init__(self):
        """Constructor to add custom class var"""
        TileProcessor.__init__(self)
        self.fs = None

    def setup(self, parameters):
        """ Method to load the file for uploading

        Args:
            parameters (dict): Parameters for the dataset to be processed


        MUST HAVE THE CUSTOM PARAMETERS: "extension": "<png|tif|jpg>",
                                         "filesystem": "<local>", (TOOD - add support for s3)
                                         "bucket": (if s3 filesystem), (TODO - add support for s3)
                                         "tile_shape": [width, height] (example: [1024, 1024])

        Returns:
            None
        """
        self.parameters = parameters
        #self.fs = DynamicFilesystem(parameters['filesystem'], parameters)

    def _find_tile(self, path, r, c):
        print('path', path, 'r', r, 'c', c)
        suffix = '_tr{}-tc{}.{}'.format(r, c, self.parameters["extension"])

        # Read all files in the directory, and find one with the given row and column
        files = os.listdir(path)
        for fname in files:
            if fname.endswith(suffix):
                return os.path.join(path, fname)

        return None

    def process(self, path, x_index, y_index, z_index, t_index=0):
        """
        Method to load the image file. Can break the image into smaller tiles to help make ingest go smoother, but
        currently must be perfectly divisible

        Args:
            #file_prefix(str): An absolute file path prefix for the specified tile (everythin but the _tr{}-tc{}.ext)
            path(str): An absolute file path for the specified tile
            x_index(int): The tile index in the X dimension
            y_index(int): The tile index in the Y dimension
            z_index(int): The tile index in the Z dimension (0-based)
            t_index(int): The time index

        Returns:
            (io.BufferedReader): A file handle for the specified tile

        """
        x_range = [self.parameters["ingest_job"]["tile_size"]["x"] * x_index,
                   self.parameters["ingest_job"]["tile_size"]["x"] * (x_index + 1)]
        y_range = [self.parameters["ingest_job"]["tile_size"]["y"] * y_index,
                   self.parameters["ingest_job"]["tile_size"]["y"] * (y_index + 1)]

        col_range = [int(x_range[0] / self.parameters["tile_shape"][0]), int(x_range[1] / self.parameters["tile_shape"][0])]
        row_range = [int(y_range[0] / self.parameters["tile_shape"][1]), int(y_range[1] / self.parameters["tile_shape"][1])]
        if col_range[0] == col_range[1] and row_range[0] == row_range[1]:
            # If the wanted range exists in a single file, open that file and crop the desired range
            fname = self._find_tile(path, row_range[0] + 1, col_range[0] + 1)
            tile_data = Image.open(fname)
            upload_img = tile_data.crop((x_range[0] - col_range[0] * self.parameters["tile_shape"][0],
                                         y_range[0] - row_range[0] * self.parameters["tile_shape"][1],
                                         x_range[1] - col_range[0] * self.parameters["tile_shape"][0],
                                         y_range[1] - row_range[0] * self.parameters["tile_shape"][1]))
        else:
            # Create a place holder for the image to upload
            upload_img = Image.new("L", (x_range[1] - x_range[0], y_range[1] - y_range[0]))

            # Load all the relevant images, and crop them and paste them into the output image
            for r in range(*row_range):
                for c in range(*col_range):
                    fname = self._find_tile(path, r + 1, c + 1)
                    tile_data = Image.open(fname)
                    cropped_img = tile_data.crop((max(x_range[0] - c * self.parameters["tile_shape"][0], 0),
                                                  max(y_range[0] - r * self.parameters["tile_shape"][1], 0),
                                                  min(x_range[1] - c * self.parameters["tile_shape"][0], self.parameters["tile_shape"][0]),
                                                  min(y_range[1] - r * self.parameters["tile_shape"][1], self.parameters["tile_shape"][1])))
                    # paste the image into the desired location
                    upload_img.paste(cropped_img, 
                                     (max(c * self.parameters["tile_shape"][0] - x_range[0], 0), max(r * self.parameters["tile_shape"][1] - y_range[0], 0)))

        
        # Save sub-img to png and return handle
        output = six.BytesIO()
        upload_img.save(output, format=self.parameters["extension"].upper())

        # Send handle back
        return output
