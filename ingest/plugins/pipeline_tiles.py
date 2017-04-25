'''pipeline_tiles - a tile processor for the ariadne microns pipeline

The path processor uses a sqlite database to get the path of each tile.

The schema of the database is

        tiles (
            column integer not null,
            row integer not null,
            z integer not null,
            location text not null,
            primary key (column, row, z)
        )

'''

from PIL import Image
import sqlite3

from .path import PathProcessor
from .tile import TileProcessor

class PipelineTilePathProcessor(PathProcessor):
    '''Process tile paths as supplied by the ariadne microns pipeline
    
    The BossPipelineTask generates a database with a single table
    containing the locations of the tiles.
    '''
    
    def __init__(self):
        PathProcessor.__init__(self)
    
    def setup(self, parameters):
        '''Initialize the processor with its parameters
        
        :param parameters: a dictionary of the initialization parameters
                           For this plugin, "database" is required and
                           has the path to the sqlite database.
        '''
        self.database = parameters["database"]
        self.connection = sqlite3.connect(self.database)
        self.cursor = self.connection.cursor()
    
    def process(self, xi, yi, zi, ti=None):
        '''Compute the file path for a given index
        
        :param xi: the X index or column of the tile
        :param yi: the Y index or row of the tile
        :param zi: the 0-based z coordinate of the tile
        :param ti: ignored
        :returns: A path to the indexed tile
        '''
        path = self.cursor.execute(
            """select location from tiles
               where column=%d
                 and row=%d
                 and z=%d""" % (xi, yi, zi)).fetchone()[0]
        return path

class PipelineTileProcessor(TileProcessor):
    '''A simple tile processor for the pipeline output'''
    
    def __init__(self):
        TileProcessor.__init__(self)
    
    def setup(self, parameters):
        '''Set up the processor
        
        :param parameters: The customization parameters for the plugin.
                           There are no parameters.
        '''
        pass

    def process(self, path, xi, yi, zi, ti=0):
        '''Return a file handle for the tile
        
        :param path: the path to the tile file
        :param xi: the x index of the tile (column). Ignored.
        :param yi: the y index of the tile (row). Ignored.
        :param zi: the z coordinate of the tile. Ignored
        :param ti: the time coordinate of the tile. Ignored.
        '''
        return open(path, "rb")