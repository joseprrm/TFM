import utils

class Index(): 
    """
    Index data structure:
        The index data structure is a dict, but is not used as such.
        keys() hold the "last row + 1" of each partition.
        values() hold the path of each partition file.

    index means the index object or index data structure
    idx is the integer that determines the position in the self.last_rows and self.paths arrays
    """
    def __init__(self, index):
        # list of last row for each partition
        self.last_rows = list(index.keys())
        # list of path for each partition
        self.paths = list(index.values())

    def find_partition_idx(self, row):
        """
        Find the partition idx in the index data structure

        row: row that the caller wants the partition idx

        returns: the number of the partition
        """
        return utils.first_true_element([row < i for i in self.last_rows])

    def find_partition_paths(self, first_row, last_row):
        """
        first_row: first row in the range
        last_row: last row in the range (exclusive range)

        returns:
            paths: list of the paths of the partitions
            start: first row in the partition group
            end: last row in the partition group (exclusive range)

        """
        first_row_idx = self.find_partition_idx(first_row)
        last_row_idx = self.find_partition_idx(last_row)

        # +1 because we also want the last partition 
        paths = [self.paths[i] for i in range(first_row_idx, last_row_idx + 1)]

        # start is 0 if first partition is the first of the dataset
        # otherwise, the start is the previous partition "row" (the one that determines the partition in the index)
        start = 0 if first_row_idx == 0 else self.last_rows[first_row_idx - 1]
        # end is the partition "row"
        end = self.last_rows[last_row_idx]

        return paths, start, end
