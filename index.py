import utils

class Index(): 
    def __init__(self, index):
        # list of last row for each partition
        self.last_rows = list(index.keys())
        # list of path for each partition
        self.paths = list(index.values())

    def find_partition_index(self, row):
        """
        Find the partition index in the index data structure

        row: row that the caller wants the partition index

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

        Index data structure:
            The index data structure is a dict, but is not used as such.
            keys() hold the "last row + 1" of each partition.
            values() hold the path of each partition file.
        """
        # variables with index_<> refer to index as a the whole "indexing data structure" (index)
        # variables with <>_index refer to the actual index number (an integer) in the "indexing data structure" (index)

        first_row_index = self.find_partition_index(first_row)
        last_row_index = self.find_partition_index(last_row)

        # +1 because we also want the last partition 
        paths = [self.paths[i] for i in range(first_row_index, last_row_index + 1)]

        # start is 0 if first partition is the first of the dataset
        # otherwise, the start is the previous partition "row" (the one that determines the partition in the index)
        start = 0 if first_row_index == 0 else self.last_rows[first_row_index - 1]
        # end is the partition "row"
        end = self.last_rows[last_row_index]

        return paths, start, end
