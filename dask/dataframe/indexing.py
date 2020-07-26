from datetime import datetime
from collections import defaultdict

import bisect
import numpy as np
import pandas as pd

from .core import new_dd_object, Series
from ..array.core import Array
from .utils import is_index_like, meta_nonempty
from . import methods
from ..base import tokenize
from ..highlevelgraph import HighLevelGraph


class _IndexerBase(object):
    def __init__(self, obj):
        self.obj = obj

    @property
    def _name(self):
        return self.obj._name

    @property
    def _meta_indexer(self):
        raise NotImplementedError

    def _make_meta(self, iindexer, cindexer):
        """
        get metadata
        """
        if cindexer is None:
            return self.obj
        else:
            return self._meta_indexer[:, cindexer]


class _iLocIndexer(_IndexerBase):
    @property
    def _meta_indexer(self):
        return self.obj._meta.iloc

    def __getitem__(self, key):
        if isinstance(key, int):
            key = slice(key, key+1)

        if isinstance(key, slice):
            key = tuple([key])

        if not isinstance(key, tuple):
            raise ValueError("Expected slice or tuple, got %s" % str(key))

        obj = self.obj

        if len(key) == 0:
            return obj
        elif len(key) == 1:
            iindexer = key[0]
            cindexer = slice(None)
        elif len(key) == 2:
            if isinstance(obj, Series):
                raise ValueError("Can't slice Series on 2 axes: %s" % str(key))
            iindexer, cindexer = key
        else:
            raise ValueError("Expected tuple of length ≤2: %s" % str(key))

        partition_sizes = obj.partition_sizes
        if iindexer != slice(None):
            if not partition_sizes:
                # TODO: implement for lists/arrays of integers
                raise NotImplementedError("%s.iloc only supported for `slice`s (on row axis): %s" % (obj.__class__.__name__, iindexer))

            _len = obj._len

            if not isinstance(iindexer, slice):
                raise ValueError("Unexpected iindexer: %s" % str(iindexer))

            start, stop, step = (iindexer.start or 0), (iindexer.stop if iindexer.stop is not None else obj._len), (iindexer.step or 1)
            if start < 0:
                start += obj._len
            if stop < 0:
                stop += obj._len
            m, M = min(start, stop), max(start, stop)
            m = np.clip(m, 0, _len)
            M = np.clip(M, 0, _len)

            partition_data = [
                dict(partition_idx=partition_idx, start=start, end=end, m=m, M=M)
                for partition_idx, (start, end)
                in enumerate(obj.partition_idx_ranges)
                if start < M and end > m
            ]

            if not partition_data:
                # The passed slice either begins after obj's end, or is empty and falls at a partition boundary (e.g. 0:0, N:N for N == partition_sizes[0], etc.)
                if m > _len:
                    partition_data = [
                         dict(partition_idx=partition_idx, start=start, end=end, m=m, M=M)
                         for partition_idx, (start, end)
                         in list(enumerate(obj.partition_idx_ranges))[-1:]
                    ]
                else:
                    partition_data = [
                        dict(partition_idx=partition_idx, start=start, end=end, m=m, M=M)
                        for partition_idx, (start, end)
                        in enumerate(obj.partition_idx_ranges)
                        if start <= M and end >= m
                    ] \
                    [:1]

            first_partition = partition_data[0]
            first_partition_idx = first_partition['partition_idx']
            first_full_partition_idx = first_partition_idx

            last_partition = partition_data[-1]
            last_partition_idx = last_partition['partition_idx']
            last_full_partition_idx = last_partition_idx

            partition_end_idx = last_partition_idx + 1
            divisions = list(obj.divisions[first_partition_idx:(partition_end_idx + 1)])

            partial_prefix = None
            partial_suffix = None
            partition_sizes_prefix = []
            partition_sizes_suffix = []

            assert first_partition['start'] <= first_partition['m']
            has_partial_prefix = first_partition['start'] < first_partition['m']

            assert last_partition['end'] >= last_partition['M']
            has_partial_suffix = last_partition['end'] > last_partition['M']

            has_clipped_single_partition = has_partial_prefix and has_partial_suffix and first_partition_idx == last_partition_idx

            if has_partial_prefix:
                divisions[0] = None
                first_full_partition_idx += 1
                start = first_partition['m'] - first_partition['start']
                end = min(first_partition['end'], first_partition['M'])
                drop_right = first_partition['end'] - end
                partition_sizes_prefix = [
                    partition_sizes[first_partition_idx] - start - drop_right
                ]
                partial_prefix = \
                    obj \
                        .partitions[first_partition_idx] \
                        .map_partitions(
                            lambda df, start, drop_right: \
                                df.iloc[start:-drop_right] \
                                if drop_right \
                                else df.iloc[start:],
                            start,
                            drop_right
                        )

            if last_partition['end'] != last_partition['M']:
                divisions[-1] = None
                last_full_partition_idx -= 1
                if not has_clipped_single_partition:
                    # Only compute+set a "suffix" if the partial final partition is not the same
                    # as a partial initial partition (handled above)
                    end = last_partition['end'] - last_partition['M']
                    partition_sizes_suffix = [ partition_sizes[last_partition_idx] - end ]
                    partial_suffix = \
                        obj \
                            .partitions[last_partition_idx] \
                            .map_partitions(lambda df, end: df.iloc[:-end], end)

            full_partition_idx_range = slice(first_full_partition_idx, last_full_partition_idx + 1)
            num_full_partitions = max(0, last_full_partition_idx + 1 - first_full_partition_idx)
            new_partition_sizes = partition_sizes_prefix + partition_sizes[full_partition_idx_range] + partition_sizes_suffix

            keys = []
            dependencies = []

            if partial_prefix is not None:
                keys.append((partial_prefix._name, 0))
                dependencies.append(partial_prefix)

            if num_full_partitions:
                keys += [
                    (obj._name, first_full_partition_idx + idx)
                    for idx in range(num_full_partitions)
                ]
                dependencies.append(obj)

            if partial_suffix is not None:
                keys.append((partial_suffix._name, 0))
                dependencies.append(partial_suffix)

            name = 'iloc-%s' % tokenize(obj, key)
            dsk = { (name, idx): key for idx, key in enumerate(keys) }
            meta = obj._meta
            graph = HighLevelGraph.from_collections(name, dsk, dependencies=dependencies)
            row_sliced = new_dd_object(graph, name, meta=meta, divisions=divisions, partition_sizes=new_partition_sizes)
            if cindexer == slice(None):
                return row_sliced
            else:
                return row_sliced.iloc[:, cindexer]
        else:
            if cindexer == slice(None):
                return obj
            from dask.dataframe import DataFrame
            if not isinstance(obj, DataFrame):
                raise NotImplementedError(
                    "'DataFrame.iloc' with unknown partition sizes not supported. "
                    "`partition_sizes` must be computed/set ahead of time, or propagated from an "
                    "upstream DataFrame or Series, in order to call `iloc`."
                )

            if not obj.columns.is_unique:
                # if there are any duplicate column names, do an iloc
                return self._iloc(iindexer, cindexer)
            else:
                # otherwise dispatch to dask.dataframe.core.DataFrame.__getitem__
                col_names = obj.columns[cindexer]
                return obj.__getitem__(col_names)

    def _iloc(self, iindexer, cindexer):
        assert iindexer == slice(None)
        meta = self._make_meta(iindexer, cindexer)

        return self.obj.map_partitions(methods.iloc, cindexer, meta=meta, preserve_partitions=True)


class _LocIndexer(_IndexerBase):
    """ Helper class for the .loc accessor """

    @property
    def _meta_indexer(self):
        return self.obj._meta.loc

    def __getitem__(self, key):

        if isinstance(key, tuple):
            # multi-dimensional selection
            if len(key) > self.obj.ndim:
                # raise from pandas
                msg = "Too many indexers"
                raise pd.core.indexing.IndexingError(msg)

            iindexer = key[0]
            cindexer = key[1]
        else:
            # if self.obj is Series, cindexer is always None
            iindexer = key
            cindexer = None
        return self._loc(iindexer, cindexer)

    def _loc(self, iindexer, cindexer):
        """ Helper function for the .loc accessor """
        if isinstance(iindexer, Series):
            return self._loc_series(iindexer, cindexer)
        elif isinstance(iindexer, Array):
            return self._loc_array(iindexer, cindexer)
        elif callable(iindexer):
            return self._loc(iindexer(self.obj), cindexer)

        if self.obj.known_divisions:
            iindexer = self._maybe_partial_time_string(iindexer)

            if isinstance(iindexer, slice):
                return self._loc_slice(iindexer, cindexer)
            elif isinstance(iindexer, (list, np.ndarray)):
                return self._loc_list(iindexer, cindexer)
            else:
                # element should raise KeyError
                return self._loc_element(iindexer, cindexer)
        else:
            if isinstance(iindexer, (list, np.ndarray)):
                # applying map_pattition to each partitions
                # results in duplicated NaN rows
                msg = "Cannot index with list against unknown division"
                raise KeyError(msg)
            elif not isinstance(iindexer, slice):
                iindexer = slice(iindexer, iindexer)

            meta = self._make_meta(iindexer, cindexer)
            return self.obj.map_partitions(
                methods.try_loc, iindexer, cindexer, meta=meta,  # TODO: partition_sizes
            )

    def _maybe_partial_time_string(self, iindexer):
        """
        Convert index-indexer for partial time string slicing
        if obj.index is DatetimeIndex / PeriodIndex
        """
        idx = meta_nonempty(self.obj._meta.index)
        iindexer = _maybe_partial_time_string(idx, iindexer, kind="loc")
        return iindexer

    def _loc_series(self, iindexer, cindexer):
        meta = self._make_meta(iindexer, cindexer)
        return self.obj.map_partitions(
            methods.loc, iindexer, cindexer, token="loc-series", meta=meta  # TODO: partition_sizes
        )

    def _loc_array(self, iindexer, cindexer):
        iindexer_series = iindexer.to_dask_dataframe("_", self.obj.index)
        return self._loc_series(iindexer_series, cindexer)

    def _loc_list(self, iindexer, cindexer):
        name = "loc-%s" % tokenize(iindexer, self.obj)
        parts = self._get_partitions(iindexer)
        meta = self._make_meta(iindexer, cindexer)

        if len(iindexer):
            dsk = {}
            divisions = []
            items = sorted(parts.items())
            for i, (div, indexer) in enumerate(items):
                dsk[name, i] = (methods.loc, (self._name, div), indexer, cindexer)
                # append minimum value as division
                divisions.append(sorted(indexer)[0])
            # append maximum value of the last division
            divisions.append(sorted(items[-1][1])[-1])
        else:
            divisions = [None, None]
            dsk = {(name, 0): meta.head(0)}
        graph = HighLevelGraph.from_collections(name, dsk, dependencies=[self.obj])
        return new_dd_object(graph, name, meta=meta, divisions=divisions)

    def _loc_element(self, iindexer, cindexer):
        name = "loc-%s" % tokenize(iindexer, self.obj)
        part = self._get_partitions(iindexer)

        if iindexer < self.obj.divisions[0] or iindexer > self.obj.divisions[-1]:
            raise KeyError("the label [%s] is not in the index" % str(iindexer))

        dsk = {
            (name, 0): (
                methods.loc,
                (self._name, part),
                slice(iindexer, iindexer),
                cindexer,
            )
        }

        meta = self._make_meta(iindexer, cindexer)
        graph = HighLevelGraph.from_collections(name, dsk, dependencies=[self.obj])
        # TODO: partition_sizes presumably [1], unless index values can repeat?
        return new_dd_object(graph, name, meta=meta, divisions=[iindexer, iindexer], partition_sizes=None)

    def _get_partitions(self, keys):
        if isinstance(keys, (list, np.ndarray)):
            return _partitions_of_index_values(self.obj.divisions, keys)
        else:
            # element
            return _partition_of_index_value(self.obj.divisions, keys)

    def _coerce_loc_index(self, key):
        return _coerce_loc_index(self.obj.divisions, key)

    def _loc_slice(self, iindexer, cindexer):
        name = "loc-%s" % tokenize(iindexer, cindexer, self)

        assert isinstance(iindexer, slice)
        assert iindexer.step in (None, 1)

        if iindexer.start is not None:
            start = self._get_partitions(iindexer.start)
        else:
            start = 0
        if iindexer.stop is not None:
            stop = self._get_partitions(iindexer.stop)
        else:
            stop = self.obj.npartitions - 1

        if iindexer.start is None and self.obj.known_divisions:
            istart = self.obj.divisions[0]
        else:
            istart = self._coerce_loc_index(iindexer.start)
        if iindexer.stop is None and self.obj.known_divisions:
            istop = self.obj.divisions[-1]
        else:
            istop = self._coerce_loc_index(iindexer.stop)

        if stop == start:
            dsk = {
                (name, 0): (
                    methods.loc,
                    (self._name, start),
                    slice(iindexer.start, iindexer.stop),
                    cindexer,
                )
            }
            divisions = [istart, istop]
        else:
            dsk = {
                (name, 0): (
                    methods.loc,
                    (self._name, start),
                    slice(iindexer.start, None),
                    cindexer,
                )
            }
            for i in range(1, stop - start):
                if cindexer is None:
                    dsk[name, i] = (self._name, start + i)
                else:
                    dsk[name, i] = (
                        methods.loc,
                        (self._name, start + i),
                        slice(None, None),
                        cindexer,
                    )

            dsk[name, stop - start] = (
                methods.loc,
                (self._name, stop),
                slice(None, iindexer.stop),
                cindexer,
            )

            if iindexer.start is None:
                div_start = self.obj.divisions[0]
            else:
                div_start = max(istart, self.obj.divisions[start])

            if iindexer.stop is None:
                div_stop = self.obj.divisions[-1]
            else:
                div_stop = min(istop, self.obj.divisions[stop + 1])

            divisions = (
                (div_start,) + self.obj.divisions[start + 1 : stop + 1] + (div_stop,)
            )

        assert len(divisions) == len(dsk) + 1

        meta = self._make_meta(iindexer, cindexer)
        graph = HighLevelGraph.from_collections(name, dsk, dependencies=[self.obj])
        # TODO: could at least figure out partition_sizes for ":" all-rows iindexer, and non-boundary partition_sizes in
        #  general (leaving Nones in self.partition_sizes on the ends?)
        return new_dd_object(graph, name, meta=meta, divisions=divisions)


def _partition_of_index_value(divisions, val):
    """ In which partition does this value lie?

    >>> _partition_of_index_value([0, 5, 10], 3)
    0
    >>> _partition_of_index_value([0, 5, 10], 8)
    1
    >>> _partition_of_index_value([0, 5, 10], 100)
    1
    >>> _partition_of_index_value([0, 5, 10], 5)  # left-inclusive divisions
    1
    """
    if divisions[0] is None:
        msg = "Can not use loc on DataFrame without known divisions"
        raise ValueError(msg)
    val = _coerce_loc_index(divisions, val)
    i = bisect.bisect_right(divisions, val)
    return min(len(divisions) - 2, max(0, i - 1))


def _partitions_of_index_values(divisions, values):
    """ Return defaultdict of division and values pairs
    Each key corresponds to the division which values are index values belong
    to the division.

    >>> sorted(_partitions_of_index_values([0, 5, 10], [3]).items())
    [(0, [3])]
    >>> sorted(_partitions_of_index_values([0, 5, 10], [3, 8, 5]).items())
    [(0, [3]), (1, [8, 5])]
    """
    if divisions[0] is None:
        msg = "Can not use loc on DataFrame without known divisions"
        raise ValueError(msg)

    results = defaultdict(list)
    values = pd.Index(values, dtype=object)
    for val in values:
        i = bisect.bisect_right(divisions, val)
        div = min(len(divisions) - 2, max(0, i - 1))
        results[div].append(val)
    return results


def _coerce_loc_index(divisions, o):
    """ Transform values to be comparable against divisions

    This is particularly valuable to use with pandas datetimes
    """
    if divisions and isinstance(divisions[0], datetime):
        return pd.Timestamp(o)
    if divisions and isinstance(divisions[0], np.datetime64):
        return np.datetime64(o).astype(divisions[0].dtype)
    return o


def _maybe_partial_time_string(index, indexer, kind):
    """
    Convert indexer for partial string selection
    if data has DatetimeIndex/PeriodIndex
    """
    # do not pass dd.Index
    assert is_index_like(index)

    if not isinstance(index, (pd.DatetimeIndex, pd.PeriodIndex)):
        return indexer

    if isinstance(indexer, slice):
        if isinstance(indexer.start, str):
            start = index._maybe_cast_slice_bound(indexer.start, "left", kind)
        else:
            start = indexer.start

        if isinstance(indexer.stop, str):
            stop = index._maybe_cast_slice_bound(indexer.stop, "right", kind)
        else:
            stop = indexer.stop
        return slice(start, stop)

    elif isinstance(indexer, str):
        start = index._maybe_cast_slice_bound(indexer, "left", "loc")
        stop = index._maybe_cast_slice_bound(indexer, "right", "loc")
        return slice(min(start, stop), max(start, stop))

    return indexer
