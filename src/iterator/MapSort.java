package iterator;

import java.io.IOException;

import BigT.Map;
import commandLine.MiniTable;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;

public class MapSort extends MapIterator implements GlobalConst {
	private static final int ARBIT_RUNS = 10;
	private static short REC_LEN1 = 32;
	private int[] n_Maps;
	private int n_runs;
	private short mapSize;
	private short[] str_fld_lens = null;
	private int num_cols = 4;
	private MapIterator mapIterObj;
	private int _sort_fld;
	private TupleOrder sortOrder;
	private int _num_pages;
	private byte[][] bufs;
	private boolean first_time;
	private Heapfile[] temp_files;
	private int n_tempfiles;
	private OBuf o_buf;
	private int max_elems_in_heap;
	private int sortFldLen;
	private pnodeSplayPQ queue;
	private Map op_map_buf, output_map;
	AttrType[] mapAttributes = new AttrType[4];
	private SpoofIbuf[] i_buf;
	private PageId[] bufs_pids;

	public MapSort(AttrType[] attrTypes, short[] field_sizes, MapIterator am, int sort_fld, TupleOrder sort_order,
			int n_pages, int sortFieldLength) throws SortException {
		int str_att_count = 0;
		for (int i = 0; i < num_cols; i++) {
			mapAttributes[i] = new AttrType(attrTypes[i].attrType);
			if (attrTypes[i].attrType == AttrType.attrString) {
				str_att_count++;
			}
		}
		str_fld_lens = new short[str_att_count];
		str_att_count = 0;
		for (int i = 0; i < num_cols; i++) {
			if (mapAttributes[i].attrType == AttrType.attrString) {
				str_fld_lens[str_att_count] = field_sizes[str_att_count];
				str_att_count++;
			}
		}
		Map tempMap = new Map();
		try {
			tempMap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
		} catch (Exception e) {
			throw new SortException(e, "Sort.java: t.setHdr() failed");
		}
		mapSize = tempMap.size();
		mapIterObj = am;
		_sort_fld = sort_fld;
		sortOrder = sort_order;
		_num_pages = n_pages;
		bufs_pids = new PageId[_num_pages];
		bufs = new byte[_num_pages][];
		try {
			get_buffer_pages(_num_pages, bufs_pids, bufs);
		} catch (IteratorBMException e) {
			e.printStackTrace();
		}
		first_time = true;
		temp_files = new Heapfile[ARBIT_RUNS];
		n_tempfiles = ARBIT_RUNS;
		n_Maps = new int[ARBIT_RUNS];
		n_runs = ARBIT_RUNS;
		try {
			temp_files[0] = new Heapfile(null);
		} catch (Exception e) {
			throw new SortException(e, "Sort.java: Heapfile error");
		}
		o_buf = new OBuf();
		o_buf.init(bufs, _num_pages, mapSize, temp_files[0], false);
		max_elems_in_heap = 5000;
		sortFldLen = sortFieldLength;
		queue = new pnodeSplayPQ(sort_fld, attrTypes[sort_fld - 1], sortOrder);
		try {
			op_map_buf = new Map(tempMap);
			op_map_buf.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
		} catch (Exception e) {
			throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
		}
	}

	@Override
	public void close() throws SortException {
		try {
			mapIterObj.close();
		} catch (Exception e) {
			throw new SortException(e, "MapSort.java: error in closing iterator.");
		}
		try {
			free_buffer_pages(_num_pages, bufs_pids);
		} catch (IteratorBMException e) {
			e.printStackTrace();
		}
		for (SpoofIbuf spoofIbuf : i_buf) {
			spoofIbuf.close();
		}
		for (int i = 0; i < temp_files.length; i++) {
			if (temp_files[i] != null) {
				try {
					temp_files[i].deleteFile();
				} catch (Exception e) {
					e.printStackTrace();
					throw new SortException(e, "MapSort.java: Heapfile error");
				}
				temp_files[i] = null;
			}
		}
	}

	private Map delete_min() throws Exception {
		pnode cur_node;
		Map newMap, oldMap;
		cur_node = queue.deq();
		oldMap = cur_node.map;
		if (!i_buf[cur_node.run_num].empty()) {
			try {
				newMap = new Map(mapSize);
				newMap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
			} catch (Exception e) {
				throw new SortException(e, "Sort.java: setHdr() failed");
			}
			newMap = i_buf[cur_node.run_num].Get(newMap);
			if (newMap != null) {
				cur_node.map = newMap;
				try {
					queue.enq(cur_node);
				} catch (UnknowAttrType e) {
					throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
				} catch (TupleUtilsException e) {
					throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
				}
			} else {
				throw new SortException("********** Wait a minute, I thought input is not empty ***************");
			}
		}
		return oldMap;
	}

	private int generate_runs(int max_elems, AttrType sortFldType, int sortFldLen) throws Exception {
		Map map;
		pnode cur_node;
		pnodeSplayPQ Q1 = new pnodeSplayPQ(_sort_fld, sortFldType, sortOrder);
		pnodeSplayPQ Q2 = new pnodeSplayPQ(_sort_fld, sortFldType, sortOrder);
		pnodeSplayPQ pcurr_Q = Q1;
		pnodeSplayPQ pother_Q = Q2;
		Map lastElem = new Map(mapSize);
		try {
			lastElem.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
		} catch (Exception e) {
			throw new SortException(e, "Sort.java: setHdr() failed");
		}
		int run_num = 0;
		int p_elems_curr_Q = 0;
		int p_elems_other_Q = 0;
		int comp_res;
		if (sortOrder.tupleOrder == TupleOrder.Ascending) {
			try {
				MIN_VAL(lastElem, sortFldType);
			} catch (UnknowAttrType e) {
				throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
			} catch (Exception e) {
				throw new SortException(e, "MIN_VAL failed");
			}
		} else {
			try {
				MAX_VAL(lastElem, sortFldType);
			} catch (UnknowAttrType e) {
				throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
			} catch (Exception e) {
				throw new SortException(e, "MIN_VAL failed");
			}
		}
		while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
			try {
				map = mapIterObj.get_next();
			} catch (Exception e) {
				e.printStackTrace();
				throw new SortException(e, "Map Sort.java: get_next() failed");
			}
			if (map == null) {
				break;
			}
			cur_node = new pnode();
			cur_node.map = new Map(map);
			pcurr_Q.enq(cur_node);
			p_elems_curr_Q++;
		}
		while (true) {
			cur_node = pcurr_Q.deq();
			if (cur_node == null)
				break;
			p_elems_curr_Q--;
			comp_res = MapUtils.CompareMapsOnOrderType(cur_node.map, lastElem);
			if ((comp_res < 0 && sortOrder.tupleOrder == TupleOrder.Ascending)
					|| (comp_res > 0 && sortOrder.tupleOrder == TupleOrder.Descending)) {
				try {
					pother_Q.enq(cur_node);
				} catch (UnknowAttrType e) {
					throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
				}
				p_elems_other_Q++;
			} else {
				MapUtils.SetValue(lastElem, cur_node.map, _sort_fld, sortFldType);
				o_buf.Put(cur_node.map);
			}
			if (p_elems_other_Q == max_elems) {
				n_Maps[run_num] = (int) o_buf.flush();
				run_num++;
				if (run_num == n_tempfiles) {
					Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
					for (int i = 0; i < n_tempfiles; i++) {
						temp1[i] = temp_files[i];
					}
					temp_files = temp1;
					n_tempfiles *= 2;
					int[] temp2 = new int[2 * n_runs];
					for (int i = 0; i < n_runs; i++) {
						temp2[i] = n_Maps[i];
					}
					n_Maps = temp2;
					n_runs *= 2;
				}
				try {
					temp_files[run_num] = new Heapfile(null);
				} catch (Exception e) {
					throw new SortException(e, "MapSort.java: create Heapfile failed");
				}
				o_buf.init(bufs, _num_pages, mapSize, temp_files[run_num], false);
				if (sortOrder.tupleOrder == TupleOrder.Ascending) {
					try {
						MIN_VAL(lastElem, sortFldType);
					} catch (UnknowAttrType e) {
						throw new SortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
					} catch (Exception e) {
						throw new SortException(e, "MIN_VAL failed");
					}
				} else {
					try {
						MAX_VAL(lastElem, sortFldType);
					} catch (UnknowAttrType e) {
						throw new SortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
					} catch (Exception e) {
						throw new SortException(e, "MIN_VAL failed");
					}
				}
				pnodeSplayPQ tempQ = pcurr_Q;
				pcurr_Q = pother_Q;
				pother_Q = tempQ;
				int tempelems = p_elems_curr_Q;
				p_elems_curr_Q = p_elems_other_Q;
				p_elems_other_Q = tempelems;
			} else if (p_elems_curr_Q == 0) {
				while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
					try {
						map = mapIterObj.get_next();
					} catch (Exception e) {
						throw new SortException(e, "get_next() failed");
					}
					if (map == null) {
						break;
					}
					cur_node = new pnode();
					cur_node.map = new Map(map);
					try {
						pcurr_Q.enq(cur_node);
					} catch (UnknowAttrType e) {
						throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
					}
					p_elems_curr_Q++;
				}
			}
			if (p_elems_curr_Q == 0) {
				if (p_elems_other_Q == 0) {
					break;
				} else {
					n_Maps[run_num] = (int) o_buf.flush();
					run_num++;
					if (run_num == n_tempfiles) {
						Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
						for (int i = 0; i < n_tempfiles; i++) {
							temp1[i] = temp_files[i];
						}
						temp_files = temp1;
						n_tempfiles *= 2;
						int[] temp2 = new int[2 * n_runs];
						for (int i = 0; i < n_runs; i++) {
							temp2[i] = n_Maps[i];
						}
						n_Maps = temp2;
						n_runs *= 2;
					}
					try {
						temp_files[run_num] = new Heapfile(null);
					} catch (Exception e) {
						e.printStackTrace();
						throw new SortException(e, "MapSort.java: create Heapfile failed");
					}
					o_buf.init(bufs, _num_pages, mapSize, temp_files[run_num], false);
					if (sortOrder.tupleOrder == TupleOrder.Ascending) {
						try {
							MIN_VAL(lastElem, sortFldType);
						} catch (UnknowAttrType e) {
							e.printStackTrace();
							throw new SortException(e, "MapSort.java: UnknowAttrType caught from MIN_VAL()");
						} catch (Exception e) {
							throw new SortException(e, "MIN_VAL failed");
						}
					} else {
						try {
							MAX_VAL(lastElem, sortFldType);
						} catch (UnknowAttrType e) {
							e.printStackTrace();
							throw new SortException(e, "MapSort.java: UnknowAttrType caught from MAX_VAL()");
						} catch (Exception e) {
							throw new SortException(e, "MIN_VAL failed");
						}
					}
					pnodeSplayPQ tempQ = pcurr_Q;
					pcurr_Q = pother_Q;
					pother_Q = tempQ;
					int tempelems = p_elems_curr_Q;
					p_elems_curr_Q = p_elems_other_Q;
					p_elems_other_Q = tempelems;
				}
			}
		}
		n_Maps[run_num] = (int) o_buf.flush();
		run_num++;
		return run_num;
	}

	@Override
	public Map get_next() throws Exception {
		if (this.first_time) {
			this.first_time = false;
			int nruns = generate_runs(max_elems_in_heap, mapAttributes[_sort_fld - 1], sortFldLen);
			setup_for_merge(mapSize, nruns);
		}
		if (queue.empty()) {
			return null;
		}
		output_map = delete_min();
		if (output_map != null) {
			op_map_buf.copyMap(output_map);
			return op_map_buf;
		} else
			return null;
	}

	/**
	 * Set lastElem to be the maximum value of the appropriate type
	 *
	 * @param lastElem    the tuple
	 * @param sortFldType the sort field type
	 * @throws IOException    from lower layers
	 * @throws UnknowAttrType attrSymbol or attrNull encountered
	 */
	private void MAX_VAL(Map lastElem, AttrType sortFldType)
			throws IOException, FieldNumberOutOfBoundException, UnknowAttrType {
		char[] c = new char[1];
		c[0] = Character.MAX_VALUE;
		String s = new String(c);
		switch (sortFldType.attrType) {
		case AttrType.attrInteger:
			lastElem.setTimeStamp(Integer.MIN_VALUE);
			break;
		case AttrType.attrString:
			if (_sort_fld == 1)
				lastElem.setRowLabel(s);
			else if (_sort_fld == 2)
				lastElem.setColumnLabel(s);
			else if (_sort_fld == 4)
				lastElem.setValue(s);
			break;
		default:
			throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
		}
		return;
	}

	/**
	 * Set lastElem to be the minimum value of the appropriate type
	 *
	 * @param lastElem    the tuple
	 * @param sortFldType the sort field type
	 * @throws IOException    from lower layers
	 * @throws UnknowAttrType attrSymbol or attrNull encountered
	 */
	private void MIN_VAL(Map lastElem, AttrType sortFldType)
			throws IOException, FieldNumberOutOfBoundException, UnknowAttrType {
		char[] c = new char[1];
		c[0] = Character.MIN_VALUE;
		String s = new String(c);
		switch (sortFldType.attrType) {
		case AttrType.attrInteger:
			lastElem.setTimeStamp(Integer.MIN_VALUE);
			break;
		case AttrType.attrString:
			if (_sort_fld == 1)
				lastElem.setRowLabel(s);
			else if (_sort_fld == 2)
				lastElem.setColumnLabel(s);
			else if (_sort_fld == 4)
				lastElem.setValue(s);
			break;
		default:
			throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
		}
		return;
	}

	private void setup_for_merge(int mapSize, int n_R_runs)
			throws IOException, LowMemException, SortException, Exception {
		if (n_R_runs > _num_pages)
			throw new LowMemException("Sort.java: Not enough memory to sort in two passes.");
		int i;
		pnode cur_node;
		i_buf = new SpoofIbuf[n_R_runs];
		for (int j = 0; j < n_R_runs; j++)
			i_buf[j] = new SpoofIbuf();
		for (i = 0; i < n_R_runs; i++) {
			byte[][] apage = new byte[1][];
			apage[0] = bufs[i];
			i_buf[i].init(temp_files[i], apage, 1, mapSize, n_Maps[i]);
			cur_node = new pnode();
			cur_node.run_num = i;
			Map tempMap = new Map(mapSize);
			try {
				tempMap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
			} catch (Exception e) {
				throw new SortException(e, "Sort.java: Tuple.setHdr() failed");
			}
			tempMap = i_buf[i].Get(tempMap);
			if (tempMap != null) {
				cur_node.map = tempMap;
				try {
					queue.enq(cur_node);
				} catch (UnknowAttrType e) {
					throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
				} catch (TupleUtilsException e) {
					throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
				}
			}
		}
	}
}