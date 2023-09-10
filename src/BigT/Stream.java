package BigT;

import java.io.IOException;

import btree.BTFileScan;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import commandLine.MiniTable;
import diskmgr.OutOfSpaceException;
import global.MID;
import global.RID;
import global.TupleOrder;
import heap.Heapfile;
import heap.MapScan;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.MapSort;
import iterator.RelSpec;

public class Stream {
	private final String rowFilter;
	private final String columnFilter;
	private final String valueFilter;
	private bigT bigtable;
	private boolean scanAll = false;
	private String starFilter;
	private String rangeRegex = "\\[\\S+,\\S+\\]";
	private String lastChar;
	private BTFileScan btreeScanner, dummyScanner;
	public Heapfile tempHeapFile;
	private MID[] midList;
	private int midCounter = 0;
	private MapSort sortObj;
	private boolean versionEnabled = true;
	private MapScan mapScan;
	private int type, orderType;

	public Stream(bigT bigTable, int orderType, String rowFilter, String columnFilter, String valueFilter)
			throws Exception {

		this.bigtable = bigTable;
		this.rowFilter = rowFilter;
		this.columnFilter = columnFilter;
		this.valueFilter = valueFilter;
		this.type = bigTable.indexingType;
		this.orderType = orderType;
		this.starFilter = "*";
		this.lastChar = "Z";

		queryConditions();
		filterAndSortData(this.orderType);

	}

	public void queryConditions() throws Exception {
		StringKey startQuery = null, endQuery = null;
		switch (this.type) {
		case 1:
		default:
			this.scanAll = true;
			break;
		case 2:
			if (rowFilter.equals(starFilter)) {
				this.scanAll = true;
			} else {
				if (rowFilter.matches(rangeRegex)) {
					String[] range = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
					startQuery = new StringKey(range[0]);
					endQuery = new StringKey(range[1] + this.lastChar);
				} else {
					startQuery = new StringKey(rowFilter);
					endQuery = new StringKey(rowFilter + this.lastChar);
				}
			}
			break;
		case 3:
			if (columnFilter.equals("*")) {
				this.scanAll = true;
			} else {
				if (columnFilter.matches(rangeRegex)) {
					String[] range = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
					startQuery = new StringKey(range[0]);
					endQuery = new StringKey(range[1] + this.lastChar);
				} else {
					startQuery = new StringKey(columnFilter);
					endQuery = new StringKey(columnFilter + this.lastChar);
				}
			}
			break;
		case 4:
			if ((rowFilter.equals("*")) && (columnFilter.equals("*"))) {
				scanAll = true;
			} else {
				if ((rowFilter.matches(rangeRegex)) && (columnFilter.matches(rangeRegex))) {
					String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
					String[] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
					startQuery = new StringKey(columnRange[0] + "$" + rowRange[0]);
					endQuery = new StringKey(columnRange[1] + "$" + rowRange[1] + this.lastChar);
				} else if ((rowFilter.matches(rangeRegex)) && (!columnFilter.matches(rangeRegex))) {
					String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
					if (columnFilter.equals(starFilter)) {
						scanAll = true;
					} else {
						startQuery = new StringKey(columnFilter + "$" + rowRange[0]);
						endQuery = new StringKey(columnFilter + "$" + rowRange[1] + this.lastChar);
					}
				} else if ((!rowFilter.matches(rangeRegex)) && (columnFilter.matches(rangeRegex))) {
					String[] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
					if (rowFilter.equals(starFilter)) {
						startQuery = new StringKey(columnRange[0]);
						endQuery = new StringKey(columnRange[1] + this.lastChar);
					} else {
						startQuery = new StringKey(columnRange[0] + "$" + rowFilter);
						endQuery = new StringKey(columnRange[1] + "$" + rowFilter + this.lastChar);
					}
				} else {
					if (columnFilter.equals(starFilter)) {
						scanAll = true;
					} else if (rowFilter.equals(starFilter)) {
						startQuery = endQuery = new StringKey(columnFilter);
					} else {
						startQuery = new StringKey(columnFilter + "$" + rowFilter);
						endQuery = new StringKey(columnFilter + "$" + rowFilter + this.lastChar);
					}
				}
			}
			break;
		case 5:
			if ((valueFilter.equals(starFilter)) && (rowFilter.equals(starFilter))) {
				scanAll = true;
			} else {
				if ((valueFilter.matches(rangeRegex)) && (rowFilter.matches(rangeRegex))) {
					String[] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
					String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
					startQuery = new StringKey(rowRange[0] + "$" + valueRange[0]);
					endQuery = new StringKey(rowRange[1] + "$" + valueRange[1] + this.lastChar);
				} else if ((valueFilter.matches(rangeRegex)) && (!rowFilter.matches(rangeRegex))) {
					String[] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
					if (rowFilter.equals(starFilter)) {
						scanAll = true;
					} else {
						startQuery = new StringKey(rowFilter + "$" + valueRange[0]);
						endQuery = new StringKey(rowFilter + "$" + valueRange[1] + this.lastChar);
					}
				} else if ((!valueFilter.matches(rangeRegex)) && (rowFilter.matches(rangeRegex))) {
					String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
					if (valueFilter.equals("*")) {
						startQuery = new StringKey(rowRange[0]);
						endQuery = new StringKey(rowRange[1] + this.lastChar);
					} else {
						startQuery = new StringKey(rowRange[0] + "$" + valueFilter);
						endQuery = new StringKey(rowRange[1] + "$" + valueFilter + this.lastChar);
					}
				} else {
					if (rowFilter.equals("*")) {
						scanAll = true;
					} else if (valueFilter.equals("*")) {
						startQuery = new StringKey(rowFilter);
						endQuery = new StringKey(rowFilter + lastChar);
					} else {
						startQuery = new StringKey(rowFilter + "$" + valueFilter);
						endQuery = new StringKey(rowFilter + "$" + valueFilter + this.lastChar);
					}
				}
			}
			break;
		}
		if (!this.scanAll) {
			this.btreeScanner = bigtable.indexBTreeFile.new_scan(startQuery, endQuery);
		}
	}

	public void filterAndSortData(int orderType) throws Exception {
	    tempHeapFile = new Heapfile("tempSort4");
	    MID midObj = new MID();
	    if (this.scanAll) {
	        mapScan = bigtable.heapFileMap.openMapScan();
	        Map mapObj = null;
	        int count = 0;
	        mapObj = this.mapScan.getNext(midObj);
	        while (mapObj != null) {
	            count++;
	            short kaka = 0;
	            if (genericMatcher(mapObj, "row", rowFilter) && genericMatcher(mapObj, "column", columnFilter)
	                    && genericMatcher(mapObj, "value", valueFilter)) {
	                tempHeapFile.insertMap(mapObj.getMapByteArray());
	            }
	            mapObj = mapScan.getNext(midObj);
	        }
	    } else {
	        KeyDataEntry entry = btreeScanner.get_next();
	        while (entry != null) {
	            RID rid = ((LeafData) entry.data).getData();
	            if (rid != null) {
	                MID midFromRid = new MID(rid.pageNo, rid.slotNo);
	                Map mapObj = bigtable.heapFileMap.getMap(midFromRid);
	                if (genericMatcher(mapObj, "row", rowFilter) && genericMatcher(mapObj, "column", columnFilter)
	                        && genericMatcher(mapObj, "value", valueFilter)) {
	                    tempHeapFile.insertMap(mapObj.getMapByteArray());
	                }
	            }
	            entry = btreeScanner.get_next();
	        }
	    }
	    FldSpec[] projection = new FldSpec[4];
	    RelSpec rel = new RelSpec(RelSpec.outer);
	    projection[0] = new FldSpec(rel, 1);
	    projection[1] = new FldSpec(rel, 2);
	    projection[2] = new FldSpec(rel, 3);
	    projection[3] = new FldSpec(rel, 4);
	    FileScan fscan = null;
	    try {
	        fscan = new FileScan("tempSort4", MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, (short) 4, 4,
	                projection, null);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    int sortField, num_pages = 10, sortFieldLength;
	    switch (orderType) {
	    case 1:
	    case 3:
	        sortField = 1;
	        sortFieldLength = MiniTable.BIGT_STR_SIZES[0];
	        break;
	    case 2:
	    case 4:
	        sortField = 2;
	        sortFieldLength = MiniTable.BIGT_STR_SIZES[1];
	        break;
	    case 5:
	        sortField = 3;
	        sortFieldLength = MiniTable.BIGT_STR_SIZES[2];
	        break;
	    default:
	        throw new IllegalStateException("Unexpected value: " + orderType);
	    }
	    try {
	        this.sortObj = new MapSort(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES, fscan, sortField,
	                new TupleOrder(TupleOrder.Ascending), num_pages, sortFieldLength);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}

	private boolean genericMatcher(Map map, String field, String genericFilter) throws Exception {
		if (genericFilter.matches(rangeRegex)) {
			String[] range = genericFilter.replaceAll("[\\[ \\]]", "").split(",");
			return map.getGenericValue(field).compareTo(range[0]) >= 0
					&& map.getGenericValue(field).compareTo(range[1]) <= 0;
		} else if (genericFilter.equals(map.getGenericValue(field))) {
			return true;
		} else {
			return genericFilter.equals(starFilter);
		}
	}

	public void closeStream() throws Exception {

		if (this.sortObj != null) {
			this.sortObj.close();
		}
		if (mapScan != null) {
			mapScan.closescan();
		}
		if (btreeScanner != null) {
			btreeScanner.DestroyBTreeFileScan();
		}
	}

	public Map getNext() throws Exception {
		if (this.sortObj == null) {
			System.out.println("sort object is not initialised");
			return null;
		}
		Map m = null;
		try {
			m = this.sortObj.get_next();

		} catch (OutOfSpaceException e) {
			System.out.println("outofspace");
			e.printStackTrace();
			closeStream();
		}
		if (m == null) {
			tempHeapFile.deleteFile();
			closeStream();
			return null;
		}
		return m;
	}

	public boolean setFilter(Map map, String rowFilter, String columnFilter, String valueFilter) throws IOException {
		boolean result = true;
		if (rowFilter.matches(rangeRegex)) {
			String[] rowRange = rowFilter.replaceAll("[\\[ \\]]", "").split(",");
			if (map.getRowLabel().compareTo(rowRange[0]) < 0 || map.getRowLabel().compareTo(rowRange[1]) > 0)
				result = false;
		} else {
			if (!(rowFilter.matches(starFilter))) {
				if (!map.getRowLabel().equals(rowFilter)) {
					result = false;
				}
			}
		}
		if (columnFilter.matches(rangeRegex)) {
			String[] columnRange = columnFilter.replaceAll("[\\[ \\]]", "").split(",");
			if (map.getRowLabel().compareTo(columnRange[0]) < 0 || map.getRowLabel().compareTo(columnRange[1]) > 0)
				result = false;
		} else {
			if (!(columnFilter.matches(starFilter))) {
				if (!map.getColumnLabel().equals(columnFilter)) {
					result = false;
				}
			}
		}
		if (valueFilter.matches(rangeRegex)) {
			String[] valueRange = valueFilter.replaceAll("[\\[ \\]]", "").split(",");
			if (map.getRowLabel().compareTo(valueRange[0]) < 0 || map.getRowLabel().compareTo(valueRange[1]) > 0)
				result = false;
		} else {
			if (!(valueFilter.matches(starFilter))) {
				if (!map.getValue().equals(valueFilter)) {
					result = false;
				}
			}
		}
		return result;
	}
}
