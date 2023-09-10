package BigT;

import static global.GlobalConst.MINIBASE_PAGESIZE;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import btree.BTreeFile;
import btree.DeleteFashion;
import btree.IntegerKey;
import btree.StringKey;
import bufmgr.BufMgrException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import commandLine.MiniTable;
import global.AttrType;
import global.MID;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;
import iterator.MapUtils;

public class bigT {
	Heapfile heapFileMap;// heapfile;//heapFileMap;

	HashMap<String, ArrayList<MID>> mapVersions;// mapVersion

	BTreeFile indexBTreeFile;// indexFile

	public static final int MAX_SIZE = MINIBASE_PAGESIZE;

	int indexingType; // type

	String fileName;// name

	BTreeFile timestampIndexBTreeFile;// timestampIndexFile;

	public bigT(String fileName) {
		this.fileName = fileName;
		try {
			PageId heapFileId = SystemDefs.JavabaseDB.get_file_entry(fileName + ".meta");
			if (heapFileId == null) {
				throw new Exception("BigT File with name: " + fileName + " doesn't exist");
			}
			Heapfile metadataFile = new Heapfile(fileName + ".meta");
			Scan metascan = metadataFile.openScan();
			Tuple metadata = metascan.getNext(new RID());
			metadata.setHdr((short) 1, new AttrType[] { new AttrType(AttrType.attrInteger) }, null);
			metascan.closescan();
			this.indexingType = metadata.getIntFld(1);
			setIndexFiles();
			this.heapFileMap = new Heapfile(fileName + ".heap");
			try (ObjectInputStream objectInputStream = new ObjectInputStream(
					new FileInputStream("/tmp/" + this.fileName + ".hashmap.ser"))) {
				this.indexingType = objectInputStream.readByte();
				this.mapVersions = (HashMap<String, ArrayList<MID>>) objectInputStream.readObject();
			} catch (IOException e) {
				throw new IOException("File not writable: " + e.toString());
			}

		} catch (Exception e) {
			System.out.println("Exception occured in bigT() " + e.getMessage());
		}
	}

	public bigT(String name, int indexingType) throws Exception {
		try {
			this.indexingType = indexingType;
			this.fileName = name;
			Heapfile metadataFile = new Heapfile(name + ".meta");
			Tuple metadata = new Tuple();
			metadata.setHdr((short) 1, new AttrType[] { new AttrType(AttrType.attrInteger) }, null);
			metadata.setIntFld(1, this.indexingType);
			metadataFile.insertRecord(metadata.getTupleByteArray());
			this.heapFileMap = new Heapfile(name + ".heap");
			this.mapVersions = new HashMap<>();
			createIndex();
		} catch (Exception e) {
			System.out.println("Exception occured in bigT() " + e.getMessage());
		}
	}

	private void setIndexFiles() throws Exception {
		switch (this.indexingType) {
		case 1:
			this.indexBTreeFile = null;
			break;
		case 2:
			this.indexBTreeFile = new BTreeFile(this.fileName + "_row.idx");
			break;
		case 3:
			this.indexBTreeFile = new BTreeFile(this.fileName + "_col.idx");
			break;
		case 4:
			this.indexBTreeFile = new BTreeFile(this.fileName + "_col_row.idx");
			this.timestampIndexBTreeFile = new BTreeFile(this.fileName + "_timestamp.idx");
			break;
		case 5:
			this.indexBTreeFile = new BTreeFile(this.fileName + "row_val.idx");
			this.timestampIndexBTreeFile = new BTreeFile(this.fileName + "_timestamp.idx");
			break;
		default:
			throw new Exception("Invalid Index Type in setIndexFiles()");
		}
	}

	private void createIndex() throws Exception {
		switch (this.indexingType) {
		case 1:
			this.indexBTreeFile = null;
			break;
		case 2:
			this.indexBTreeFile = new BTreeFile(this.fileName + "_row.idx", AttrType.attrString,
					MiniTable.BIGT_STR_SIZES[0], DeleteFashion.NAIVE_DELETE);
			break;
		case 3:
			this.indexBTreeFile = new BTreeFile(this.fileName + "_col.idx", AttrType.attrString,
					MiniTable.BIGT_STR_SIZES[1], DeleteFashion.NAIVE_DELETE);
			break;
		case 4:
			this.indexBTreeFile = new BTreeFile(this.fileName + "_col_row.idx", AttrType.attrString,
					MiniTable.BIGT_STR_SIZES[0] + MiniTable.BIGT_STR_SIZES[1] + "$".getBytes().length,
					DeleteFashion.NAIVE_DELETE);
			this.timestampIndexBTreeFile = new BTreeFile(this.fileName + "_timestamp.idx", AttrType.attrInteger, 4,
					DeleteFashion.NAIVE_DELETE);
			break;
		case 5:
			this.indexBTreeFile = new BTreeFile(this.fileName + "row_val.idx", AttrType.attrString,
					MiniTable.BIGT_STR_SIZES[0] + MiniTable.BIGT_STR_SIZES[2] + "$".getBytes().length,
					DeleteFashion.NAIVE_DELETE);
			this.timestampIndexBTreeFile = new BTreeFile(this.fileName + "_timestamp.idx", AttrType.attrInteger, 4,
					DeleteFashion.NAIVE_DELETE);
			break;
		default:
			throw new Exception("Invalid Index Type in setIndexFiles()");
		}
	}

	public void deleteBigt(String name, int type) {
		try {
			heapFileMap.deleteFile();
			indexBTreeFile.destroyFile();
		}

		catch (Exception e) {
			System.out.println("Exception in deleteBigt()");
		}
	}

	public int getMapCnt() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException,
			InvalidTupleSizeException {
		return this.heapFileMap.getRecCnt();
	}

	public int getRowCnt() {
		Set<String> distinctRow = new HashSet<>();
		mapVersions.keySet().forEach(key -> distinctRow.add(key.split("\\$")[0]));
		return distinctRow.size();
	}

	public int getColumnCnt() {
		Set<String> distinctCol = new HashSet<>();
		mapVersions.keySet().forEach(key -> distinctCol.add(key.split("\\$")[1]));
		return distinctCol.size();
	}

	public MID insertMap(byte[] mapPtr) throws Exception {
		Map map = new Map();
		map.setData(mapPtr);

		String key;
		String mapVersionKey = map.getRowLabel() + "$" + map.getColumnLabel();
		ArrayList<MID> list = mapVersions.get(mapVersionKey);
		if (list == null) {
			list = new ArrayList<>();
		} else {
			int oldestTimestamp = Integer.MAX_VALUE;
			MID oldestMID = null;
			Map oldestMap = new Map();
			if (list.size() > 3) {
				throw new IOException("Metadata file is corrupted, please delete it");
			}
			if (list.size() == 3) {
				for (MID mid1 : list) {
					Map map1 = heapFileMap.getMap(mid1);
					if (MapUtils.Equal(map1, map)) {
						return mid1;
					} else {
						if (map1.getTimeStamp() < oldestTimestamp) {
							oldestTimestamp = map1.getTimeStamp();
							oldestMID = mid1;
							oldestMap = map1;
						}
					}
				}
			}
			if (list.size() == 3 && map.getTimeStamp() < oldestTimestamp) {
				return oldestMID;
			}

			if (list.size() == 3) {
//                Map oldestMap = heapfile.getMap(oldestMID);
				switch (this.indexingType) {
				case 1:
					key = null;
					break;
				case 2:
					key = oldestMap.getRowLabel();
					break;
				case 3:
					key = oldestMap.getColumnLabel();
					break;
				case 4:
					key = oldestMap.getColumnLabel() + "$" + oldestMap.getRowLabel();
					this.timestampIndexBTreeFile.Delete(new IntegerKey(oldestMap.getTimeStamp()),
							MapUtils.ridFromMid(oldestMID));
					break;
				case 5:
					key = oldestMap.getRowLabel() + "$" + oldestMap.getValue();
					this.timestampIndexBTreeFile.Delete(new IntegerKey(oldestMap.getTimeStamp()),
							MapUtils.ridFromMid(oldestMID));
					break;
				default:
					throw new Exception("Invalid Index Type");
				}
				if (key != null) {
					this.indexBTreeFile.Delete(new StringKey(key), MapUtils.ridFromMid(oldestMID));
				}
				heapFileMap.deleteMap(oldestMID);
				list.remove(oldestMID);

			}
		}
		MID mid = this.heapFileMap.insertMap(mapPtr);
		RID rid = MapUtils.ridFromMid(mid);
		list.add(mid);
		mapVersions.put(mapVersionKey, list);

		switch (this.indexingType) {
		case 1:
			key = null;
			break;
		case 2:
			key = map.getRowLabel();
			break;
		case 3:
			key = map.getColumnLabel();
			break;
		case 4:
			key = map.getColumnLabel() + "$" + map.getRowLabel();
			this.timestampIndexBTreeFile.insert(new IntegerKey(map.getTimeStamp()), rid);
			break;
		case 5:
			key = map.getRowLabel() + "$" + map.getValue();
			this.timestampIndexBTreeFile.insert(new IntegerKey(map.getTimeStamp()), rid);
			break;
		default:
			throw new Exception("Invalid Index Type");
		}
		if (key != null) {
			this.indexBTreeFile.insert(new StringKey(key), rid);
		}
		return mid;
	}

	public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter)
			throws Exception {
		return new Stream(this, orderType, rowFilter, columnFilter, valueFilter);
	}

	public void close()
			throws PageUnpinnedException, PagePinnedException, PageNotFoundException, HashOperationException,
			BufMgrException, IOException, HashEntryNotFoundException, InvalidFrameNumberException, ReplacerException {
		if (this.indexBTreeFile != null)
			this.indexBTreeFile.close();
		if (this.timestampIndexBTreeFile != null)
			this.timestampIndexBTreeFile.close();

		try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(
				new FileOutputStream("/tmp/" + this.fileName + ".hashmap.ser"))) {
			objectOutputStream.writeByte(indexingType);
			objectOutputStream.writeObject(mapVersions);
		} catch (IOException e) {
			throw new IOException("File not writable: " + e.toString());
		}
	}

	int getTimeStampCnt() {
		Set<String> distinctTS = new HashSet<>();
		mapVersions.keySet().forEach(key -> distinctTS.add(key.split("\\$")[3]));
		return distinctTS.size();
	}

	public int getType() {
		return indexingType;
	}
	
	public void setType(int type) {
		this.indexingType = type;
	}
}
