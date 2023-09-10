package iterator;

import java.io.IOException;

import BigT.Map;
import commandLine.MiniTable;
import global.AttrType;
import global.MID;
import global.RID;

public class MapUtils {

	public static RID ridFromMid(MID mid) {
		return new RID(mid.getPageNo(), mid.getSlotNo());
	}

	public static int CompareMapsOnOrderType(Map map1, Map map2) throws IOException {
		int rowCompare = map1.getRowLabel().compareTo(map2.getRowLabel());
		int colCompare = map1.getColumnLabel().compareTo(map2.getColumnLabel());
		boolean tsCompare = (map1.getTimeStamp() >= map2.getTimeStamp());

		if (MiniTable.orderType == 2) {
			if (colCompare > 0)
				return 1;
			else if (colCompare < 0)
				return -1;
			else if (rowCompare > 0)
				return 1;
			else if (rowCompare < 0)
				return -1;
			else if (tsCompare)
				return 1;
			else
				return -1;
		} else if (MiniTable.orderType == 3) {
			if (rowCompare > 0)
				return 1;
			else if (rowCompare < 0)
				return -1;
			else {
				if (tsCompare)
					return 1;
				else
					return -1;
			}
		} else if (MiniTable.orderType == 4) {
			if (colCompare > 0)
				return 1;
			else if (colCompare < 0)
				return -1;
			else {
				if (tsCompare)
					return 1;
				else
					return -1;
			}
		} else if (MiniTable.orderType == 5) {
			if (tsCompare)
				return 1;
			else
				return -1;
		}
		if (rowCompare > 0)
			return 1;
		else if (rowCompare < 0)
			return -1;
		else if (colCompare > 0)
			return 1;
		else if (colCompare < 0)
			return -1;
		else {
			if (tsCompare)
				return 1;
			else
				return -1;
		}
	}

	public static int compareMapWithValue(Map map, int fieldNo, String value)
			throws IOException, UnknowAttrType, TupleUtilsException, InvalidFieldNo {
		Map map2 = new Map();
		switch (fieldNo) {
		case 1:
			map2.setRowLabel(value);
			break;
		case 2:
			map2.setColumnLabel(value);
			break;
		case 3:
			map2.setValue(value);
			break;
		default:
			throw new InvalidFieldNo("Field Number should be in the range (1,3)");
		}
		return CompareMapWithMap(map, map2, fieldNo);
	}

	public static boolean Equal(Map map1, Map map2) throws IOException, InvalidFieldNo {
		for (int i = 0; i <= 3; i++) {
			if (CompareMapWithMap(map1, map2, i) != 0)
				return false;
		}
		return true;
	}

	public static int CompareMapWithMap(Map map1, Map map2, int fieldNo) throws IOException, InvalidFieldNo {
		int map1Int, map2Int;
		String map1Str, map2Str;

		switch (fieldNo) {
		case 0:
			map1Str = map1.getRowLabel();
			map2Str = map2.getRowLabel();
			return Integer.compare(map1Str.compareTo(map2Str), 0);
		case 1:
			if (map1.getColumnLabel() == null) {
				System.out.println("map1.getFieldOffset() = " + map1.getFieldOffset());
				System.out.println("dkajsdalsdjalsdjalsdj = ");
				System.out.println("map1 = " + map1);
			}
			map1Str = map1.getColumnLabel();
			map2Str = map2.getColumnLabel();
			return Integer.compare(map1Str.compareTo(map2Str), 0);
		case 2:
			map1Int = map1.getTimeStamp();
			map2Int = map2.getTimeStamp();
			return Integer.compare(map1Int, map2Int);
		case 3:
			map1Str = map1.getValue();
			map2Str = map2.getValue();
			return Integer.compare(map1Str.compareTo(map2Str), 0);
		default:
			throw new InvalidFieldNo("Field Number should be in the range (0,3)");
		}
	}

	public static MID midFromRid(RID rid) {
		MID mid = new MID();
		mid.setPageNo(rid.pageNo);
		mid.setSlotNo(rid.slotNo);
		return mid;
	}

	public static short[] setup_op_tuple(Map jMap, AttrType[] resultAttrs, AttrType[] inputAttrs, int inputLength,
			short[] inputStrSizes, FldSpec[] projectList, int outputFields)
			throws IOException, TupleUtilsException, InvalidRelation {
		short[] sizes = new short[inputLength];
		int i, count = 0;

		for (i = 0; i < inputLength; i++)
			if (inputAttrs[i].attrType == AttrType.attrString)
				sizes[i] = inputStrSizes[count++];

		int nStrs = 0;
		for (i = 0; i < outputFields; i++) {
			if (projectList[i].relation.key == RelSpec.outer)
				resultAttrs[i] = new AttrType(inputAttrs[projectList[i].offset - 1].attrType);
			else
				throw new InvalidRelation("Invalid relation -innerRel");
		}

		// Now construct the resStrSizes array.
		for (i = 0; i < outputFields; i++) {
			if (projectList[i].relation.key == RelSpec.outer
					&& inputAttrs[projectList[i].offset - 1].attrType == AttrType.attrString)
				nStrs++;
		}

		short[] resStrSizes = new short[nStrs];
		count = 0;
		for (i = 0; i < outputFields; i++) {
			if (projectList[i].relation.key == RelSpec.outer
					&& inputAttrs[projectList[i].offset - 1].attrType == AttrType.attrString)
				resStrSizes[count++] = sizes[projectList[i].offset - 1];
		}

		try {
			jMap.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
		} catch (Exception e) {
			throw new TupleUtilsException(e, "setHdr() failed");
		}
		return resStrSizes;
	}

	public static void SetValue(Map map1, Map map2, int mapFieldNo, AttrType fieldType)
			throws IOException, UnknowAttrType, TupleUtilsException {
		switch (mapFieldNo) {
		case 1:
			map1.setRowLabel(map2.getRowLabel());
			break;
		case 2:
			map1.setColumnLabel(map2.getColumnLabel());
		case 3:
			map1.setTimeStamp(map2.getTimeStamp());
		case 4:
			map1.setValue(map2.getValue());
		}
	}
}
