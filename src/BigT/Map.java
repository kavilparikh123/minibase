package BigT;

import java.io.IOException;

import commandLine.StringAlignUtils;
import global.AttrType;
import global.Convert;
import global.GlobalConst;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidMapSizeException;
import heap.InvalidTypeException;

public class Map implements GlobalConst {

	private static final int MAX_PAGE_SIZE = MINIBASE_PAGESIZE;
	private static final short NUM_OF_FIELDS = 4;
	private static final int TIMESTAMP = 3;
	private static final short VAL = 4;
	private byte[] data;
	private int offset;
	private int maxLength;
	private static final short ROW = 1;
	private static final short COL = 2;
	private short[] fieldOffset;
	private short fieldCount;

	public Map() {
		this.data = new byte[MAX_PAGE_SIZE];
		this.offset = 0;
		this.maxLength = MAX_PAGE_SIZE;
	}

	public Map(byte[] amap, int offset) throws IOException {
		this.data = amap;
		this.offset = offset;
		setOffsetWithData();
		setFieldCount(Convert.getShortValue(offset, this.data));
	}

	public Map(Map fromMap) {
		this.data = fromMap.getMapByteArray();
		this.maxLength = fromMap.getMapLength();
		this.offset = 0;
		this.fieldCount = fromMap.getFieldCount();
		this.fieldOffset = fromMap.duplicateOffsetField();

	}

	public Map(byte[] amap, int offset, int mapLength) throws IOException {
		this.data = amap;
		this.offset = offset;
		this.maxLength = mapLength;
		setFieldOffsetFromData();
		setFieldCount(Convert.getShortValue(offset, this.data));
	}

	public String getRowLabel() throws IOException {
		return Convert.getStrValue(this.fieldOffset[ROW - 1], this.data,
				this.fieldOffset[ROW] - this.fieldOffset[ROW - 1]);
	}

	public String getColumnLabel() throws IOException {
		return Convert.getStrValue(this.fieldOffset[COL - 1], this.data,
				this.fieldOffset[COL] - this.fieldOffset[COL - 1]);
	}

	public int getTimeStamp() throws IOException {
		return Convert.getIntValue(this.fieldOffset[TIMESTAMP - 1], this.data);
	}

	public String getValue() throws IOException {
		return Convert.getStrValue(this.fieldOffset[VAL - 1], this.data,
				this.fieldOffset[VAL] - this.fieldOffset[VAL - 1]);
	}

	private void setOffsetWithData() throws IOException {
		int pos = this.offset + 2;
		this.fieldOffset = new short[NUM_OF_FIELDS + 1];

		for (int i = 0; i <= NUM_OF_FIELDS; i++) {
			this.fieldOffset[i] = Convert.getShortValue(pos, this.data);
			pos += 2;
		}
	}

	public void setRowLabel(String rowLabel) throws IOException {
		Convert.setStrValue(rowLabel, this.fieldOffset[ROW - 1], this.data);
	}

	public void setColumnLabel(String val) throws IOException {
		Convert.setStrValue(val, this.fieldOffset[COL - 1], this.data);
	}

	public void setTimeStamp(int val) throws IOException {
		Convert.setIntValue(val, this.fieldOffset[TIMESTAMP - 1], this.data);
	}

	public void setValue(String val) throws IOException {
		Convert.setStrValue(val, this.fieldOffset[VAL - 1], this.data);
	}

	public byte[] getMapByteArray() {
		byte[] copyOfMap = new byte[this.maxLength];
		System.arraycopy(this.data, this.offset, copyOfMap, 0, this.maxLength);
		return copyOfMap;
	}

	public void print(StringAlignUtils util) throws IOException {
		String rowLab = getRowLabel();
		String columnLab = getColumnLabel();
		int timestamp = getTimeStamp();
		String value = getValue();
		System.out.printf("[%20s|%20s|%20d|%20s]\n", rowLab,columnLab ,timestamp,value);
	}

	public short size() {
		return ((short) (this.fieldOffset[fieldCount] - this.offset));
	}

	public void mapCopy(Map fromMap) {
		byte[] mapArray = fromMap.getMapByteArray();
		System.arraycopy(mapArray, 0, data, offset, maxLength);
	}

	// this is used when you don't want to use the constructor
	public void mapInit(byte[] amap, int offset) {
		this.data = amap;
		this.offset = offset;
	}

	// set a map with the given byte array and offset
	public void mapSet(byte[] fromMap, int offset) {
		System.arraycopy(fromMap, offset, this.data, 0, this.maxLength);
		this.offset = 0;
	}

	public short getFieldCount() {
		return fieldCount;
	}

	public void setFieldCount(short fieldCount) {
		this.fieldCount = fieldCount;
	}

	public int getMapLength() {
		return maxLength;
	}

	public short[] getFieldOffset() {
		return fieldOffset;
	}

	public void setData(byte[] data) throws IOException {
		this.data = data;
		setOffsetWithData();
		setFieldCount(Convert.getShortValue(0, data));
	}

	public String getStringField(short field) throws IOException, FieldNumberOutOfBoundException {
		if (field == 3) {
			throw new FieldNumberOutOfBoundException(null, "IN MAP, FIELD NUMBER PASSED IS INVALID");
		} else {
			return Convert.getStrValue(this.fieldOffset[field - 1], this.data,
					this.fieldOffset[field] - this.fieldOffset[field - 1]);
		}
	}

	private short[] duplicateOffsetField() {
		short[] offsetFieldNew = new short[this.fieldCount + 1];
		System.arraycopy(this.fieldOffset, 0, offsetFieldNew, 0, this.fieldCount + 1);
		return offsetFieldNew;
	}

	public void setHeader(AttrType[] types, short[] stringSizes)
			throws InvalidMapSizeException, IOException, InvalidTypeException, InvalidStringSizeArrayException {

		if (stringSizes.length != 3) {
			throw new InvalidStringSizeArrayException(null, "String sizes array must exactly be 3");
		}
		this.fieldCount = NUM_OF_FIELDS;
		Convert.setShortValue(NUM_OF_FIELDS, this.offset, this.data);
		this.fieldOffset = new short[NUM_OF_FIELDS + 1];
		int pos = this.offset + 2;
		this.fieldOffset[0] = (short) ((NUM_OF_FIELDS + 2) * 2 + this.offset);
		Convert.setShortValue(this.fieldOffset[0], pos, data);
		pos += 2;

		short increment;
		short stringCount = 0;
		for (short i = 0; i < NUM_OF_FIELDS; i++) {
			switch (types[i].attrType) {
			case AttrType.attrInteger:
				increment = 4;
				break;
			case AttrType.attrString:
				increment = (short) (stringSizes[stringCount++] + 2);
				break;
			default:
				throw new InvalidTypeException(null, "IN MAP, INVALID MAP TYPE");
			}
			this.fieldOffset[i + 1] = (short) (this.fieldOffset[i] + increment);
			Convert.setShortValue(this.fieldOffset[i + 1], pos, data);
			pos += 2;
		}

		this.maxLength = this.fieldOffset[NUM_OF_FIELDS] - this.offset;

		if (this.maxLength > MAX_PAGE_SIZE) {
			throw new InvalidMapSizeException(null, "IN MAP, INVALID MAP SIZE");
		}

	}

	public String getGenericValue(String field) throws Exception {
		if (field.matches(".*row.*")) {
			return this.getRowLabel();
		} else if (field.matches(".*column.*")) {
			return this.getColumnLabel();
		} else if (field.matches(".*value.*")) {
			return this.getValue();
		} else {
			throw new Exception("Invalid field type.");
		}
	}
	public Map setStrFld(int fieldNumber, String val) throws IOException, FieldNumberOutOfBoundException {
		if ((fieldNumber > 0) && (fieldNumber <= fieldCount)) {
			Convert.setStrValue(val, fieldOffset[fieldNumber - 1], data);
			return this;
		} else
			throw new FieldNumberOutOfBoundException(null, "IN MAP, FIELD NUMBER IS OUT OF BOUND");
	}

	public Map setIntFld(int fieldNumber, int val) throws IOException, FieldNumberOutOfBoundException {
		if ((fieldNumber > 0) && (fieldNumber <= fieldCount)) {
			Convert.setIntValue(val, fieldOffset[fieldNumber - 1], data);
			return this;
		} else
			throw new FieldNumberOutOfBoundException(null, "IN MAP, FIELD NUMBER IS OUT OF BOUND");
	}

	public Map setFloFld(int fieldNumber, float val) throws IOException, FieldNumberOutOfBoundException {
		if ((fieldNumber > 0) && (fieldNumber <= fieldCount)) {
			Convert.setFloValue(val, fieldOffset[fieldNumber - 1], data);
			return this;
		} else
			throw new FieldNumberOutOfBoundException(null, "IN MAP, FIELD NUMBER IS OUT OF BOUND");

	}

	private void setFieldOffsetFromData() throws IOException {
		int position = this.offset + 2;
		this.fieldOffset = new short[NUM_OF_FIELDS + 1];

		for (int i = 0; i <= NUM_OF_FIELDS; i++) {
			this.fieldOffset[i] = Convert.getShortValue(position, this.data);
			position += 2;
		}
	}

	public Map(int size) {
		this.data = new byte[size];
		this.offset = 0;
		this.maxLength = size;
		this.fieldCount = 4;
	}

	public void copyMap(Map fromMap) {
		byte[] tempArray = fromMap.getMapByteArray();
		System.arraycopy(tempArray, 0, data, offset, maxLength);
	}

}