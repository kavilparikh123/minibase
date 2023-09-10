package commandLine;

import static global.GlobalConst.NUMBUF;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

import BigT.Map;
import BigT.Stream;
import BigT.bigT;
import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import commandLine.StringAlignUtils.Alignment;
import diskmgr.Pcounter;
import global.AttrType;
import global.MID;
import global.SystemDefs;

public class MiniTable {
	public static final AttrType[] BIGT_ATTR_TYPES = new AttrType[] { new AttrType(0), new AttrType(0), new AttrType(1),
			new AttrType(0) };
	public static short[] BIGT_STR_SIZES = new short[] { (short) 25, (short) 25, (short) 25 };
	public static int orderType = 1;
	private static final int NUM_PAGES = 100000;

	public static void main(String[] args) throws IOException, PageUnpinnedException, PagePinnedException,
			PageNotFoundException, BufMgrException, HashOperationException {

		String input = null;
		String[] inputStr = null;
		while (true) {
			System.out.print("bigTable>  ");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			input = br.readLine();
			if (input.equals(""))
				continue;
			inputStr = input.trim().split("\\s+");
			final long startTime = System.currentTimeMillis();

			try {
				if (inputStr[0].equalsIgnoreCase("quit"))
					break;
				else if (inputStr[0].equalsIgnoreCase("batchinsert")) {
					String dataFile = inputStr[1];
					BIGT_STR_SIZES = setBigTConstants(dataFile);
					Integer type = Integer.parseInt(inputStr[2]);
					String tableName = inputStr[3]+"_"+type;
//					verifyDatabaseExists(tableName);
					File file = new File("/tmp/" + tableName + "_metadata.txt");
					FileWriter fileWriter = new FileWriter(file);
					BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
					bufferedWriter.write(dataFile);
					bufferedWriter.close();
					String databasePath = "/tmp/" + tableName + ".db";
					System.out.println(databasePath);
					Integer numPages = NUM_PAGES;
					new SystemDefs(databasePath, numPages, NUMBUF, "Clock");
					Pcounter.initialize();

					FileInputStream fileStream = null;
					BufferedReader bufferedReader = null;
					try {
						bigT bigTable = new bigT(tableName, type);
						fileStream = new FileInputStream(dataFile);
						bufferedReader = new BufferedReader(new InputStreamReader(fileStream));
						String inputString;
//						int mapCount = 0;

						while ((inputString = bufferedReader.readLine()) != null) {
							String[] inputArray = inputString.split(",");
							// set the map
							Map map = new Map();
							map.setHeader(MiniTable.BIGT_ATTR_TYPES, MiniTable.BIGT_STR_SIZES);
							map.setRowLabel(inputArray[0]);
							map.setColumnLabel(inputArray[1]);
							map.setTimeStamp(Integer.parseInt(inputArray[2]));
							map.setValue(inputArray[3]);
							MID mid = bigTable.insertMap(map.getMapByteArray());
//							mapCount++;
						}
						System.out.println("===============TABLE DETAILS========================\n");
						System.out.println("Map count: " + bigTable.getMapCnt());
						System.out.println("No. of Unique Rows= " + bigTable.getRowCnt()+ " ,Unique Columns= "+bigTable.getColumnCnt());
						System.out.println("\n=============PERFORMANCE METRICS==========================\n");
						System.out.println("Total Reads : " + Pcounter.rcounter);
						System.out.println("Total Writes: " + Pcounter.wcounter);
						System.out.println("Number of Buffers: " + NUMBUF);
						System.out.println("\n=======================================\n");
						bigTable.close();

					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						fileStream.close();
						bufferedReader.close();
					}

					SystemDefs.JavabaseBM.flushAllPages();
					SystemDefs.JavabaseDB.closeDB();
				} else if (inputStr[0].equalsIgnoreCase("query")) {
					Integer type = Integer.parseInt(inputStr[2]);
					String tableName = inputStr[1].trim()+"_"+type;
					
					String filename = "/tmp/" + tableName + "_metadata.txt";
					FileReader fileReader;
					BufferedReader bufferedReader = null;
					try {
						fileReader = new FileReader(filename);
						bufferedReader = new BufferedReader(fileReader);
					} catch (FileNotFoundException e) {
						System.out.println("Given tableName does not exist\n\n");
						continue;
					}
					String metadataFile = bufferedReader.readLine();
					bufferedReader.close();
					BIGT_STR_SIZES = setBigTConstants(metadataFile);
					
					orderType = Integer.parseInt(inputStr[3]);
					String rowFilter = inputStr[4].trim();
					String colFilter = inputStr[5].trim();
					String valFilter = inputStr[6].trim();
					Integer NUMBUF = Integer.parseInt(inputStr[7]);
					verifyDatabaseMissing(tableName);
					String dbPath = "/tmp/" + tableName + ".db";
					new SystemDefs(dbPath, 0, NUMBUF, "Clock");
					Pcounter.initialize();
					int resultCount = 0;
					try {
						bigT bigTable = new bigT(tableName);
						if (!type.equals(bigTable.getType())) {
							System.out.println("The Type is not matching");
							bigTable.close();
							return;
						}
						Stream mapStream = bigTable.openStream(orderType, rowFilter, colFilter, valFilter);
						StringAlignUtils util = new StringAlignUtils(20, Alignment.CENTER);
                        System.out.printf("[%20s|%20s|%20s|%20s]\n\n",util.format("Row "),util.format("Column"),util.format("Timestamp"),util.format("Value"));
						while (true) {
							Map map = mapStream.getNext();
							if (map == null)
								break;
							map.print(util);
							resultCount++;
						}
						bigTable.close();
						mapStream.closeStream();
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.out.println("\n=============PERFORMANCE METRICS==========================\n");
					System.out.println("Total Matched Records: " + resultCount);
					System.out.println("Total Reads : " + Pcounter.rcounter);
					System.out.println("Total Writes: " + Pcounter.wcounter);
					System.out.println("\n=======================================\n");
				} else {
					System.out.println("Invalid input. Type quit to exit.\n\n");
					continue;
				}
			} catch (Exception e) {
				System.out.println("Invalid Input parameters. Try again.\n\n");
				continue;
			}
			SystemDefs.JavabaseBM.flushAllPages();

			final long endTime = System.currentTimeMillis();
			System.out.println("Total execution time: " + (endTime - startTime)  + " milliseconds");

		}

		System.out.print("exiting...");
	}

	private static short[] setBigTConstants(String dataFileName) {
		try (BufferedReader reader = new BufferedReader(new FileReader(dataFileName))) {
			String line;
			int maxRowKeyLength = Short.MIN_VALUE;
			int maxColumnKeyLength = Short.MIN_VALUE;
			int maxValueLength = Short.MIN_VALUE;
			int maxTimeStampLength = Short.MIN_VALUE;
			while ((line = reader.readLine()) != null) {
				String[] fields = line.split(",");
				OutputStream out = new ByteArrayOutputStream();
				DataOutputStream rowKeyStream = new DataOutputStream(out);
				DataOutputStream columnKeyStream = new DataOutputStream(out);
				DataOutputStream timestampStream = new DataOutputStream(out);
				DataOutputStream valueStream = new DataOutputStream(out);

				rowKeyStream.writeUTF(fields[0]);
				maxRowKeyLength = Math.max(rowKeyStream.size(), maxRowKeyLength);

				columnKeyStream.writeUTF(fields[1]);
				maxColumnKeyLength = Math.max(columnKeyStream.size(), maxColumnKeyLength);

				timestampStream.writeUTF(fields[2]);
				maxTimeStampLength = Math.max(timestampStream.size(), maxTimeStampLength);

				valueStream.writeUTF(fields[3]);
				maxValueLength = Math.max(valueStream.size(), maxValueLength);
			}
			return new short[] { (short) maxRowKeyLength, (short) maxColumnKeyLength, (short) maxValueLength };
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return new short[0];
	}

	private static void verifyDatabaseMissing(String databaseName) {
		String databasePath = "/tmp/" + databaseName + ".db";
		File databaseFile = new File(databasePath);
		if (!databaseFile.exists()) {
			System.out.println("Database does not exist. Exiting.");
			System.exit(0);
		}
	}

}
